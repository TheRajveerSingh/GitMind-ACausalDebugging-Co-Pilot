"""
gitmind_agent.py — Causal Debugging Copilot (production-ready)
===============================================================
Integrates with:
  • slack_ingest.SlackIngester      (Slack → Neo4j + Snowflake)
  • jira_ingest.JiraIngester        (Jira  → Neo4j + Snowflake)
  • github_ingest.GitHubIngester    (GitHub → Neo4j + Snowflake)
  • snowflake_client.SnowflakeClient (analytics + stats layer)
  • config.GitMindConfig             (env-var driven, strict validation)

All connection errors surface immediately with actionable messages.
The agent itself is unchanged from the original design but now runs
against real data sources when the environment is configured.

Requirements
------------
    pip install langchain langchain-anthropic neo4j \
                "snowflake-connector-python[pandas]" cryptography \
                slack-sdk jira PyGithub

Environment variables — see config.py for the full list.
Minimum required to run the agent only (no ingest):
    ANTHROPIC_API_KEY
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD   (optional: uses placeholders when absent)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import textwrap
import uuid
from typing import Any, Optional

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool

# Local modules
from config import (
    GitMindConfig,
    GitMindConfigError,
    GitHubConfig,
    JiraConfig,
    LLMConfig,
    Neo4jConfig,
    SlackConfig,
    SnowflakeConfig,
)
from snowflake_client import IngestStats, SnowflakeClient, SnowflakeError
from slack_ingest import SlackIngester, SlackIngestError
from jira_ingest import JiraIngester, JiraIngestError
from github_ingest import GitHubIngester, GitHubIngestError

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gitmind")


# ===========================================================================
# Runtime context — holds live connections shared across tools
# ===========================================================================

class GitMindRuntime:
    """
    Lazily initialises and holds all external connections.
    Passed into tool closures via a module-level singleton.

    Call GitMindRuntime.from_env() to boot from environment variables.
    Call GitMindRuntime.from_config(cfg) if you already have a config.
    Use GitMindRuntime.demo() for local/CI runs without real credentials.
    """

    def __init__(
        self,
        llm_config: LLMConfig,
        neo4j_driver=None,
        sf_client: Optional[SnowflakeClient] = None,
        slack_ingester: Optional[SlackIngester] = None,
        jira_ingester: Optional[JiraIngester] = None,
        github_ingester: Optional[GitHubIngester] = None,
        use_placeholders: bool = False,
    ) -> None:
        self.llm_config = llm_config
        self.neo4j = neo4j_driver
        self.sf = sf_client
        self.slack = slack_ingester
        self.jira = jira_ingester
        self.github = github_ingester
        self.use_placeholders = use_placeholders

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "GitMindRuntime":
        """
        Build a fully wired runtime from environment variables.
        Raises GitMindConfigError with every missing variable listed.
        """
        cfg = GitMindConfig.from_env()
        return cls.from_config(cfg)

    @classmethod
    def from_config(cls, cfg: GitMindConfig) -> "GitMindRuntime":
        """Wire up all connections from a pre-built GitMindConfig."""

        # --- Neo4j ---
        neo4j_driver = None
        try:
            from neo4j import GraphDatabase, exceptions as neo4j_exc
            neo4j_driver = GraphDatabase.driver(
                cfg.neo4j.uri,
                auth=(cfg.neo4j.user, cfg.neo4j.password),
            )
            neo4j_driver.verify_connectivity()
            log.info("Neo4j connected: %s", cfg.neo4j.uri)
        except ImportError:
            raise ImportError(
                "neo4j driver not installed. Run: pip install neo4j"
            )
        except Exception as exc:
            raise ConnectionError(
                f"Cannot connect to Neo4j at '{cfg.neo4j.uri}': {exc}. "
                "Check NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD and network access."
            ) from exc

        # --- Snowflake ---
        sf_client = None
        try:
            sf_client = SnowflakeClient(cfg.snowflake)
        except SnowflakeError as exc:
            # Propagate with a clear top-level message
            raise SnowflakeError(f"Snowflake initialisation failed: {exc}") from exc

        # --- Ingesters ---
        slack = SlackIngester(cfg.slack, neo4j_driver=neo4j_driver, sf_client=sf_client)
        jira  = JiraIngester(cfg.jira,  neo4j_driver=neo4j_driver, sf_client=sf_client)
        github = GitHubIngester(cfg.github, neo4j_driver=neo4j_driver, sf_client=sf_client)

        return cls(
            llm_config=cfg.llm,
            neo4j_driver=neo4j_driver,
            sf_client=sf_client,
            slack_ingester=slack,
            jira_ingester=jira,
            github_ingester=github,
            use_placeholders=False,
        )

    @classmethod
    def demo(cls) -> "GitMindRuntime":
        """
        Return a placeholder runtime for local demo / CI runs.
        Requires only ANTHROPIC_API_KEY.
        """
        log.warning(
            "GitMind running in DEMO mode — all data sources use hard-coded placeholders. "
            "Set real environment variables to connect to live systems."
        )
        llm_cfg = LLMConfig.from_env()
        return cls(llm_config=llm_cfg, use_placeholders=True)

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self.neo4j:
            try:
                self.neo4j.close()
            except Exception:
                pass
        if self.sf:
            self.sf.close()


# Module-level singleton — set by build_agent() or debug()
_RUNTIME: Optional[GitMindRuntime] = None


def set_runtime(runtime: GitMindRuntime) -> None:
    global _RUNTIME
    _RUNTIME = runtime


def get_runtime() -> GitMindRuntime:
    if _RUNTIME is None:
        raise RuntimeError(
            "GitMind runtime not initialised. Call set_runtime() or use debug() entry point."
        )
    return _RUNTIME


# ===========================================================================
# Placeholder data (demo / unit-test mode)
# ===========================================================================

def _placeholder_commits(query: str) -> list[dict]:
    log.info("[PLACEHOLDER] search_commits query=%r", query)
    return [
        {
            "id": "commit:abc123",
            "sha": "abc123f",
            "message": "fix: remove duplicate cache invalidation on auth token refresh",
            "author": "alice@example.com",
            "timestamp": "2024-11-12T09:14:22Z",
            "files_changed": ["src/auth/token_cache.py"],
            "similarity": 0.91,
        },
        {
            "id": "commit:def456",
            "sha": "def456a",
            "message": "refactor: centralise Redis TTL config",
            "author": "bob@example.com",
            "timestamp": "2024-11-10T16:30:05Z",
            "files_changed": ["src/cache/redis_config.py", "src/auth/token_cache.py"],
            "similarity": 0.78,
        },
    ]


def _placeholder_tickets(query: str) -> list[dict]:
    log.info("[PLACEHOLDER] search_tickets query=%r", query)
    return [
        {
            "id": "ticket:PLAT-4821",
            "key": "PLAT-4821",
            "summary": "Users intermittently logged out after token refresh",
            "status": "In Progress",
            "priority": "High",
            "reporter": "charlie@example.com",
            "created": "2024-11-13T08:00:00Z",
            "similarity": 0.88,
        }
    ]


def _placeholder_slack(query: str) -> list[dict]:
    log.info("[PLACEHOLDER] search_slack query=%r", query)
    return [
        {
            "id": "slack:msg_001",
            "channel": "#incidents",
            "user": "dave",
            "text": (
                "Seeing a spike in 401s since the deploy at 09:00 UTC. "
                "Looks like token_cache.py may be deleting valid tokens too early."
            ),
            "timestamp": "2024-11-12T11:45:00Z",
            "similarity": 0.85,
        }
    ]


def _placeholder_traverse(node_id: str) -> dict:
    log.info("[PLACEHOLDER] traverse_graph node_id=%r", node_id)
    return {
        "start_node": node_id,
        "causal_chain": [
            {
                "node_id": node_id,
                "type": "commit",
                "description": "remove duplicate cache invalidation on auth token refresh",
            },
            {
                "node_id": "commit:def456",
                "type": "commit",
                "description": "centralise Redis TTL config — introduced DEFAULT_TTL=0 fallback",
                "relation": "CAUSED_BY",
            },
            {
                "node_id": "ticket:PLAT-4710",
                "type": "ticket",
                "description": "Redis TTL misconfiguration allows TTL=0, tokens expire instantly",
                "relation": "CAUSED_BY",
            },
        ],
        "root_node": {
            "node_id": "ticket:PLAT-4710",
            "type": "ticket",
            "description": "Redis TTL misconfiguration — DEFAULT_TTL defaults to 0 when env var absent",
        },
    }


def _placeholder_patch(context: str) -> dict:
    log.info("[PLACEHOLDER] generate_patch context_length=%d chars", len(context))
    diff = textwrap.dedent(
        """\
        --- a/src/cache/redis_config.py
        +++ b/src/cache/redis_config.py
        @@ -12,7 +12,8 @@
         class RedisConfig:
        -    DEFAULT_TTL: int = int(os.getenv("REDIS_DEFAULT_TTL", 0))
        +    # BUG FIX: default to 3600 s (1 h) so tokens are not expired instantly
        +    # when the environment variable is absent (e.g. during local dev / CI).
        +    DEFAULT_TTL: int = int(os.getenv("REDIS_DEFAULT_TTL", 3600))
        """
    )
    return {
        "patch": diff,
        "annotation": (
            "The root cause is DEFAULT_TTL=0 used as the fallback when the env var "
            "REDIS_DEFAULT_TTL is not set. This causes every token to expire "
            "immediately on creation, triggering the duplicate-invalidation bug "
            "observed in commit abc123f."
        ),
        "regression_safe": True,
        "test_suggestion": (
            "Add a unit test asserting that RedisConfig.DEFAULT_TTL > 0 "
            "when REDIS_DEFAULT_TTL is absent from the environment."
        ),
    }


# ===========================================================================
# Neo4j semantic search helpers (used by real tools)
# ===========================================================================

def _neo4j_search(label: str, query: str, limit: int = 5) -> list[dict]:
    """
    Full-text / vector search against Neo4j.
    Falls back to a simple CONTAINS match when no vector index exists.
    """
    runtime = get_runtime()
    if runtime.use_placeholders or runtime.neo4j is None:
        return []

    cypher = f"""
    CALL db.index.fulltext.queryNodes('{label.lower()}_fulltext', $query)
    YIELD node, score
    RETURN node {{ .* }} AS props, score
    ORDER BY score DESC
    LIMIT $limit
    """
    try:
        with runtime.neo4j.session() as session:
            result = session.run(cypher, query=query, limit=limit)
            return [
                {**r["props"], "similarity": round(r["score"], 4)}
                for r in result
            ]
    except Exception as exc:
        # Full-text index may not exist yet — fall back gracefully
        log.warning("Neo4j fulltext search failed (%s), using CONTAINS fallback: %s", label, exc)
        fallback = f"""
        MATCH (n:{label})
        WHERE toLower(n.text) CONTAINS toLower($query)
           OR toLower(n.message) CONTAINS toLower($query)
           OR toLower(n.summary) CONTAINS toLower($query)
        RETURN n {{ .* }} AS props
        LIMIT $limit
        """
        try:
            with runtime.neo4j.session() as session:
                result = session.run(fallback, query=query, limit=limit)
                return [{**r["props"], "similarity": 0.5} for r in result]
        except Exception as exc2:
            log.error("Neo4j CONTAINS fallback also failed: %s", exc2)
            return []


def _neo4j_traverse(node_id: str) -> dict:
    """Walk backward in the causal graph."""
    runtime = get_runtime()
    if runtime.use_placeholders or runtime.neo4j is None:
        return _placeholder_traverse(node_id)

    cypher = """
    MATCH path = (root)-[:CAUSED_BY*1..5]->(start {id: $node_id})
    WITH nodes(path) AS chain
    RETURN [n IN chain | {node_id: n.id, type: labels(n)[0], description: coalesce(n.summary, n.message, n.text, '')}] AS chain
    ORDER BY length(path) DESC
    LIMIT 1
    """
    try:
        with runtime.neo4j.session() as session:
            result = session.run(cypher, node_id=node_id)
            record = result.single()
            if not record:
                return {"start_node": node_id, "causal_chain": [], "root_node": None}
            chain = record["chain"]
            return {
                "start_node": node_id,
                "causal_chain": chain,
                "root_node": chain[0] if chain else None,
            }
    except Exception as exc:
        log.error("Neo4j graph traversal failed for node %s: %s", node_id, exc)
        raise


# ===========================================================================
# LangChain @tool definitions
# ===========================================================================

@tool
def search_commits(query: str) -> str:
    """Search git commits from the Neo4j knowledge graph by semantic similarity.

    Args:
        query: Natural-language description of the change or bug to search for.

    Returns:
        JSON list of matching commit records with id, sha, message, author,
        timestamp, files_changed, and similarity score.
    """
    runtime = get_runtime()
    if runtime.use_placeholders:
        return json.dumps(_placeholder_commits(query), indent=2)

    results = _neo4j_search("Commit", query)
    return json.dumps(results, indent=2)


@tool
def search_tickets(query: str) -> str:
    """Search Jira tickets from the Neo4j knowledge graph by semantic similarity.

    Args:
        query: Natural-language description of the issue to search for.

    Returns:
        JSON list of matching ticket records with id, key, summary, status,
        priority, reporter, created, and similarity score.
    """
    runtime = get_runtime()
    if runtime.use_placeholders:
        return json.dumps(_placeholder_tickets(query), indent=2)

    results = _neo4j_search("JiraTicket", query)
    return json.dumps(results, indent=2)


@tool
def search_slack(query: str) -> str:
    """Search Slack messages from the Neo4j knowledge graph by semantic similarity.

    Args:
        query: Natural-language description of the topic or incident to search for.

    Returns:
        JSON list of matching Slack messages with id, channel, user, text,
        timestamp, and similarity score.
    """
    runtime = get_runtime()
    if runtime.use_placeholders:
        return json.dumps(_placeholder_slack(query), indent=2)

    results = _neo4j_search("SlackMessage", query)
    return json.dumps(results, indent=2)


@tool
def traverse_graph(node_id: str) -> str:
    """Walk backward in the Neo4j causal graph from a given node to find its
    causal parents — the chain of commits, tickets, or Slack threads that led to it.

    Args:
        node_id: The Neo4j node identifier to start from (e.g. "commit:abc123").

    Returns:
        JSON object with start_node, causal_chain list, and root_node.
    """
    runtime = get_runtime()
    if runtime.use_placeholders:
        return json.dumps(_placeholder_traverse(node_id), indent=2)

    result = _neo4j_traverse(node_id)
    return json.dumps(result, indent=2)


@tool
def generate_patch(context: str) -> str:
    """Call Claude to produce an annotated unified diff that fixes the root cause
    identified in the provided context.

    Args:
        context: A thorough description of the root cause, affected files,
                 and relevant evidence gathered so far.

    Returns:
        JSON object with patch (unified diff), annotation, regression_safe (bool),
        and test_suggestion.
    """
    runtime = get_runtime()
    if runtime.use_placeholders:
        return json.dumps(_placeholder_patch(context), indent=2)

    # Use a dedicated Claude call for patch generation (not the agent's own loop)
    llm_cfg = runtime.llm_config
    patch_llm = ChatAnthropic(
        model=llm_cfg.model,
        temperature=0,
        max_tokens=2048,
    )
    patch_prompt = (
        "You are a senior software engineer. Given the root-cause description below, "
        "produce a minimal, regression-safe unified diff fixing the issue. "
        "Reply ONLY with a JSON object with keys: patch (string), annotation (string), "
        "regression_safe (bool), test_suggestion (string). No markdown fences.\n\n"
        f"Context:\n{context}"
    )
    response = patch_llm.invoke(patch_prompt)
    raw = response.content if hasattr(response, "content") else str(response)
    try:
        clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        return clean  # already JSON
    except Exception:
        return raw


@tool
def run_ingest(source: str) -> str:
    """Trigger a fresh data ingestion from Slack, Jira, or GitHub into Neo4j and Snowflake.

    Args:
        source: One of 'slack', 'jira', 'github', or 'all'.

    Returns:
        JSON summary of the ingest run with record counts and duration.
    """
    runtime = get_runtime()
    if runtime.use_placeholders:
        return json.dumps({"status": "skipped", "reason": "demo mode — no live connections"})

    results: dict[str, Any] = {}
    sources = ["slack", "jira", "github"] if source == "all" else [source]

    for src in sources:
        try:
            if src == "slack":
                if not runtime.slack:
                    results[src] = {"error": "Slack ingester not configured"}
                    continue
                stats = runtime.slack.ingest()
            elif src == "jira":
                if not runtime.jira:
                    results[src] = {"error": "Jira ingester not configured"}
                    continue
                stats = runtime.jira.ingest()
            elif src == "github":
                if not runtime.github:
                    results[src] = {"error": "GitHub ingester not configured"}
                    continue
                stats = runtime.github.ingest()
            else:
                results[src] = {"error": f"Unknown source '{src}'. Use slack/jira/github/all."}
                continue

            results[src] = {
                "status": "ok",
                "records_fetched": stats.records_fetched,
                "records_inserted": stats.records_inserted,
                "records_skipped": stats.records_skipped,
                "duration_seconds": round(stats.duration_seconds, 1),
            }
        except (SlackIngestError, JiraIngestError, GitHubIngestError) as exc:
            results[src] = {"status": "error", "error": str(exc)}

    return json.dumps(results, indent=2)


@tool
def query_snowflake(sql: str) -> str:
    """Execute a read-only SQL query against the GitMind Snowflake analytics database.
    Use this to retrieve statistics, historical ingest summaries, or cross-source
    data not available in the causal graph.

    Args:
        sql: A SELECT statement. DML/DDL is rejected.

    Returns:
        JSON list of result rows (up to 500).
    """
    if not sql.strip().upper().startswith("SELECT"):
        return json.dumps({"error": "Only SELECT statements are permitted."})

    runtime = get_runtime()
    if runtime.use_placeholders or runtime.sf is None:
        return json.dumps({"error": "Snowflake not connected (demo mode)."})

    try:
        rows = runtime.sf.execute(sql + " LIMIT 500")
        return json.dumps(rows, indent=2, default=str)
    except SnowflakeError as exc:
        return json.dumps({"error": str(exc)})


# ===========================================================================
# Agent construction
# ===========================================================================

TOOLS = [
    search_commits,
    search_tickets,
    search_slack,
    traverse_graph,
    generate_patch,
    run_ingest,
    query_snowflake,
]

SYSTEM_PROMPT = """\
You are GitMind, an expert causal debugging copilot with access to:
  • A Neo4j causal knowledge graph (commits, Jira tickets, Slack messages)
  • A Snowflake analytics warehouse (ingest stats, historical data)
  • Live ingest triggers for Slack, Jira, and GitHub

Your job:
1. Understand the bug description supplied by the user.
2. Optionally trigger a fresh ingest if data may be stale (use run_ingest).
3. Search commits, Jira tickets, and Slack messages for evidence.
4. Traverse the causal knowledge graph to find the root cause.
5. Generate a regression-safe patch.
6. Return a **single** JSON object (and nothing else) as your final answer:

{
  "root_cause":      "<concise 1-2 sentence description>",
  "evidence_chain":  ["<item 1>", "<item 2>", ...],
  "patch":           "<unified diff string>",
  "regression_safe": true | false
}

Rules:
- Call tools as many times as needed within the iteration budget.
- Be systematic: gather evidence → traverse graph → generate patch.
- Always ground root_cause in specific node IDs or commit SHAs.
- Your FINAL message must be valid JSON only — no markdown fences, no prose.
- If a tool returns an error, log it in evidence_chain and continue reasoning.
"""


def build_agent(runtime: Optional[GitMindRuntime] = None) -> AgentExecutor:
    """Construct and return the GitMind AgentExecutor.

    Parameters
    ----------
    runtime : GitMindRuntime, optional
        If not provided the module-level singleton is used.
        Pass GitMindRuntime.demo() for local runs without live credentials.
    """
    if runtime is not None:
        set_runtime(runtime)

    rt = get_runtime()

    llm = ChatAnthropic(
        model=rt.llm_config.model,
        temperature=rt.llm_config.temperature,
        max_tokens=rt.llm_config.max_tokens,
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ]
    )

    agent = create_tool_calling_agent(llm=llm, tools=TOOLS, prompt=prompt)

    return AgentExecutor(
        agent=agent,
        tools=TOOLS,
        max_iterations=rt.llm_config.max_agent_iterations,
        verbose=True,
        return_intermediate_steps=True,
        handle_parsing_errors=True,
    )


# ===========================================================================
# Step logging
# ===========================================================================

def _log_intermediate_steps(steps: list) -> None:
    log.info("=" * 60)
    log.info("AGENT TRACE  (%d step(s))", len(steps))
    log.info("=" * 60)
    for i, (action, observation) in enumerate(steps, start=1):
        log.info("Step %d | Tool: %s", i, action.tool)
        log.info("       | Input: %s", json.dumps(action.tool_input))
        obs_preview = str(observation)[:300].replace("\n", " ")
        log.info("       | Result (preview): %s …", obs_preview)
    log.info("=" * 60)


# ===========================================================================
# Public entry point
# ===========================================================================

def debug(bug_description: str, runtime: Optional[GitMindRuntime] = None) -> dict:
    """
    Run GitMind on *bug_description* and return a structured result dict.

    Parameters
    ----------
    bug_description : str
        Free-text description of the bug to investigate.
    runtime : GitMindRuntime, optional
        Defaults to GitMindRuntime.demo() when not supplied. In production,
        pass GitMindRuntime.from_env() or GitMindRuntime.from_config(cfg).

    Returns
    -------
    dict
        Keys: root_cause, evidence_chain, patch, regression_safe.

    Raises
    ------
    GitMindConfigError
        When environment variables are missing and from_env() is used.
    ValueError
        When the agent's final output is not valid JSON.
    """
    if runtime is None:
        runtime = GitMindRuntime.demo()

    set_runtime(runtime)

    log.info("GitMind starting | bug=%r", bug_description[:120])

    executor = build_agent(runtime)
    result = executor.invoke({"input": bug_description})

    _log_intermediate_steps(result.get("intermediate_steps", []))

    raw_output: str = result["output"]
    log.info("Raw agent output:\n%s", raw_output)

    try:
        clean = raw_output.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        report: dict = json.loads(clean)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Agent did not return valid JSON.\nRaw output:\n{raw_output}"
        ) from exc

    report["regression_safe"] = bool(report.get("regression_safe", False))
    log.info("GitMind finished | root_cause=%r", report.get("root_cause", ""))
    return report


# ===========================================================================
# CLI demo
# ===========================================================================

if __name__ == "__main__":
    BUG = (
        "Users are being randomly logged out shortly after a successful login. "
        "The issue started appearing after yesterday's 09:00 UTC deployment. "
        "Auth tokens seem to expire immediately rather than lasting their expected TTL."
    )

    # Try live env → fall back to demo gracefully
    try:
        rt = GitMindRuntime.from_env()
    except GitMindConfigError as exc:
        log.warning("Live config unavailable (%s). Falling back to demo mode.", exc)
        rt = GitMindRuntime.demo()

    try:
        report = debug(BUG, runtime=rt)
    finally:
        rt.close()

    print("\n" + "=" * 60)
    print("GITMIND FINAL REPORT")
    print("=" * 60)
    print(json.dumps(report, indent=2))
