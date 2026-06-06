"""
jira_ingest.py — Jira → Neo4j + Snowflake ingestion for GitMind
================================================================
Fetches Jira issues via JQL, upserts them into Neo4j, and mirrors them
to Snowflake. Designed to be run on a schedule (e.g. every 15 minutes).

Install:
    pip install jira
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("gitmind.ingest.jira")


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------

class JiraIngestError(Exception):
    """Base class for all Jira ingestion errors."""


class JiraConnectionError(JiraIngestError):
    """Raised when the Jira client cannot authenticate or reach the server."""


class JiraQueryError(JiraIngestError):
    """Raised when a JQL query fails."""


class JiraDataValidationError(JiraIngestError):
    """Raised when a Jira issue is missing required fields."""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class JiraTicket:
    ticket_id: str          # e.g. "PLAT-4821"
    summary: str
    description: str
    status: str
    priority: str
    reporter: str
    assignee: str
    labels: list[str]
    components: list[str]
    created: str            # ISO-8601
    updated: str
    resolved: Optional[str]
    url: str
    project: str
    issue_type: str
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_neo4j_props(self) -> dict[str, Any]:
        return {
            "id": f"ticket:{self.ticket_id}",
            "key": self.ticket_id,
            "summary": self.summary,
            "status": self.status,
            "priority": self.priority,
            "reporter": self.reporter,
            "created": self.created,
        }

    def to_snowflake_row(self) -> tuple:
        return (
            self.ticket_id,
            self.project,
            self.issue_type,
            self.summary,
            self.description[:4096] if self.description else "",
            self.status,
            self.priority,
            self.reporter,
            self.assignee,
            json.dumps(self.labels),
            json.dumps(self.components),
            self.created,
            self.updated,
            self.resolved,
            self.url,
            self.fetched_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# Jira client wrapper
# ---------------------------------------------------------------------------

def _import_jira():
    try:
        from jira import JIRA
        from jira.exceptions import JIRAError
        return JIRA, JIRAError
    except ImportError as exc:
        raise ImportError(
            "jira library not installed. Run: pip install jira"
        ) from exc


class JiraIngester:
    """
    Fetches Jira tickets and persists them to Neo4j + Snowflake.

    Parameters
    ----------
    jira_cfg     : JiraConfig
    neo4j_driver : neo4j.Driver  (or None to skip Neo4j writes)
    sf_client    : SnowflakeClient (or None to skip Snowflake writes)
    """

    _SF_DDL = """
    CREATE TABLE IF NOT EXISTS JIRA_TICKETS (
        TICKET_ID    VARCHAR(64)    PRIMARY KEY,
        PROJECT      VARCHAR(64)    NOT NULL,
        ISSUE_TYPE   VARCHAR(128),
        SUMMARY      VARCHAR(2048)  NOT NULL,
        DESCRIPTION  VARCHAR(65536),
        STATUS       VARCHAR(128),
        PRIORITY     VARCHAR(64),
        REPORTER     VARCHAR(256),
        ASSIGNEE     VARCHAR(256),
        LABELS       VARIANT,
        COMPONENTS   VARIANT,
        CREATED_AT   TIMESTAMP_TZ,
        UPDATED_AT   TIMESTAMP_TZ,
        RESOLVED_AT  TIMESTAMP_TZ,
        URL          VARCHAR(1024),
        FETCHED_AT   TIMESTAMP_TZ  NOT NULL
    )
    """

    _SF_MERGE = """
    MERGE INTO JIRA_TICKETS AS tgt
    USING (SELECT %s AS TICKET_ID) AS src ON tgt.TICKET_ID = src.TICKET_ID
    WHEN MATCHED THEN UPDATE SET
        STATUS      = %s,
        ASSIGNEE    = %s,
        UPDATED_AT  = %s,
        RESOLVED_AT = %s,
        FETCHED_AT  = %s
    WHEN NOT MATCHED THEN INSERT (
        TICKET_ID, PROJECT, ISSUE_TYPE, SUMMARY, DESCRIPTION,
        STATUS, PRIORITY, REPORTER, ASSIGNEE, LABELS, COMPONENTS,
        CREATED_AT, UPDATED_AT, RESOLVED_AT, URL, FETCHED_AT
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, %s, %s
    )
    """

    def __init__(self, jira_cfg, neo4j_driver=None, sf_client=None) -> None:
        self._cfg = jira_cfg
        self._neo4j = neo4j_driver
        self._sf = sf_client
        self._client = self._connect()

        if sf_client:
            self._ensure_sf_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        jql: Optional[str] = None,
        max_results: int = 500,
    ) -> "IngestStats":
        from snowflake_client import IngestStats

        if jql is None:
            jql = (
                f"project = {self._cfg.default_project} "
                "AND updated >= -7d "
                "ORDER BY updated DESC"
            )

        run_id = str(uuid.uuid4())
        stats = IngestStats(source="jira", run_id=run_id)
        log.info("Jira ingest started | run_id=%s jql=%r", run_id, jql)

        try:
            self._ingest_jql(jql, max_results, stats)
        except Exception as exc:
            stats.finish(error=str(exc))
            if self._sf:
                self._sf.write_stats(stats)
            raise

        stats.finish()
        if self._sf:
            self._sf.write_stats(stats)

        log.info(
            "Jira ingest finished | fetched=%d inserted=%d skipped=%d errored=%d duration=%.1fs",
            stats.records_fetched, stats.records_inserted,
            stats.records_skipped, stats.records_errored,
            stats.duration_seconds,
        )
        return stats

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _connect(self):
        JIRA, JIRAError = _import_jira()
        cfg = self._cfg
        try:
            client = JIRA(
                server=cfg.url,
                basic_auth=(cfg.user, cfg.api_token),
                options={"verify": True},
                timeout=30,
            )
            # Validate credentials eagerly
            client.myself()
            log.info("Connected to Jira: %s as %s", cfg.url, cfg.user)
            return client
        except JIRAError as exc:
            status = getattr(exc, "status_code", None)
            if status == 401:
                raise JiraConnectionError(
                    f"Jira authentication failed for user '{cfg.user}' at '{cfg.url}'. "
                    "Check JIRA_USER and JIRA_API_TOKEN."
                ) from exc
            if status == 403:
                raise JiraConnectionError(
                    f"Jira access denied for user '{cfg.user}'. "
                    "Ensure the account has Browse Projects permission."
                ) from exc
            if status == 404:
                raise JiraConnectionError(
                    f"Jira URL not found: '{cfg.url}'. Check JIRA_URL."
                ) from exc
            raise JiraConnectionError(
                f"Failed to connect to Jira at '{cfg.url}': {exc}"
            ) from exc
        except Exception as exc:
            raise JiraConnectionError(
                f"Unexpected error connecting to Jira: {type(exc).__name__}: {exc}"
            ) from exc

    def _ingest_jql(self, jql: str, max_results: int, stats) -> None:
        _, JIRAError = _import_jira()

        start = 0
        page_size = min(100, max_results)

        while stats.records_fetched < max_results:
            try:
                issues = self._client.search_issues(
                    jql, startAt=start, maxResults=page_size,
                    fields="summary,description,status,priority,reporter,assignee,"
                           "labels,components,created,updated,resolutiondate,issuetype",
                )
            except JIRAError as exc:
                raise JiraQueryError(
                    f"JQL query failed: {jql!r}\n"
                    f"Jira error: {exc.text if hasattr(exc, 'text') else exc}"
                ) from exc

            if not issues:
                break

            stats.records_fetched += len(issues)

            for issue in issues:
                try:
                    ticket = self._parse_issue(issue)
                except JiraDataValidationError as exc:
                    log.warning("Skipping malformed Jira issue: %s", exc)
                    stats.records_skipped += 1
                    continue

                ok = self._write_ticket(ticket)
                if ok:
                    stats.records_inserted += 1
                else:
                    stats.records_skipped += 1

            start += len(issues)
            if len(issues) < page_size:
                break

    def _parse_issue(self, issue) -> JiraTicket:
        try:
            f = issue.fields
            return JiraTicket(
                ticket_id=issue.key,
                summary=f.summary or "",
                description=getattr(f, "description", "") or "",
                status=f.status.name if f.status else "Unknown",
                priority=f.priority.name if f.priority else "None",
                reporter=f.reporter.emailAddress if f.reporter else "unknown",
                assignee=f.assignee.emailAddress if f.assignee else "unassigned",
                labels=list(f.labels) if f.labels else [],
                components=[c.name for c in (f.components or [])],
                created=str(f.created),
                updated=str(f.updated),
                resolved=str(f.resolutiondate) if f.resolutiondate else None,
                url=f"{self._cfg.url}/browse/{issue.key}",
                project=issue.key.split("-")[0],
                issue_type=f.issuetype.name if f.issuetype else "Unknown",
            )
        except AttributeError as exc:
            raise JiraDataValidationError(
                f"Issue {issue.key} missing expected field: {exc}"
            ) from exc

    def _write_ticket(self, ticket: JiraTicket) -> bool:
        inserted = False
        if self._neo4j:
            inserted = self._neo4j_upsert(ticket)
        if self._sf:
            sf_inserted = self._sf_merge(ticket)
            inserted = inserted or sf_inserted
        return inserted

    def _neo4j_upsert(self, ticket: JiraTicket) -> bool:
        cypher = """
        MERGE (t:JiraTicket {id: $id})
        SET t += $props, t.updatedAt = timestamp()
        RETURN (t.createdAt IS NULL) AS is_new
        """
        props = ticket.to_neo4j_props()
        node_id = f"ticket:{ticket.ticket_id}"
        try:
            with self._neo4j.session() as session:
                result = session.run(cypher, id=node_id, props=props)
                record = result.single()
                return bool(record and record["is_new"])
        except Exception as exc:
            raise JiraIngestError(f"Neo4j write failed for ticket {ticket.ticket_id}: {exc}") from exc

    def _sf_merge(self, ticket: JiraTicket) -> bool:
        r = ticket.to_snowflake_row()
        # r indices: 0=ticket_id, 1=project, 2=issue_type, 3=summary, 4=description,
        #            5=status, 6=priority, 7=reporter, 8=assignee, 9=labels,
        #            10=components, 11=created, 12=updated, 13=resolved, 14=url, 15=fetched_at
        try:
            with self._sf.cursor() as cur:
                cur.execute(self._SF_MERGE, (
                    r[0],                          # USING TICKET_ID
                    r[5], r[8], r[12], r[13], r[15],  # WHEN MATCHED SET
                    r[0], r[1], r[2], r[3], r[4],     # INSERT values (part 1)
                    r[5], r[6], r[7], r[8],            # INSERT values (part 2)
                    r[9], r[10], r[11], r[12], r[13], r[14], r[15],  # rest
                ))
                return cur.rowcount > 0
        except Exception as exc:
            raise JiraIngestError(f"Snowflake write failed for ticket {ticket.ticket_id}: {exc}") from exc

    def _ensure_sf_table(self) -> None:
        try:
            self._sf.execute(self._SF_DDL)
        except Exception as exc:
            raise JiraIngestError(f"Could not create JIRA_TICKETS table: {exc}") from exc
