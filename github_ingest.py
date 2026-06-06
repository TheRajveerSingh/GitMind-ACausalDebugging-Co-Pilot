"""
github_ingest.py — GitHub → Neo4j + Snowflake ingestion for GitMind
====================================================================
Fetches commits and pull-requests from GitHub repos, upserts them into
Neo4j, and mirrors raw records to Snowflake.

Install:
    pip install PyGithub
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("gitmind.ingest.github")


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------

class GitHubIngestError(Exception):
    """Base class for all GitHub ingestion errors."""


class GitHubConnectionError(GitHubIngestError):
    """Raised when the GitHub client cannot authenticate."""


class GitHubRepoNotFoundError(GitHubIngestError):
    """Raised when a repo does not exist or the token lacks access."""


class GitHubRateLimitError(GitHubIngestError):
    """Raised when the GitHub API rate limit is exceeded."""


class GitHubDataValidationError(GitHubIngestError):
    """Raised when a commit or PR is missing required fields."""


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class GitCommit:
    sha: str
    repo: str
    message: str
    author_name: str
    author_email: str
    committed_at: str       # ISO-8601
    files_changed: list[str]
    additions: int
    deletions: int
    url: str
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_neo4j_props(self) -> dict[str, Any]:
        return {
            "id": f"commit:{self.sha[:7]}",
            "sha": self.sha,
            "message": self.message[:512],
            "author": self.author_email,
            "timestamp": self.committed_at,
            "files_changed": self.files_changed,
        }

    def to_snowflake_row(self) -> tuple:
        return (
            self.sha,
            self.repo,
            self.message[:2048],
            self.author_name,
            self.author_email,
            self.committed_at,
            json.dumps(self.files_changed[:200]),
            self.additions,
            self.deletions,
            self.url,
            self.fetched_at.isoformat(),
        )


@dataclass
class PullRequest:
    pr_id: str              # "{repo}#{number}"
    repo: str
    number: int
    title: str
    body: str
    state: str              # "open" | "closed" | "merged"
    author: str
    base_branch: str
    head_branch: str
    created_at: str
    merged_at: Optional[str]
    commit_shas: list[str]  # commits included in this PR
    labels: list[str]
    url: str
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_neo4j_props(self) -> dict[str, Any]:
        return {
            "id": f"pr:{self.pr_id}",
            "repo": self.repo,
            "number": self.number,
            "title": self.title,
            "state": self.state,
            "author": self.author,
            "mergedAt": self.merged_at,
        }

    def to_snowflake_row(self) -> tuple:
        return (
            self.pr_id,
            self.repo,
            self.number,
            self.title,
            self.body[:8192] if self.body else "",
            self.state,
            self.author,
            self.base_branch,
            self.head_branch,
            self.created_at,
            self.merged_at,
            json.dumps(self.commit_shas),
            json.dumps(self.labels),
            self.url,
            self.fetched_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# GitHub client wrapper
# ---------------------------------------------------------------------------

def _import_github():
    try:
        from github import Github, GithubException, RateLimitExceededException, UnknownObjectException
        return Github, GithubException, RateLimitExceededException, UnknownObjectException
    except ImportError as exc:
        raise ImportError(
            "PyGithub is not installed. Run: pip install PyGithub"
        ) from exc


class GitHubIngester:
    """
    Fetches GitHub commits/PRs and persists them to Neo4j + Snowflake.

    Parameters
    ----------
    github_cfg   : GitHubConfig
    neo4j_driver : neo4j.Driver  (or None)
    sf_client    : SnowflakeClient (or None)
    """

    _SF_COMMITS_DDL = """
    CREATE TABLE IF NOT EXISTS GITHUB_COMMITS (
        SHA            VARCHAR(40)    PRIMARY KEY,
        REPO           VARCHAR(256)   NOT NULL,
        MESSAGE        VARCHAR(8192),
        AUTHOR_NAME    VARCHAR(256),
        AUTHOR_EMAIL   VARCHAR(256),
        COMMITTED_AT   TIMESTAMP_TZ,
        FILES_CHANGED  VARIANT,
        ADDITIONS      INTEGER        DEFAULT 0,
        DELETIONS      INTEGER        DEFAULT 0,
        URL            VARCHAR(1024),
        FETCHED_AT     TIMESTAMP_TZ   NOT NULL
    )
    """

    _SF_PRS_DDL = """
    CREATE TABLE IF NOT EXISTS GITHUB_PULL_REQUESTS (
        PR_ID          VARCHAR(256)   PRIMARY KEY,
        REPO           VARCHAR(256)   NOT NULL,
        PR_NUMBER      INTEGER        NOT NULL,
        TITLE          VARCHAR(2048),
        BODY           VARCHAR(65536),
        STATE          VARCHAR(32),
        AUTHOR         VARCHAR(256),
        BASE_BRANCH    VARCHAR(256),
        HEAD_BRANCH    VARCHAR(256),
        CREATED_AT     TIMESTAMP_TZ,
        MERGED_AT      TIMESTAMP_TZ,
        COMMIT_SHAS    VARIANT,
        LABELS         VARIANT,
        URL            VARCHAR(1024),
        FETCHED_AT     TIMESTAMP_TZ   NOT NULL
    )
    """

    _SF_COMMIT_INSERT = """
    INSERT INTO GITHUB_COMMITS
        (SHA, REPO, MESSAGE, AUTHOR_NAME, AUTHOR_EMAIL, COMMITTED_AT,
         FILES_CHANGED, ADDITIONS, DELETIONS, URL, FETCHED_AT)
    SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s
    WHERE NOT EXISTS (SELECT 1 FROM GITHUB_COMMITS WHERE SHA = %s)
    """

    _SF_PR_MERGE = """
    MERGE INTO GITHUB_PULL_REQUESTS AS tgt
    USING (SELECT %s AS PR_ID) AS src ON tgt.PR_ID = src.PR_ID
    WHEN MATCHED THEN UPDATE SET STATE = %s, MERGED_AT = %s, FETCHED_AT = %s
    WHEN NOT MATCHED THEN INSERT (
        PR_ID, REPO, PR_NUMBER, TITLE, BODY, STATE, AUTHOR, BASE_BRANCH,
        HEAD_BRANCH, CREATED_AT, MERGED_AT, COMMIT_SHAS, LABELS, URL, FETCHED_AT
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              PARSE_JSON(%s), PARSE_JSON(%s), %s, %s)
    """

    def __init__(self, github_cfg, neo4j_driver=None, sf_client=None) -> None:
        self._cfg = github_cfg
        self._neo4j = neo4j_driver
        self._sf = sf_client
        self._gh = self._connect()

        if sf_client:
            self._ensure_sf_tables()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        repos: Optional[list[str]] = None,
        since: Optional[datetime] = None,
        max_commits_per_repo: int = 500,
    ) -> "IngestStats":
        from snowflake_client import IngestStats

        repos = repos or self._cfg.default_repos
        if not repos:
            raise GitHubIngestError(
                "No repos specified. Pass repos= or set GITHUB_DEFAULT_REPOS."
            )

        run_id = str(uuid.uuid4())
        stats = IngestStats(source="github", run_id=run_id)
        log.info("GitHub ingest started | run_id=%s repos=%s", run_id, repos)

        try:
            for repo_name in repos:
                full_name = (
                    repo_name
                    if "/" in repo_name
                    else f"{self._cfg.org}/{repo_name}"
                )
                self._ingest_repo(full_name, since, max_commits_per_repo, stats)
        except (GitHubConnectionError, GitHubRateLimitError):
            stats.finish(error="GitHub API failure — see logs")
            if self._sf:
                self._sf.write_stats(stats)
            raise
        except Exception as exc:
            stats.finish(error=str(exc))
            if self._sf:
                self._sf.write_stats(stats)
            raise

        stats.finish()
        if self._sf:
            self._sf.write_stats(stats)

        log.info(
            "GitHub ingest finished | fetched=%d inserted=%d skipped=%d duration=%.1fs",
            stats.records_fetched, stats.records_inserted,
            stats.records_skipped, stats.duration_seconds,
        )
        return stats

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _connect(self):
        Github, GithubException, _, _ = _import_github()
        cfg = self._cfg
        try:
            gh = Github(login_or_token=cfg.token, per_page=100, retry=3, timeout=30)
            # Validate token eagerly
            rate = gh.get_rate_limit()
            log.info(
                "Connected to GitHub. Rate limit: %d/%d remaining (resets %s)",
                rate.core.remaining, rate.core.limit, rate.core.reset,
            )
            return gh
        except GithubException as exc:
            if exc.status == 401:
                raise GitHubConnectionError(
                    "GitHub authentication failed. Check GITHUB_TOKEN."
                ) from exc
            raise GitHubConnectionError(
                f"Failed to connect to GitHub API: {exc}"
            ) from exc
        except Exception as exc:
            raise GitHubConnectionError(
                f"Unexpected error connecting to GitHub: {type(exc).__name__}: {exc}"
            ) from exc

    def _get_repo(self, full_name: str):
        _, GithubException, RateLimitExceededException, UnknownObjectException = _import_github()
        try:
            return self._gh.get_repo(full_name)
        except UnknownObjectException:
            raise GitHubRepoNotFoundError(
                f"GitHub repo '{full_name}' not found or GITHUB_TOKEN lacks access."
            )
        except RateLimitExceededException as exc:
            raise GitHubRateLimitError(
                "GitHub API rate limit exceeded. Wait until the reset time shown in logs."
            ) from exc
        except GithubException as exc:
            raise GitHubIngestError(f"GitHub error accessing repo '{full_name}': {exc}") from exc

    def _ingest_repo(self, full_name: str, since, max_commits: int, stats) -> None:
        _, GithubException, RateLimitExceededException, _ = _import_github()
        repo = self._get_repo(full_name)
        log.info("Ingesting repo: %s", full_name)

        # Commits
        commit_kwargs: dict = {}
        if since:
            commit_kwargs["since"] = since

        count = 0
        try:
            for gh_commit in repo.get_commits(**commit_kwargs):
                if count >= max_commits:
                    break
                stats.records_fetched += 1
                try:
                    commit = self._parse_commit(gh_commit, full_name)
                except GitHubDataValidationError as exc:
                    log.warning("Skipping malformed commit: %s", exc)
                    stats.records_skipped += 1
                    continue

                ok = self._write_commit(commit)
                if ok:
                    stats.records_inserted += 1
                else:
                    stats.records_skipped += 1
                count += 1
        except RateLimitExceededException as exc:
            raise GitHubRateLimitError(
                "GitHub API rate limit hit during commit ingestion. "
                "Reduce max_commits_per_repo or wait for reset."
            ) from exc

        # Pull Requests
        try:
            for gh_pr in repo.get_pulls(state="all", sort="updated", direction="desc"):
                stats.records_fetched += 1
                try:
                    pr = self._parse_pr(gh_pr, full_name)
                except GitHubDataValidationError as exc:
                    log.warning("Skipping malformed PR: %s", exc)
                    stats.records_skipped += 1
                    continue

                ok = self._write_pr(pr)
                if ok:
                    stats.records_inserted += 1
                else:
                    stats.records_skipped += 1
        except RateLimitExceededException as exc:
            raise GitHubRateLimitError(
                "GitHub API rate limit hit during PR ingestion."
            ) from exc

    def _parse_commit(self, gh_commit, repo: str) -> GitCommit:
        try:
            commit = gh_commit.commit
            author = commit.author
            files = [f.filename for f in (gh_commit.files or [])]
            return GitCommit(
                sha=gh_commit.sha,
                repo=repo,
                message=commit.message or "",
                author_name=author.name if author else "unknown",
                author_email=author.email if author else "unknown",
                committed_at=author.date.isoformat() if author and author.date else "",
                files_changed=files,
                additions=gh_commit.stats.additions if gh_commit.stats else 0,
                deletions=gh_commit.stats.deletions if gh_commit.stats else 0,
                url=gh_commit.html_url or "",
            )
        except Exception as exc:
            raise GitHubDataValidationError(
                f"Could not parse commit {getattr(gh_commit, 'sha', '?')}: {exc}"
            ) from exc

    def _parse_pr(self, gh_pr, repo: str) -> PullRequest:
        try:
            state = "merged" if gh_pr.merged else gh_pr.state
            return PullRequest(
                pr_id=f"{repo}#{gh_pr.number}",
                repo=repo,
                number=gh_pr.number,
                title=gh_pr.title or "",
                body=gh_pr.body or "",
                state=state,
                author=gh_pr.user.login if gh_pr.user else "unknown",
                base_branch=gh_pr.base.ref,
                head_branch=gh_pr.head.ref,
                created_at=gh_pr.created_at.isoformat() if gh_pr.created_at else "",
                merged_at=gh_pr.merged_at.isoformat() if gh_pr.merged_at else None,
                commit_shas=[c.sha for c in gh_pr.get_commits()],
                labels=[l.name for l in gh_pr.labels],
                url=gh_pr.html_url or "",
            )
        except Exception as exc:
            raise GitHubDataValidationError(
                f"Could not parse PR #{getattr(gh_pr, 'number', '?')}: {exc}"
            ) from exc

    def _write_commit(self, commit: GitCommit) -> bool:
        inserted = False
        if self._neo4j:
            inserted = self._neo4j_upsert_commit(commit)
        if self._sf:
            sf_ok = self._sf_insert_commit(commit)
            inserted = inserted or sf_ok
        return inserted

    def _write_pr(self, pr: PullRequest) -> bool:
        inserted = False
        if self._neo4j:
            inserted = self._neo4j_upsert_pr(pr)
        if self._sf:
            sf_ok = self._sf_merge_pr(pr)
            inserted = inserted or sf_ok
        return inserted

    def _neo4j_upsert_commit(self, commit: GitCommit) -> bool:
        cypher = """
        MERGE (c:Commit {id: $id})
        SET c += $props, c.updatedAt = timestamp()
        RETURN (c.createdAt IS NULL) AS is_new
        """
        try:
            with self._neo4j.session() as session:
                result = session.run(
                    cypher,
                    id=f"commit:{commit.sha[:7]}",
                    props=commit.to_neo4j_props(),
                )
                record = result.single()
                return bool(record and record["is_new"])
        except Exception as exc:
            raise GitHubIngestError(f"Neo4j write failed for commit {commit.sha}: {exc}") from exc

    def _neo4j_upsert_pr(self, pr: PullRequest) -> bool:
        cypher = """
        MERGE (p:PullRequest {id: $id})
        SET p += $props, p.updatedAt = timestamp()
        RETURN (p.createdAt IS NULL) AS is_new
        """
        try:
            with self._neo4j.session() as session:
                result = session.run(
                    cypher,
                    id=f"pr:{pr.pr_id}",
                    props=pr.to_neo4j_props(),
                )
                record = result.single()
                return bool(record and record["is_new"])
        except Exception as exc:
            raise GitHubIngestError(f"Neo4j write failed for PR {pr.pr_id}: {exc}") from exc

    def _sf_insert_commit(self, commit: GitCommit) -> bool:
        r = commit.to_snowflake_row()
        try:
            with self._sf.cursor() as cur:
                cur.execute(self._SF_COMMIT_INSERT, r + (r[0],))
                return cur.rowcount > 0
        except Exception as exc:
            raise GitHubIngestError(f"Snowflake write failed for commit {commit.sha}: {exc}") from exc

    def _sf_merge_pr(self, pr: PullRequest) -> bool:
        r = pr.to_snowflake_row()
        # r: 0=pr_id,1=repo,2=number,3=title,4=body,5=state,6=author,
        #    7=base_branch,8=head_branch,9=created_at,10=merged_at,
        #    11=commit_shas,12=labels,13=url,14=fetched_at
        try:
            with self._sf.cursor() as cur:
                cur.execute(self._SF_PR_MERGE, (
                    r[0],                              # USING PR_ID
                    r[5], r[10], r[14],                # WHEN MATCHED SET
                    r[0], r[1], r[2], r[3], r[4],      # INSERT 1
                    r[5], r[6], r[7], r[8], r[9], r[10],  # INSERT 2
                    r[11], r[12], r[13], r[14],        # INSERT 3
                ))
                return cur.rowcount > 0
        except Exception as exc:
            raise GitHubIngestError(f"Snowflake write failed for PR {pr.pr_id}: {exc}") from exc

    def _ensure_sf_tables(self) -> None:
        for ddl in (self._SF_COMMITS_DDL, self._SF_PRS_DDL):
            try:
                self._sf.execute(ddl)
            except Exception as exc:
                raise GitHubIngestError(f"Could not create GitHub Snowflake tables: {exc}") from exc
