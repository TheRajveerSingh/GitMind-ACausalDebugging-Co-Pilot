"""
slack_ingest.py — Slack → Neo4j + Snowflake ingestion for GitMind
==================================================================
Fetches messages from configured Slack channels, upserts them into the
Neo4j causal knowledge graph, and mirrors raw records to Snowflake for
analytics and long-term storage.

Errors are raised as specific typed exceptions so the caller always
knows exactly what went wrong and where.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("gitmind.ingest.slack")


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------

class SlackIngestError(Exception):
    """Base class for all Slack ingestion errors."""


class SlackAPIError(SlackIngestError):
    """Raised when the Slack API returns an error or rate-limits us."""

    def __init__(self, method: str, error_code: str, message: str = "") -> None:
        self.method = method
        self.error_code = error_code
        super().__init__(
            f"Slack API error on '{method}': [{error_code}] {message}. "
            "Check SLACK_BOT_TOKEN scopes and channel membership."
        )


class SlackChannelNotFoundError(SlackIngestError):
    """Raised when a channel cannot be found or the bot is not a member."""


class SlackDataValidationError(SlackIngestError):
    """Raised when a fetched Slack message is malformed."""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class SlackMessage:
    message_id: str          # sha256 of channel+ts
    channel: str
    channel_id: str
    user: str
    user_id: str
    text: str
    timestamp: str           # Slack epoch string e.g. "1699789500.123456"
    thread_ts: Optional[str]
    reactions: list[str] = field(default_factory=list)
    attachments: int = 0
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @staticmethod
    def make_id(channel_id: str, ts: str) -> str:
        raw = f"{channel_id}:{ts}"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    def to_neo4j_props(self) -> dict[str, Any]:
        return {
            "id": f"slack:{self.message_id}",
            "channel": self.channel,
            "user": self.user,
            "text": self.text,
            "timestamp": self.timestamp,
            "thread_ts": self.thread_ts,
        }

    def to_snowflake_row(self) -> tuple:
        return (
            self.message_id,
            self.channel,
            self.channel_id,
            self.user,
            self.user_id,
            self.text,
            self.timestamp,
            self.thread_ts,
            json.dumps(self.reactions),
            self.attachments,
            self.fetched_at.isoformat(),
        )


# ---------------------------------------------------------------------------
# Slack API client (wraps slack_sdk)
# ---------------------------------------------------------------------------

def _import_slack():
    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError as _SlackSdkError
        return WebClient, _SlackSdkError
    except ImportError as exc:
        raise ImportError(
            "slack_sdk is not installed. Run: pip install slack-sdk"
        ) from exc


class SlackIngester:
    """
    Fetches Slack messages and persists them to Neo4j + Snowflake.

    Parameters
    ----------
    slack_cfg   : SlackConfig
    neo4j_driver: neo4j.Driver (or None to skip Neo4j writes)
    sf_client   : SnowflakeClient (or None to skip Snowflake writes)
    """

    _SF_DDL = """
    CREATE TABLE IF NOT EXISTS SLACK_MESSAGES (
        MESSAGE_ID   VARCHAR(64)    PRIMARY KEY,
        CHANNEL      VARCHAR(256)   NOT NULL,
        CHANNEL_ID   VARCHAR(64)    NOT NULL,
        USER_NAME    VARCHAR(256),
        USER_ID      VARCHAR(64),
        TEXT         VARCHAR(65536),
        SLACK_TS     VARCHAR(32)    NOT NULL,
        THREAD_TS    VARCHAR(32),
        REACTIONS    VARIANT,
        ATTACHMENTS  INTEGER        DEFAULT 0,
        FETCHED_AT   TIMESTAMP_TZ   NOT NULL
    )
    """

    _SF_INSERT = """
    INSERT INTO SLACK_MESSAGES
        (MESSAGE_ID, CHANNEL, CHANNEL_ID, USER_NAME, USER_ID,
         TEXT, SLACK_TS, THREAD_TS, REACTIONS, ATTACHMENTS, FETCHED_AT)
    SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM SLACK_MESSAGES WHERE MESSAGE_ID = %s
    )
    """

    def __init__(self, slack_cfg, neo4j_driver=None, sf_client=None) -> None:
        self._cfg = slack_cfg
        self._neo4j = neo4j_driver
        self._sf = sf_client

        WebClient, _ = _import_slack()
        self._client = WebClient(token=slack_cfg.bot_token)

        if sf_client:
            self._ensure_sf_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        channels: Optional[list[str]] = None,
        oldest: str = "0",
        limit_per_channel: int = 1000,
    ) -> "IngestStats":
        from snowflake_client import IngestStats

        channels = channels or self._cfg.default_channels
        if not channels:
            raise SlackIngestError(
                "No channels specified. Pass channels= or set SLACK_DEFAULT_CHANNELS."
            )

        run_id = str(uuid.uuid4())
        stats = IngestStats(source="slack", run_id=run_id)
        log.info("Slack ingest started | run_id=%s channels=%s", run_id, channels)

        try:
            for channel in channels:
                self._ingest_channel(channel, oldest, limit_per_channel, stats)
        except Exception as exc:
            stats.finish(error=str(exc))
            if self._sf:
                self._sf.write_stats(stats)
            raise

        stats.finish()
        if self._sf:
            self._sf.write_stats(stats)

        log.info(
            "Slack ingest finished | fetched=%d inserted=%d skipped=%d errored=%d duration=%.1fs",
            stats.records_fetched, stats.records_inserted,
            stats.records_skipped, stats.records_errored,
            stats.duration_seconds,
        )
        return stats

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ingest_channel(self, channel: str, oldest: str, limit: int, stats) -> None:
        _, SlackSdkError = _import_slack()

        # Resolve channel name → ID
        channel_id, channel_name = self._resolve_channel(channel)

        cursor = None
        fetched = 0

        while fetched < limit:
            batch_size = min(200, limit - fetched)
            try:
                resp = self._client.conversations_history(
                    channel=channel_id,
                    oldest=oldest,
                    limit=batch_size,
                    cursor=cursor,
                )
            except SlackSdkError as exc:
                self._handle_slack_sdk_error("conversations.history", exc)

            messages = resp.get("messages", [])
            stats.records_fetched += len(messages)
            fetched += len(messages)

            for raw in messages:
                try:
                    msg = self._parse_message(raw, channel_name, channel_id)
                except SlackDataValidationError as exc:
                    log.warning("Skipping malformed Slack message: %s", exc)
                    stats.records_skipped += 1
                    continue

                ok = self._write_message(msg)
                if ok:
                    stats.records_inserted += 1
                else:
                    stats.records_skipped += 1

            if not resp.get("has_more"):
                break
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

            time.sleep(0.5)   # Be polite to the Slack API

    def _resolve_channel(self, channel: str) -> tuple[str, str]:
        """Return (channel_id, display_name). Raises SlackChannelNotFoundError on failure."""
        _, SlackSdkError = _import_slack()

        # If it already looks like an ID (starts with C/G/D), use it directly
        if channel.startswith(("C", "G", "D")) and not channel.startswith("#"):
            return channel, channel

        name = channel.lstrip("#")
        cursor = None
        while True:
            try:
                resp = self._client.conversations_list(
                    types="public_channel,private_channel",
                    cursor=cursor,
                    limit=200,
                )
            except SlackSdkError as exc:
                self._handle_slack_sdk_error("conversations.list", exc)

            for ch in resp.get("channels", []):
                if ch.get("name") == name:
                    if ch.get("is_archived"):
                        raise SlackChannelNotFoundError(
                            f"Slack channel '#{name}' is archived and cannot be read."
                        )
                    return ch["id"], ch["name"]

            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        raise SlackChannelNotFoundError(
            f"Slack channel '#{name}' not found. "
            "Ensure the bot is a member: /invite @your-bot in that channel."
        )

    def _parse_message(self, raw: dict, channel: str, channel_id: str) -> SlackMessage:
        ts = raw.get("ts")
        if not ts:
            raise SlackDataValidationError(f"Message missing 'ts' field: {raw}")
        text = raw.get("text", "")
        user_id = raw.get("user", raw.get("bot_id", "UNKNOWN"))
        reactions = [r.get("name", "") for r in raw.get("reactions", [])]
        attachments = len(raw.get("attachments", []))

        return SlackMessage(
            message_id=SlackMessage.make_id(channel_id, ts),
            channel=channel,
            channel_id=channel_id,
            user=raw.get("username", user_id),
            user_id=user_id,
            text=text,
            timestamp=ts,
            thread_ts=raw.get("thread_ts"),
            reactions=reactions,
            attachments=attachments,
        )

    def _write_message(self, msg: SlackMessage) -> bool:
        """Write to Neo4j and/or Snowflake. Returns True if inserted."""
        inserted = False

        if self._neo4j:
            inserted = self._neo4j_upsert(msg)

        if self._sf:
            sf_inserted = self._sf_insert(msg)
            inserted = inserted or sf_inserted

        return inserted

    def _neo4j_upsert(self, msg: SlackMessage) -> bool:
        cypher = """
        MERGE (m:SlackMessage {id: $id})
        SET m += $props, m.updatedAt = timestamp()
        RETURN m.id AS id, (m.createdAt IS NULL) AS is_new
        """
        props = msg.to_neo4j_props()
        node_id = f"slack:{msg.message_id}"
        try:
            with self._neo4j.session() as session:
                result = session.run(cypher, id=node_id, props=props)
                record = result.single()
                return bool(record and record["is_new"])
        except Exception as exc:
            raise SlackIngestError(f"Neo4j write failed for message {msg.message_id}: {exc}") from exc

    def _sf_insert(self, msg: SlackMessage) -> bool:
        row = msg.to_snowflake_row()
        try:
            with self._sf.cursor() as cur:
                cur.execute(self._SF_INSERT, row + (row[0],))   # append MESSAGE_ID for WHERE NOT EXISTS
                return cur.rowcount > 0
        except Exception as exc:
            raise SlackIngestError(f"Snowflake write failed for message {msg.message_id}: {exc}") from exc

    def _ensure_sf_table(self) -> None:
        try:
            self._sf.execute(self._SF_DDL)
        except Exception as exc:
            raise SlackIngestError(f"Could not create SLACK_MESSAGES table: {exc}") from exc

    @staticmethod
    def _handle_slack_sdk_error(method: str, exc) -> None:
        error_code = getattr(exc.response, "data", {}).get("error", str(exc))
        if error_code == "ratelimited":
            retry_after = int(exc.response.headers.get("Retry-After", "60"))
            log.warning("Slack rate-limited on %s. Sleeping %ds …", method, retry_after)
            time.sleep(retry_after)
            raise SlackAPIError(method, error_code, f"rate-limited, retry after {retry_after}s")
        raise SlackAPIError(method, error_code)
