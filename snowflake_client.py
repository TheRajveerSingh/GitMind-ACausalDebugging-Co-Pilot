"""
snowflake_client.py — Snowflake connection manager for GitMind
==============================================================
Provides:
  • SnowflakeClient  — thin wrapper around snowflake-connector-python
  • SnowflakeStats   — ingestion / query statistics written to Snowflake itself
  • Strict, descriptive errors for every failure mode

Install:
    pip install "snowflake-connector-python[pandas]" cryptography
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Generator, Optional

log = logging.getLogger("gitmind.snowflake")

# ---------------------------------------------------------------------------
# Lazy import so the rest of the app works even when the connector is absent
# ---------------------------------------------------------------------------

def _import_snowflake():
    try:
        import snowflake.connector as sf
        return sf
    except ImportError as exc:
        raise ImportError(
            "snowflake-connector-python is not installed. "
            "Run: pip install 'snowflake-connector-python[pandas]'"
        ) from exc


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------

class SnowflakeError(Exception):
    """Base class for all Snowflake-related GitMind errors."""


class SnowflakeConnectionError(SnowflakeError):
    """Raised when a Snowflake connection cannot be established."""


class SnowflakeQueryError(SnowflakeError):
    """Raised when a Snowflake query fails."""


class SnowflakeIngestionError(SnowflakeError):
    """Raised when a batch write to Snowflake fails."""


# ---------------------------------------------------------------------------
# Stats record (written back to Snowflake for observability)
# ---------------------------------------------------------------------------

@dataclass
class IngestStats:
    source: str                        # "slack" | "jira" | "github"
    run_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    records_fetched: int = 0
    records_inserted: int = 0
    records_skipped: int = 0
    records_errored: int = 0
    error_message: str = ""

    def finish(self, error: str = "") -> None:
        self.finished_at = datetime.now(timezone.utc)
        self.error_message = error

    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    def to_row(self) -> dict[str, Any]:
        return {
            "SOURCE": self.source,
            "RUN_ID": self.run_id,
            "STARTED_AT": self.started_at.isoformat(),
            "FINISHED_AT": self.finished_at.isoformat() if self.finished_at else None,
            "DURATION_SECONDS": self.duration_seconds,
            "RECORDS_FETCHED": self.records_fetched,
            "RECORDS_INSERTED": self.records_inserted,
            "RECORDS_SKIPPED": self.records_skipped,
            "RECORDS_ERRORED": self.records_errored,
            "SUCCESS": not bool(self.error_message),
            "ERROR_MESSAGE": self.error_message,
        }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SnowflakeClient:
    """
    Thread-safe Snowflake client for GitMind.

    Usage
    -----
        from config import SnowflakeConfig
        cfg = SnowflakeConfig.from_env()
        client = SnowflakeClient(cfg)

        with client.cursor() as cur:
            cur.execute("SELECT CURRENT_VERSION()")
            print(cur.fetchone())
    """

    # DDL run once to ensure the stats table exists
    _STATS_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS GITMIND_INGEST_STATS (
        SOURCE           VARCHAR(64)   NOT NULL,
        RUN_ID           VARCHAR(128)  NOT NULL,
        STARTED_AT       TIMESTAMP_TZ  NOT NULL,
        FINISHED_AT      TIMESTAMP_TZ,
        DURATION_SECONDS FLOAT,
        RECORDS_FETCHED  INTEGER       DEFAULT 0,
        RECORDS_INSERTED INTEGER       DEFAULT 0,
        RECORDS_SKIPPED  INTEGER       DEFAULT 0,
        RECORDS_ERRORED  INTEGER       DEFAULT 0,
        SUCCESS          BOOLEAN       DEFAULT FALSE,
        ERROR_MESSAGE    VARCHAR(4096) DEFAULT '',
        PRIMARY KEY (SOURCE, RUN_ID)
    )
    """

    def __init__(self, config) -> None:  # config: SnowflakeConfig
        self._config = config
        self._conn = None
        self._connect()
        self._ensure_stats_table()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        sf = _import_snowflake()
        cfg = self._config

        connect_kwargs: dict[str, Any] = {
            "account": cfg.account,
            "user": cfg.user,
            "warehouse": cfg.warehouse,
            "database": cfg.database,
            "schema": cfg.schema,
            "role": cfg.role,
            # Network resilience
            "login_timeout": 30,
            "network_timeout": 120,
            "socket_timeout": 90,
            "client_session_keep_alive": True,
        }

        # Auth: private-key takes precedence over password
        if cfg.private_key_path:
            connect_kwargs["private_key"] = self._load_private_key(
                cfg.private_key_path, cfg.private_key_passphrase
            )
        elif cfg.password:
            connect_kwargs["password"] = cfg.password
        else:
            raise SnowflakeConnectionError(
                "No Snowflake authentication method configured. "
                "Provide SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH."
            )

        log.info(
            "Connecting to Snowflake: account=%s database=%s schema=%s warehouse=%s role=%s",
            cfg.account, cfg.database, cfg.schema, cfg.warehouse, cfg.role,
        )

        try:
            self._conn = sf.connect(**connect_kwargs)
            log.info("Snowflake connection established.")
        except sf.errors.DatabaseError as exc:
            # Snowflake error codes: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-error-codes
            msg = str(exc)
            if "250001" in msg or "Incorrect username or password" in msg:
                raise SnowflakeConnectionError(
                    f"Snowflake authentication failed for user '{cfg.user}'. "
                    "Check SNOWFLAKE_PASSWORD / key-pair settings."
                ) from exc
            if "250006" in msg or "IP not whitelisted" in msg:
                raise SnowflakeConnectionError(
                    f"Snowflake rejected this IP address. "
                    "Add this machine's IP to the network policy for account '{cfg.account}'."
                ) from exc
            if "404" in msg or "does not exist" in msg.lower():
                raise SnowflakeConnectionError(
                    f"Snowflake account '{cfg.account}' not found. "
                    "Verify SNOWFLAKE_ACCOUNT (format: <org>-<locator>)."
                ) from exc
            raise SnowflakeConnectionError(
                f"Failed to connect to Snowflake: {exc}"
            ) from exc
        except Exception as exc:
            raise SnowflakeConnectionError(
                f"Unexpected error connecting to Snowflake: {type(exc).__name__}: {exc}"
            ) from exc

    @staticmethod
    def _load_private_key(path: str, passphrase: str) -> bytes:
        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives.serialization import (
                Encoding, NoEncryption, PrivateFormat,
                load_pem_private_key,
            )
        except ImportError as exc:
            raise ImportError(
                "cryptography is required for Snowflake key-pair auth. "
                "Run: pip install cryptography"
            ) from exc

        try:
            with open(path, "rb") as fh:
                pem_data = fh.read()
        except OSError as exc:
            raise SnowflakeConnectionError(
                f"Cannot read Snowflake private key at '{path}': {exc}"
            ) from exc

        passphrase_bytes = passphrase.encode() if passphrase else None
        try:
            private_key = load_pem_private_key(pem_data, passphrase_bytes, default_backend())
        except Exception as exc:
            raise SnowflakeConnectionError(
                f"Failed to parse Snowflake private key at '{path}': {exc}. "
                "Ensure the key format is PEM and the passphrase (if any) is correct."
            ) from exc

        return private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )

    def reconnect(self) -> None:
        """Re-establish a dropped connection."""
        log.warning("Reconnecting to Snowflake …")
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._connect()

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
                log.info("Snowflake connection closed.")
            except Exception as exc:
                log.warning("Error closing Snowflake connection: %s", exc)

    # ------------------------------------------------------------------
    # Cursor context manager
    # ------------------------------------------------------------------

    @contextmanager
    def cursor(self) -> Generator:
        """Yield a DictCursor; raise SnowflakeQueryError on failure."""
        sf = _import_snowflake()
        cur = self._conn.cursor(sf.DictCursor)
        try:
            yield cur
        except sf.errors.ProgrammingError as exc:
            raise SnowflakeQueryError(
                f"Snowflake query error: {exc}"
            ) from exc
        except sf.errors.DatabaseError as exc:
            # Try to reconnect once on a session error, then re-raise
            if "session no longer exists" in str(exc).lower():
                log.warning("Snowflake session expired; reconnecting and retrying …")
                self.reconnect()
                raise SnowflakeQueryError(
                    "Snowflake session expired. The connection was reset; please retry."
                ) from exc
            raise SnowflakeQueryError(f"Snowflake database error: {exc}") from exc
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: tuple = ()) -> list[dict]:
        """Run *sql* and return all rows as a list of dicts."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() or []

    def execute_many(self, sql: str, rows: list[tuple]) -> int:
        """Bulk-insert *rows* using executemany; return count inserted."""
        if not rows:
            return 0
        with self.cursor() as cur:
            cur.executemany(sql, rows)
            return len(rows)

    # ------------------------------------------------------------------
    # Stats table
    # ------------------------------------------------------------------

    def _ensure_stats_table(self) -> None:
        try:
            self.execute(self._STATS_TABLE_DDL)
        except SnowflakeQueryError as exc:
            raise SnowflakeIngestionError(
                f"Could not create GITMIND_INGEST_STATS table: {exc}"
            ) from exc

    def write_stats(self, stats: IngestStats) -> None:
        """Upsert an IngestStats record into GITMIND_INGEST_STATS."""
        row = stats.to_row()
        sql = """
        MERGE INTO GITMIND_INGEST_STATS AS tgt
        USING (SELECT %s AS SOURCE, %s AS RUN_ID) AS src
          ON tgt.SOURCE = src.SOURCE AND tgt.RUN_ID = src.RUN_ID
        WHEN MATCHED THEN UPDATE SET
            FINISHED_AT      = %s,
            DURATION_SECONDS = %s,
            RECORDS_FETCHED  = %s,
            RECORDS_INSERTED = %s,
            RECORDS_SKIPPED  = %s,
            RECORDS_ERRORED  = %s,
            SUCCESS          = %s,
            ERROR_MESSAGE    = %s
        WHEN NOT MATCHED THEN INSERT (
            SOURCE, RUN_ID, STARTED_AT, FINISHED_AT, DURATION_SECONDS,
            RECORDS_FETCHED, RECORDS_INSERTED, RECORDS_SKIPPED,
            RECORDS_ERRORED, SUCCESS, ERROR_MESSAGE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, (
                    # WHEN MATCHED SET
                    row["SOURCE"], row["RUN_ID"],
                    row["FINISHED_AT"], row["DURATION_SECONDS"],
                    row["RECORDS_FETCHED"], row["RECORDS_INSERTED"],
                    row["RECORDS_SKIPPED"], row["RECORDS_ERRORED"],
                    row["SUCCESS"], row["ERROR_MESSAGE"],
                    # WHEN NOT MATCHED INSERT
                    row["SOURCE"], row["RUN_ID"], row["STARTED_AT"],
                    row["FINISHED_AT"], row["DURATION_SECONDS"],
                    row["RECORDS_FETCHED"], row["RECORDS_INSERTED"],
                    row["RECORDS_SKIPPED"], row["RECORDS_ERRORED"],
                    row["SUCCESS"], row["ERROR_MESSAGE"],
                ))
        except SnowflakeQueryError as exc:
            # Non-fatal: log and continue — stats failure shouldn't crash ingest
            log.error("Failed to write ingest stats to Snowflake: %s", exc)

    def get_stats_summary(self, source: Optional[str] = None, limit: int = 50) -> list[dict]:
        """Return recent ingest stats, optionally filtered by source."""
        where = "WHERE SOURCE = %s" if source else ""
        params = (source,) if source else ()
        sql = f"""
        SELECT * FROM GITMIND_INGEST_STATS
        {where}
        ORDER BY STARTED_AT DESC
        LIMIT {limit}
        """
        return self.execute(sql, params)
