from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pipeline_runtime import ATTEMPT_STATUSES, request_fingerprint as build_request_fingerprint


def now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_alias_value(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip().casefold()
    return re.sub(r"\s+", " ", text)


def company_short_name(company: str) -> str:
    name = re.sub(r"\(.*?\)|（.*?）", "", str(company or "")).strip()
    return re.sub(r"(有限公司|有限责任公司|股份有限公司)$", "", name).strip() or str(company or "").strip()


class PipelineStore:
    """Durable control-plane store used only by the main agent."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=60000")

    def init(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                input_file TEXT NOT NULL, config_json TEXT NOT NULL,
                started_at TEXT NOT NULL, finished_at TEXT
            );
            CREATE TABLE IF NOT EXISTS companies (
                run_id TEXT NOT NULL, company_id TEXT NOT NULL,
                company_name TEXT NOT NULL, status TEXT NOT NULL,
                phase TEXT NOT NULL, updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, company_id), FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, company_id TEXT NOT NULL,
                phase TEXT NOT NULL, round_no INTEGER NOT NULL, platform TEXT,
                status TEXT NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0,
                retry_limit INTEGER NOT NULL DEFAULT 3, grant_count INTEGER NOT NULL DEFAULT 0,
                grant_limit INTEGER NOT NULL DEFAULT 1, payload_json TEXT NOT NULL,
                request_fingerprint TEXT,
                result_json TEXT, error_code TEXT, error_message TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                UNIQUE(run_id, company_id, phase, round_no, platform, payload_json),
                FOREIGN KEY(run_id, company_id) REFERENCES companies(run_id, company_id)
            );
            CREATE TABLE IF NOT EXISTS task_attempts (
                attempt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt_no INTEGER NOT NULL,
                status TEXT NOT NULL, lease_id TEXT, lease_version INTEGER NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL, finished_at TEXT, returncode INTEGER,
                timed_out INTEGER NOT NULL DEFAULT 0, error_code TEXT, error_message TEXT,
                summary_json TEXT, output_manifest_json TEXT,
                execution_mode TEXT NOT NULL DEFAULT 'PROCESS', request_fingerprint TEXT,
                source_run_id TEXT, source_task_id TEXT, source_attempt_id TEXT,
                reuse_match_kind TEXT,
                FOREIGN KEY(task_id) REFERENCES tasks(task_id)
            );
            CREATE TABLE IF NOT EXISTS resource_states (
                resource_type TEXT NOT NULL, resource_key TEXT NOT NULL,
                status TEXT NOT NULL, capacity INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL, detail_json TEXT,
                PRIMARY KEY(resource_type, resource_key)
            );
            CREATE TABLE IF NOT EXISTS resource_waiters (
                waiter_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                resource_type TEXT NOT NULL, resource_key TEXT NOT NULL,
                status TEXT NOT NULL, queued_at TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES tasks(task_id)
            );
            CREATE TABLE IF NOT EXISTS resource_leases (
                lease_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                resource_type TEXT NOT NULL, resource_key TEXT NOT NULL,
                lease_version INTEGER NOT NULL, status TEXT NOT NULL,
                expires_at TEXT NOT NULL, created_at TEXT NOT NULL, released_at TEXT,
                FOREIGN KEY(task_id) REFERENCES tasks(task_id)
            );
            CREATE TABLE IF NOT EXISTS attempt_output_items (
                attempt_id TEXT NOT NULL, seq INTEGER NOT NULL, item_key TEXT NOT NULL,
                item_kind TEXT NOT NULL, data_json TEXT NOT NULL, consumed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(attempt_id, seq), FOREIGN KEY(attempt_id) REFERENCES task_attempts(attempt_id)
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, time TEXT NOT NULL, scope TEXT NOT NULL,
                target_id TEXT NOT NULL, event TEXT NOT NULL, payload_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS control_commands (
                command_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                action TEXT NOT NULL, company_id TEXT NOT NULL, phase TEXT NOT NULL,
                reason TEXT NOT NULL, status TEXT NOT NULL,
                created_at TEXT NOT NULL, applied_at TEXT, result_json TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(run_id)
            );
            CREATE TABLE IF NOT EXISTS enterprise_aliases (
                company_id TEXT NOT NULL,
                alias_type TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT '',
                alias_value TEXT NOT NULL,
                normalized_value TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_ref TEXT,
                confidence REAL NOT NULL DEFAULT 1.0,
                is_confirmed INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(company_id, alias_type, platform, normalized_value)
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_ready ON tasks(run_id, company_id, status, phase);
            CREATE INDEX IF NOT EXISTS idx_leases_active ON resource_leases(resource_type, resource_key, status);
            CREATE INDEX IF NOT EXISTS idx_control_commands_pending
                ON control_commands(run_id, status, created_at);
            CREATE INDEX IF NOT EXISTS idx_enterprise_aliases_lookup
                ON enterprise_aliases(company_id, platform, active, alias_type);
            """
        )
        # Forward-only, idempotent migration for databases created by older runs.
        self._ensure_column("tasks", "request_fingerprint", "TEXT")
        self._ensure_column("task_attempts", "execution_mode", "TEXT NOT NULL DEFAULT 'PROCESS'")
        self._ensure_column("task_attempts", "request_fingerprint", "TEXT")
        self._ensure_column("task_attempts", "source_run_id", "TEXT")
        self._ensure_column("task_attempts", "source_task_id", "TEXT")
        self._ensure_column("task_attempts", "source_attempt_id", "TEXT")
        self._ensure_column("task_attempts", "reuse_match_kind", "TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_fingerprint ON tasks(run_id, request_fingerprint)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_company_history "
            "ON tasks(company_id, phase, platform, run_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_attempts_history "
            "ON task_attempts(task_id, status, finished_at)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_output_items_attempt ON attempt_output_items(attempt_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_companies_history "
            "ON companies(company_id, run_id)"
        )
        self._backfill_base_enterprise_aliases()
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        columns = {str(row["name"]) for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def _backfill_base_enterprise_aliases(self) -> None:
        now = now_text()
        rows: list[tuple[Any, ...]] = []
        for row in self.conn.execute("SELECT DISTINCT company_id,company_name FROM companies"):
            for alias_type, value in (
                ("legal_name", str(row["company_name"])),
                ("short_name", company_short_name(str(row["company_name"]))),
            ):
                normalized = normalize_alias_value(value)
                if normalized:
                    rows.append((
                        str(row["company_id"]), alias_type, "", value, normalized,
                        "company_history", None, 1.0, 1, 1, now, now,
                    ))
        self.conn.executemany(
            """INSERT INTO enterprise_aliases(
                company_id,alias_type,platform,alias_value,normalized_value,
                source_type,source_ref,confidence,is_confirmed,active,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(company_id,alias_type,platform,normalized_value) DO NOTHING""",
            rows,
        )

    def close(self) -> None:
        self.conn.close()

    def _json(self, value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    def _resolve_attempt_summary(self, attempt_id: str) -> str | None:
        """返回 attempt 的 summary_json；列里为空则回退到 attempt_output_items。"""
        row = self.conn.execute(
            "SELECT summary_json FROM task_attempts WHERE attempt_id=?", (attempt_id,)
        ).fetchone()
        if row and row["summary_json"]:
            return row["summary_json"]
        row = self.conn.execute(
            "SELECT data_json FROM attempt_output_items WHERE attempt_id=? ORDER BY seq DESC LIMIT 1",
            (attempt_id,),
        ).fetchone()
        return row["data_json"] if row else None

    def _resolve_task_result(self, task_id: str) -> str | None:
        """返回 task 的 result_json；列里为空则回退到该 task 最新成功 attempt 的 data_json。"""
        row = self.conn.execute(
            "SELECT result_json FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if row and row["result_json"]:
            return row["result_json"]
        row = self.conn.execute(
            """SELECT i.data_json
               FROM attempt_output_items i
               JOIN task_attempts ta ON ta.attempt_id = i.attempt_id
               WHERE ta.task_id=? AND ta.status IN ('SUCCEEDED','EMPTY_SUCCESS')
               ORDER BY ta.attempt_no DESC, i.seq DESC LIMIT 1""",
            (task_id,),
        ).fetchone()
        return row["data_json"] if row else None

    def event(self, event_id: str, scope: str, target_id: str, name: str, payload: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, now_text(), scope, target_id, name, self._json(payload)),
        )
        self.conn.commit()

    def create_run(self, run_id: str, input_file: str, config: dict[str, Any], companies: Iterable[tuple[str, str]]) -> None:
        now = now_text()
        company_rows = list(companies)
        with self.conn:
            self.conn.execute(
                "INSERT INTO runs VALUES (?, 'RUNNING', ?, ?, ?, NULL)",
                (run_id, input_file, self._json(config), now),
            )
            self.conn.executemany(
                "INSERT INTO companies VALUES (?, ?, ?, 'OPEN', 'WEB_ICP', ?)",
                [(run_id, cid, name, now) for cid, name in company_rows],
            )
            aliases: list[tuple[Any, ...]] = []
            for cid, name in company_rows:
                for alias_type, value in (("legal_name", name), ("short_name", company_short_name(name))):
                    normalized = normalize_alias_value(value)
                    if not normalized:
                        continue
                    aliases.append((
                        cid, alias_type, "", value, normalized, "run_input", input_file,
                        1.0, 1, 1, now, now,
                    ))
            self.conn.executemany(
                """INSERT INTO enterprise_aliases(
                    company_id,alias_type,platform,alias_value,normalized_value,
                    source_type,source_ref,confidence,is_confirmed,active,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(company_id,alias_type,platform,normalized_value) DO UPDATE SET
                    alias_value=excluded.alias_value,
                    source_type=CASE
                        WHEN enterprise_aliases.is_confirmed=1 AND excluded.is_confirmed=0
                        THEN enterprise_aliases.source_type ELSE excluded.source_type END,
                    source_ref=CASE
                        WHEN enterprise_aliases.is_confirmed=1 AND excluded.is_confirmed=0
                        THEN enterprise_aliases.source_ref
                        ELSE COALESCE(excluded.source_ref,enterprise_aliases.source_ref) END,
                    confidence=MAX(enterprise_aliases.confidence,excluded.confidence),
                    is_confirmed=MAX(enterprise_aliases.is_confirmed,excluded.is_confirmed),
                    active=1,
                    updated_at=excluded.updated_at""",
                aliases,
            )

    def upsert_enterprise_alias(
        self,
        company_id: str,
        alias_type: str,
        alias_value: str,
        *,
        platform: str | None = None,
        source_type: str,
        source_ref: str | None = None,
        confidence: float = 1.0,
        is_confirmed: bool = False,
    ) -> bool:
        value = str(alias_value or "").strip()
        normalized = normalize_alias_value(value)
        if not company_id or not alias_type or not value or not normalized:
            return False
        now = now_text()
        with self.conn:
            self.conn.execute(
                """INSERT INTO enterprise_aliases(
                    company_id,alias_type,platform,alias_value,normalized_value,
                    source_type,source_ref,confidence,is_confirmed,active,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,1,?,?)
                ON CONFLICT(company_id,alias_type,platform,normalized_value) DO UPDATE SET
                    alias_value=excluded.alias_value,
                    source_type=CASE
                        WHEN enterprise_aliases.is_confirmed=1 AND excluded.is_confirmed=0
                        THEN enterprise_aliases.source_type ELSE excluded.source_type END,
                    source_ref=CASE
                        WHEN enterprise_aliases.is_confirmed=1 AND excluded.is_confirmed=0
                        THEN enterprise_aliases.source_ref
                        ELSE COALESCE(excluded.source_ref,enterprise_aliases.source_ref) END,
                    confidence=MAX(enterprise_aliases.confidence,excluded.confidence),
                    is_confirmed=MAX(enterprise_aliases.is_confirmed,excluded.is_confirmed),
                    active=1,
                    updated_at=excluded.updated_at""",
                (
                    company_id, alias_type, str(platform or ""), value, normalized,
                    source_type, source_ref, max(0.0, min(1.0, float(confidence))),
                    1 if is_confirmed else 0, now, now,
                ),
            )
        return True

    def list_enterprise_aliases(self, company_id: str, platform: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [company_id]
        where = "company_id=? AND active=1"
        if platform is not None:
            where += " AND platform IN ('', ?)"
            params.append(str(platform))
        rows = self.conn.execute(
            f"""SELECT company_id,alias_type,platform,alias_value,normalized_value,
                       source_type,source_ref,confidence,is_confirmed,active,created_at,updated_at
                FROM enterprise_aliases WHERE {where}
                ORDER BY is_confirmed DESC, confidence DESC, alias_type, alias_value""",
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def deactivate_enterprise_alias(
        self, company_id: str, alias_type: str, alias_value: str, platform: str | None = None,
    ) -> bool:
        with self.conn:
            cursor = self.conn.execute(
                """UPDATE enterprise_aliases SET active=0,updated_at=?
                   WHERE company_id=? AND alias_type=? AND platform=? AND normalized_value=?""",
                (
                    now_text(), company_id, alias_type, str(platform or ""),
                    normalize_alias_value(alias_value),
                ),
            )
        return cursor.rowcount > 0

    def create_task(self, task_id: str, run_id: str, company_id: str, phase: str, round_no: int,
                    platform: str | None, payload: dict[str, Any], retry_limit: int = 3,
                    request_fingerprint: str | None = None) -> bool:
        """Persist a task before any child rows are written.

        Re-registering the same ``task_id`` is idempotent only when its durable
        identity is unchanged.  Other integrity errors must remain visible: in
        particular, swallowing a foreign-key or logical-request conflict here
        only moves the failure to a less useful child insert.
        """
        now = now_text()
        payload_json = self._json(payload)
        with self.conn:
            cursor = self.conn.execute(
                """INSERT INTO tasks
                (task_id, run_id, company_id, phase, round_no, platform, status,
                 retry_limit, payload_json, request_fingerprint, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO NOTHING""",
                (task_id, run_id, company_id, phase, round_no, platform, retry_limit,
                 payload_json, request_fingerprint, now, now),
            )
            if cursor.rowcount == 1:
                return True

            existing = self.get_task(task_id)
            if existing is None:  # Defensive: the row cannot disappear on this connection mid-transaction.
                raise RuntimeError(f"task {task_id} disappeared while being registered")
            expected = {
                "run_id": run_id,
                "company_id": company_id,
                "phase": phase,
                "round_no": round_no,
                "platform": platform,
                "payload_json": payload_json,
                "retry_limit": retry_limit,
            }
            mismatches = {
                key: {"stored": existing[key], "requested": value}
                for key, value in expected.items()
                if existing[key] != value
            }
            stored_fingerprint = existing["request_fingerprint"]
            if stored_fingerprint and request_fingerprint and stored_fingerprint != request_fingerprint:
                mismatches["request_fingerprint"] = {
                    "stored": stored_fingerprint,
                    "requested": request_fingerprint,
                }
            if mismatches:
                raise ValueError(
                    f"task_id {task_id} is already registered with different identity: "
                    f"{self._json(mismatches)}"
                )
            if not stored_fingerprint and request_fingerprint:
                self.conn.execute(
                    "UPDATE tasks SET request_fingerprint=?, updated_at=? WHERE task_id=?",
                    (request_fingerprint, now, task_id),
                )
            return False

    def get_task(self, task_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()

    def queue_resource(self, waiter_id: str, task_id: str, resource_type: str, resource_key: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO resource_waiters VALUES (?, ?, ?, ?, 'QUEUED', ?)",
                (waiter_id, task_id, resource_type, resource_key, now_text()),
            )
            self.conn.execute("UPDATE tasks SET status='WAITING_RESOURCE', updated_at=? WHERE task_id=?", (now_text(), task_id))

    def acquire_resource_lease(self, waiter_id: str, lease_id: str, task_id: str,
                               resource_type: str, resource_key: str,
                               lease_version: int, expires_at: str) -> None:
        """Atomically queue and grant a resource lease for an existing task."""
        now = now_text()
        with self.conn:
            self.conn.execute(
                "INSERT INTO resource_states VALUES (?, ?, 'LEASED', 1, ?, '{}') "
                "ON CONFLICT(resource_type, resource_key) DO UPDATE SET "
                "status=excluded.status, capacity=excluded.capacity, updated_at=excluded.updated_at",
                (resource_type, resource_key, now),
            )
            self.conn.execute(
                "INSERT INTO resource_waiters VALUES (?, ?, ?, ?, 'QUEUED', ?)",
                (waiter_id, task_id, resource_type, resource_key, now),
            )
            self.conn.execute(
                "UPDATE resource_waiters SET status='GRANTED' WHERE waiter_id=?",
                (waiter_id,),
            )
            self.conn.execute(
                "INSERT INTO resource_leases VALUES (?, ?, ?, ?, ?, 'LEASED', ?, ?, NULL)",
                (lease_id, task_id, resource_type, resource_key, lease_version, expires_at, now),
            )
            updated = self.conn.execute(
                "UPDATE tasks SET status='RUNNING', grant_count=grant_count+1, updated_at=? WHERE task_id=?",
                (now, task_id),
            )
            if updated.rowcount != 1:
                raise LookupError(f"cannot grant resource lease for missing task {task_id}")

    def set_resource(self, resource_type: str, resource_key: str, status: str, capacity: int = 1) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO resource_states VALUES (?, ?, ?, ?, ?, '{}') "
                "ON CONFLICT(resource_type, resource_key) DO UPDATE SET status=excluded.status, capacity=excluded.capacity, updated_at=excluded.updated_at",
                (resource_type, resource_key, status, capacity, now_text()),
            )

    def grant_lease(self, lease_id: str, task_id: str, resource_type: str, resource_key: str,
                   lease_version: int, expires_at: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE resource_waiters SET status='GRANTED' WHERE task_id=? AND status='QUEUED' AND resource_type=? AND resource_key=?",
                (task_id, resource_type, resource_key),
            )
            self.conn.execute(
                "INSERT INTO resource_leases VALUES (?, ?, ?, ?, ?, 'LEASED', ?, ?, NULL)",
                (lease_id, task_id, resource_type, resource_key, lease_version, expires_at, now_text()),
            )
            self.conn.execute("UPDATE tasks SET status='RUNNING', grant_count=grant_count+1, updated_at=? WHERE task_id=?", (now_text(), task_id))

    def release_lease(self, lease_id: str, status: str = "RELEASED") -> None:
        with self.conn:
            self.conn.execute("UPDATE resource_leases SET status=?, released_at=? WHERE lease_id=?", (status, now_text(), lease_id))

    def start_attempt(self, attempt_id: str, task_id: str, attempt_no: int, lease_id: str, lease_version: int,
                      *, execution_mode: str = "PROCESS", request_fingerprint: str | None = None,
                      source_run_id: str | None = None, source_task_id: str | None = None,
                      source_attempt_id: str | None = None, reuse_match_kind: str | None = None) -> None:
        with self.conn:
            self.conn.execute(
                """INSERT INTO task_attempts
                (attempt_id, task_id, attempt_no, status, lease_id, lease_version, started_at,
                 execution_mode, request_fingerprint, source_run_id, source_task_id,
                 source_attempt_id, reuse_match_kind)
                VALUES (?, ?, ?, 'RUNNING', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (attempt_id, task_id, attempt_no, lease_id, lease_version, now_text(),
                 execution_mode, request_fingerprint, source_run_id, source_task_id,
                 source_attempt_id, reuse_match_kind),
            )

    def interrupt_running_attempts(self, task_id: str, error_code: str, message: str) -> int:
        """Close attempts abandoned by an unexpected worker exception and requeue the task.

        即使没有 RUNNING attempt（worker 在 start_attempt 之前就崩）也要计一次重试，
        否则 retry_count 停在 0，主循环的预算判定永远无法触达，任务会无限重试。
        """
        now = now_text()
        with self.conn:
            running = int(self.conn.execute(
                "SELECT COUNT(*) FROM task_attempts WHERE task_id=? AND status='RUNNING'",
                (task_id,),
            ).fetchone()[0])
            if running:
                self.conn.execute(
                    """UPDATE task_attempts
                    SET status='INTERRUPTED', finished_at=?, error_code=?, error_message=?
                    WHERE task_id=? AND status='RUNNING'""",
                    (now, error_code, message, task_id),
                )
            self.conn.execute(
                """UPDATE tasks
                SET status='RETRY_WAIT', retry_count=retry_count+1,
                    error_code=?, error_message=?, updated_at=?
                WHERE task_id=?""",
                (error_code, message, now, task_id),
            )
            return running

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()

    def latest_running_run(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM runs WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    def latest_finished_run(self) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM runs WHERE finished_at IS NOT NULL ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    def latest_started_run(self) -> sqlite3.Row | None:
        # 最新一次 run（不论状态）：resume 语义，覆盖被中断的 RUNNING/INCOMPLETE 僵尸。
        return self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    def run_companies(self, run_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT company_id, company_name FROM companies WHERE run_id=? ORDER BY rowid",
            (run_id,),
        ).fetchall())

    def historical_companies(self) -> list[sqlite3.Row]:
        """Return one canonical row for every company seen in any past run."""
        return list(self.conn.execute(
            """
            SELECT c.company_id, c.company_name, r.run_id AS latest_run_id,
                   r.started_at AS latest_started_at
            FROM companies c
            JOIN runs r ON r.run_id=c.run_id
            WHERE c.rowid=(
                SELECT c2.rowid
                FROM companies c2
                JOIN runs r2 ON r2.run_id=c2.run_id
                WHERE c2.company_id=c.company_id
                ORDER BY r2.started_at DESC, c2.rowid DESC
                LIMIT 1
            )
            ORDER BY r.started_at DESC, c.rowid
            """
        ).fetchall())

    def create_control_command(self, command_id: str, run_id: str, action: str,
                               company_id: str, phase: str, reason: str) -> None:
        with self.conn:
            self.conn.execute(
                """INSERT INTO control_commands
                (command_id, run_id, action, company_id, phase, reason, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
                (command_id, run_id, action, company_id, phase, reason, now_text()),
            )

    def pending_control_commands(self, run_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            """SELECT * FROM control_commands
            WHERE run_id=? AND status='PENDING' ORDER BY created_at, rowid""",
            (run_id,),
        ).fetchall())

    def finish_control_command(self, command_id: str, status: str,
                               result: dict[str, Any]) -> None:
        if status not in {"APPLIED", "REJECTED"}:
            raise ValueError(f"invalid control command status: {status}")
        with self.conn:
            self.conn.execute(
                """UPDATE control_commands
                SET status=?, applied_at=?, result_json=?
                WHERE command_id=? AND status='PENDING'""",
                (status, now_text(), self._json(result), command_id),
            )

    def run_tasks(self, run_id: str) -> list[sqlite3.Row]:
        """Return every task in creation order for deterministic rehydration."""
        return list(self.conn.execute(
            "SELECT rowid AS task_rowid, * FROM tasks WHERE run_id=? ORDER BY rowid",
            (run_id,),
        ).fetchall())

    def set_company_state(self, run_id: str, company_id: str, status: str, phase: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE companies SET status=?, phase=?, updated_at=? WHERE run_id=? AND company_id=?",
                (status, phase, now_text(), run_id, company_id),
            )

    def attempt_counts(self, run_id: str) -> dict[str, int]:
        rows = self.conn.execute(
            """
            SELECT t.task_id, COALESCE(MAX(ta.attempt_no), 0) AS attempt_count
            FROM tasks t LEFT JOIN task_attempts ta ON ta.task_id=t.task_id
            WHERE t.run_id=? GROUP BY t.task_id
            """,
            (run_id,),
        ).fetchall()
        return {str(row["task_id"]): int(row["attempt_count"]) for row in rows}

    def reusable_attempt(self, company_id: str, phase: str, platform: str | None,
                         exact_fingerprint: str,
                         legacy_fingerprint: str,
                         *, exclude_run_id: str) -> dict[str, Any] | None:
        """Return the company's latest matching success from all earlier runs."""
        rows = self.conn.execute(
            """
            SELECT ta.attempt_id, ta.attempt_no, ta.status AS attempt_status,
                   ta.summary_json, ta.output_manifest_json,
                   t.task_id, t.run_id AS source_run_id,
                   t.company_id, t.phase, t.platform, t.payload_json,
                   ta.request_fingerprint AS attempt_request_fingerprint,
                   t.request_fingerprint AS task_request_fingerprint,
                   c.company_name
            FROM task_attempts ta
            JOIN tasks t ON t.task_id=ta.task_id
            JOIN companies c ON c.run_id=t.run_id AND c.company_id=t.company_id
            JOIN runs r ON r.run_id=t.run_id
            WHERE t.run_id<>?
              AND t.company_id=? AND t.phase=? AND t.platform IS ?
              AND ta.status IN ('SUCCEEDED','EMPTY_SUCCESS')
            ORDER BY ta.finished_at DESC, r.started_at DESC, ta.rowid DESC
            """,
            (exclude_run_id, company_id, phase, platform),
        ).fetchall()
        for row in rows:
            stored = str(row["attempt_request_fingerprint"] or row["task_request_fingerprint"] or "")
            match_kind = "exact" if stored and stored == exact_fingerprint else ""
            if not match_kind and not stored:
                try:
                    payload = json.loads(row["payload_json"] or "{}")
                except json.JSONDecodeError:
                    continue
                source_exact = build_request_fingerprint(
                    str(row["company_id"]), str(row["phase"]), row["platform"],
                    payload if isinstance(payload, dict) else {}, hash_files=True,
                )
                legacy = build_request_fingerprint(
                    str(row["company_id"]), str(row["phase"]), row["platform"],
                    payload if isinstance(payload, dict) else {}, hash_files=False,
                )
                if source_exact == exact_fingerprint:
                    match_kind = "legacy_reconstructed_exact" if exact_fingerprint != legacy_fingerprint else "legacy_semantic"
                elif exact_fingerprint == legacy_fingerprint and legacy == legacy_fingerprint:
                    match_kind = "legacy_semantic"
            if not match_kind:
                continue
            result = dict(row)
            result["reuse_match_kind"] = match_kind
            # summary_json 可能已清空，回退到 attempt_output_items
            result["summary_json"] = self._resolve_attempt_summary(str(row["attempt_id"])) or row["summary_json"]
            return result
        return None


    def finish_attempt(self, attempt_id: str, task_id: str, status: str, returncode: int | None,
                       error_code: str | None, error_message: str | None, summary: dict[str, Any], manifest: list[dict[str, Any]]) -> None:
        terminal = status in ATTEMPT_STATUSES  # 单一真相：与 pipeline_runtime.ATTEMPT_STATUSES 对齐
        # 权威结果只存 attempt_output_items.data_json；tasks.result_json 与 task_attempts.summary_json
        # 留空，避免三份字节级相同副本撑爆 DB。读取时通过 _resolve_* 自动回退。
        with self.conn:
            self.conn.execute(
                "UPDATE task_attempts SET status=?, finished_at=?, returncode=?, error_code=?, error_message=?, summary_json=?, output_manifest_json=? WHERE attempt_id=?",
                (status, now_text(), returncode, error_code, error_message, "", self._json(manifest), attempt_id),
            )
            self.conn.execute(
                "UPDATE tasks SET status=?, result_json=?, error_code=?, error_message=?, updated_at=? WHERE task_id=?",
                ('TERMINAL' if terminal else status, "", error_code, error_message, now_text(), task_id),
            )


    def successful_attempts(self, company_id: str, phase: str) -> list[sqlite3.Row]:
        # 成功判据的权威源：返回该公司该阶段下 status 为 SUCCEEDED/EMPTY_SUCCESS 的 attempt（task_id, attempt_no）。
        return list(self.conn.execute(
            """
            SELECT ta.task_id, ta.attempt_no
            FROM task_attempts ta
            JOIN tasks t ON t.task_id = ta.task_id
            WHERE t.company_id=? AND t.phase=? AND ta.status IN ('SUCCEEDED','EMPTY_SUCCESS')
            ORDER BY ta.task_id, ta.attempt_no
            """,
            (company_id, phase),
        ).fetchall())

    def historical_content_attempts(
        self, company_id: str, phases: Iterable[str],
    ) -> list[sqlite3.Row]:
        """Return successful content attempts for this company across all runs.

        The final risk review is intentionally historical: a current run may
        legitimately produce no *new* posts because the crawler stopped at an
        existing anchor.  The control database is the authority for which
        attempts are valid; callers resolve the corresponding run artifact
        directory from ``run_id/task_id/attempt_no``.
        """
        phase_values = [str(phase) for phase in phases if str(phase).strip()]
        if not phase_values:
            return []
        placeholders = ",".join("?" for _ in phase_values)
        rows = list(self.conn.execute(
            f"""
            SELECT t.run_id, t.task_id, t.company_id, t.phase, t.round_no,
                   t.platform, t.payload_json, ta.attempt_id, ta.attempt_no,
                   ta.status AS attempt_status, ta.summary_json,
                   ta.execution_mode, ta.source_run_id
            FROM task_attempts ta
            JOIN tasks t ON t.task_id=ta.task_id
            WHERE t.company_id=?
              AND t.phase IN ({placeholders})
              AND ta.status IN ('SUCCEEDED','EMPTY_SUCCESS')
            ORDER BY COALESCE(ta.finished_at, ta.started_at), t.run_id,
                     t.task_id, ta.attempt_no
            """,
            (company_id, *phase_values),
        ).fetchall())
        return self._with_summary(rows)

    def _with_summary(self, rows: list[sqlite3.Row]) -> list[sqlite3.Row | dict[str, Any]]:
        """sqlite3.Row 不可变，把 summary_json 为空的结果用 attempt_output_items 回填。"""
        if not rows:
            return rows
        out: list[sqlite3.Row | dict[str, Any]] = []
        for row in rows:
            if row["summary_json"]:
                out.append(row)
                continue
            summary = self._resolve_attempt_summary(str(row["attempt_id"]))
            if not summary:
                out.append(row)
                continue
            # 不能再用 sqlite3.Row(cursor, ...) 重建：裸 cursor 的 description 为
            # None，造出的 Row 无列名，后续 row["phase"] 会抛 IndexError。
            # 直接返回 dict，键即列名，下游 row["..."] / dict(row) 照常可用。
            d = dict(row)
            d["summary_json"] = summary
            out.append(d)
        return out

    def output_item(self, attempt_id: str, seq: int, item_key: str, item_kind: str, data: dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO attempt_output_items VALUES (?, ?, ?, ?, ?, 0)",
                (attempt_id, seq, item_key, item_kind, self._json(data)),
            )

    def set_task_retry(self, task_id: str, error_code: str, message: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE tasks SET status='RETRY_WAIT', retry_count=retry_count+1, error_code=?, error_message=?, updated_at=? WHERE task_id=?",
                (error_code, message, now_text(), task_id),
            )

    def set_task_final_failure(self, task_id: str, error_code: str, message: str) -> None:
        """Close a task after its configured failure budget is exhausted."""
        with self.conn:
            self.conn.execute(
                "UPDATE tasks SET status='TERMINAL', error_code=?, error_message=?, updated_at=? WHERE task_id=?",
                (error_code, message, now_text(), task_id),
            )

    def active_tasks(self, run_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM tasks WHERE run_id=? AND status NOT IN ('TERMINAL') ORDER BY created_at", (run_id,)))

    def summary(self, run_id: str) -> dict[str, Any]:
        rows = self.conn.execute("SELECT status, COUNT(*) n FROM tasks WHERE run_id=? GROUP BY status", (run_id,)).fetchall()
        return {row["status"]: row["n"] for row in rows}

    def execution_summary(self, run_id: str) -> dict[str, int]:
        rows = self.conn.execute(
            """
            SELECT ta.execution_mode, COUNT(*) n
            FROM task_attempts ta JOIN tasks t ON t.task_id=ta.task_id
            WHERE t.run_id=? GROUP BY ta.execution_mode
            """,
            (run_id,),
        ).fetchall()
        return {str(row["execution_mode"]): int(row["n"]) for row in rows}

    def latest_successful_rankings(self, company_id: str, platform: str) -> sqlite3.Row | None:
        row = self.conn.execute(
            """
            SELECT run_id, round_no, result_json, created_at
            FROM tasks
            WHERE company_id=? AND phase='ACCOUNT_RANK' AND status='TERMINAL'
              AND result_json IS NOT NULL AND result_json != ''
              AND json_extract(result_json, '$.results.' || ? || '.top_accounts') IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (company_id, platform),
        ).fetchone()
        if row:
            return row
        # result_json 已被清空：从 attempt_output_items 回退
        row = self.conn.execute(
            """
            SELECT t.run_id, t.round_no, i.data_json AS result_json, t.created_at
            FROM tasks t
            JOIN task_attempts ta ON ta.task_id = t.task_id
            JOIN attempt_output_items i ON i.attempt_id = ta.attempt_id
            WHERE t.company_id=? AND t.phase='ACCOUNT_RANK' AND t.status='TERMINAL'
              AND (t.error_code IS NULL OR t.error_code='')
              AND ta.status IN ('SUCCEEDED','EMPTY_SUCCESS')
              AND json_extract(i.data_json, '$.results.' || ? || '.top_accounts') IS NOT NULL
            ORDER BY t.created_at DESC, ta.attempt_no DESC, i.seq DESC
            LIMIT 1
            """,
            (company_id, platform),
        ).fetchone()
        return row

    def historical_discovered_accounts(self, company_id: str, platform: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT t.task_id, t.result_json
            FROM tasks t
            WHERE t.company_id=? AND t.phase='ACCOUNT_DISCOVERY' AND t.platform=?
              AND t.status='TERMINAL' AND (t.error_code IS NULL OR t.error_code='')
            ORDER BY t.created_at
            """,
            (company_id, platform),
        ).fetchall()
        accounts: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            result_json = row["result_json"] or self._resolve_task_result(str(row["task_id"]))
            try:
                payload = json.loads(result_json or "{}")
            except json.JSONDecodeError:
                continue
            items = payload.get("accounts") or payload.get("results") or []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    key = str(item.get("sec_uid") or item.get("user_id") or item.get("kwai_id") or item.get("nickname") or "")
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    accounts.append(item)
        return accounts

    def historical_used_keywords(self, company_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT task_id, round_no, result_json
            FROM tasks
            WHERE company_id=? AND phase='KEYWORD' AND status='TERMINAL'
            ORDER BY created_at
            """,
            (company_id,),
        ).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            result_json = row["result_json"] or self._resolve_task_result(str(row["task_id"]))
            try:
                payload = json.loads(result_json or "{}")
            except json.JSONDecodeError:
                continue
            keywords = payload.get("keywords") if isinstance(payload, dict) else None
            if not isinstance(keywords, dict):
                continue
            for platform, value in keywords.items():
                platform_keywords = value if isinstance(value, list) else [value]
                for keyword in platform_keywords:
                    records.append({"round": row["round_no"], "platform": str(platform), "keyword": str(keyword or "")})
        return records

    def historical_post_crawl_results(self, company_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT task_id, platform, payload_json, result_json, error_code
            FROM tasks
            WHERE company_id=? AND phase='POST_CRAWL' AND status='TERMINAL'
            ORDER BY created_at
            """,
            (company_id,),
        ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            result_json = row["result_json"] or self._resolve_task_result(str(row["task_id"]))
            try:
                payload = json.loads(row["payload_json"] or "{}")
                result = json.loads(result_json or "{}")
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict) or not isinstance(result, dict):
                continue
            status = str(result.get("status") or "").lower()
            if row["error_code"] or status in {"login_required", "cookie_invalid", "login_expired", "rate_limited", "captcha_required", "error", "failed", "failed_final", "missing_output"}:
                continue
            results.append({
                "platform": row["platform"],
                "payload": payload,
                "result": result,
            })
        return results

    def finish_run(self, run_id: str, status: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE runs SET status=?, finished_at=? WHERE run_id=?", (status, now_text(), run_id))

    def mark_run_incomplete(self, run_id: str) -> None:
        """Mark a crashed run as resumable without crossing the finished boundary."""
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET status='INCOMPLETE' WHERE run_id=? AND finished_at IS NULL",
                (run_id,),
            )
