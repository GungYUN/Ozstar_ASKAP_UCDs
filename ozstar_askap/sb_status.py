"""Persistent SB-by-SB processing status for the Ozstar pipeline.

The database is the detailed processing record.  A row is created as soon as
CASDA returns an SB/beam row; step columns use ``Y``/``N``/NULL for success,
failure, and not-yet-reached respectively.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

try:
    from . import config
    from .pipeline_utils import utc_now
except ImportError:
    import config
    from pipeline_utils import utc_now


class StateDatabaseError(RuntimeError):
    """The durable SB status record could not be written."""


DATABASE_PATH = config.STATE_ROOT / "sb_processing.sqlite"
STEP_COLUMNS = (
    "casda_query",
    "archive_download",
    "checksum_download",
    "extract_archive",
    "worker_setup",
    "preprocess",
    "create_model",
    "insert_model",
    "subtract_model",
    "extract_ds",
    "crop_i",
    "crop_v",
    "product_copy",
    "cleanup",
)

PROCESSING_COLUMNS = (
    "preprocess",
    "create_model",
    "insert_model",
    "subtract_model",
    "extract_ds",
    "crop_i",
    "crop_v",
    "product_copy",
    "cleanup",
)

RETRY_STAGES = {
    "none",
    "processing_failed",
    "processing_retry_ready",
    "redownload_required",
    "redownload_retry_ready",
    "exhausted",
    "complete",
}


def _connect() -> sqlite3.Connection:
    try:
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(DATABASE_PATH, timeout=30)
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sb_processing (
                ucs_number INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                sb_id TEXT NOT NULL,
                beam TEXT NOT NULL DEFAULT '',
                ms_name TEXT,
                casda_query TEXT, archive_download TEXT, checksum_download TEXT,
                extract_archive TEXT, worker_setup TEXT, preprocess TEXT,
                create_model TEXT, insert_model TEXT, subtract_model TEXT,
                extract_ds TEXT, crop_i TEXT, crop_v TEXT, product_copy TEXT,
                cleanup TEXT,
                error_step TEXT, error_message TEXT, failure_log_path TEXT,
                casda_query_seconds REAL, archive_download_seconds REAL,
                checksum_download_seconds REAL, extract_archive_seconds REAL,
                preprocess_seconds REAL, create_model_seconds REAL,
                insert_model_seconds REAL, subtract_model_seconds REAL,
                extract_ds_seconds REAL, crop_i_seconds REAL, crop_v_seconds REAL,
                product_copy_seconds REAL, cleanup_seconds REAL,
                retry_stage TEXT NOT NULL DEFAULT 'none',
                processing_retry_count INTEGER NOT NULL DEFAULT 0,
                redownload_retry_count INTEGER NOT NULL DEFAULT 0,
                retry_updated_at TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                PRIMARY KEY (ucs_number, source_name, sb_id, beam)
            )
            """
        )
        # Migrate databases created before explicit failed-SB retry tracking
        # was introduced. SQLite only supports adding one column at a time.
        existing = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(sb_processing)")
        }
        migrations = {
            "retry_stage": "TEXT NOT NULL DEFAULT 'none'",
            "processing_retry_count": "INTEGER NOT NULL DEFAULT 0",
            "redownload_retry_count": "INTEGER NOT NULL DEFAULT 0",
            "retry_updated_at": "TEXT",
        }
        for name, definition in migrations.items():
            if name not in existing:
                connection.execute(
                    f"ALTER TABLE sb_processing ADD COLUMN {name} {definition}"
                )
        processing_placeholders = ",".join("?" for _ in PROCESSING_COLUMNS)
        connection.execute(
            "UPDATE sb_processing SET retry_stage='processing_failed', "
            "retry_updated_at=COALESCE(retry_updated_at, updated_at) "
            "WHERE retry_stage='none' AND COALESCE(product_copy, '') != 'Y' "
            f"AND error_step IN ({processing_placeholders})",
            PROCESSING_COLUMNS,
        )
        return connection
    except sqlite3.Error as exc:
        raise StateDatabaseError(f"Cannot open SB status database {DATABASE_PATH}: {exc}") from exc


def _text(value: Any) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else str(value)


def _beam(filename: str) -> str:
    match = re.search(r"beam(\d+)", filename, flags=re.IGNORECASE)
    return f"beam{match.group(1)}" if match else ""


def _sb_id(value: Any) -> str | None:
    match = re.search(r"(\d+)$", _text(value))
    return f"SB{match.group(1)}" if match else None


def initialize_source_rows(source: dict[str, Any], rows: Any, query_seconds: float | None = None) -> None:
    """Insert immutable SB identities immediately after the CASDA query."""

    now = utc_now()
    payload: list[tuple[Any, ...]] = []
    filenames = rows["filename"] if "filename" in rows.colnames else [""] * len(rows)
    for obs_id, filename in zip(rows["obs_id"], filenames):
        sb_id = _sb_id(obs_id)
        if sb_id is None:
            continue
        filename_text = _text(filename)
        payload.append(
            (
                int(source["ucs_number"]), source["source_name"], sb_id, _beam(filename_text),
                filename_text, "Y", query_seconds, now, now,
            )
        )
    if not payload:
        return
    try:
        with _connect() as connection:
            connection.executemany(
                """
                INSERT INTO sb_processing
                (ucs_number, source_name, sb_id, beam, ms_name, casda_query,
                 casda_query_seconds, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ucs_number, source_name, sb_id, beam) DO UPDATE SET
                    ms_name=excluded.ms_name,
                    casda_query='Y',
                    casda_query_seconds=excluded.casda_query_seconds,
                    updated_at=excluded.updated_at
                """,
                payload,
            )
    except sqlite3.Error as exc:
        raise StateDatabaseError(f"Cannot initialise SB status rows: {exc}") from exc


def record_step(
    source_name: str,
    sb_id: str,
    step: str,
    status: str,
    *,
    beam: str | None = None,
    seconds: float | None = None,
    error: str | None = None,
    failure_log_path: Path | None = None,
) -> None:
    """Update one step for an SB row without changing other completed steps."""

    if step not in STEP_COLUMNS:
        raise ValueError(f"Unknown SB processing step: {step}")
    if status not in {"Y", "N"}:
        raise ValueError(f"SB processing status must be Y or N, got {status!r}")
    fields = [f"{step} = ?", "updated_at = ?"]
    values: list[Any] = [status, utc_now()]
    if seconds is not None and f"{step}_seconds" in {
        "casda_query_seconds", "archive_download_seconds", "checksum_download_seconds",
        "extract_archive_seconds", "preprocess_seconds", "create_model_seconds",
        "insert_model_seconds", "subtract_model_seconds", "extract_ds_seconds",
        "crop_i_seconds", "crop_v_seconds", "product_copy_seconds", "cleanup_seconds",
    }:
        fields.append(f"{step}_seconds = ?")
        values.append(round(seconds, 3))
    if status == "N":
        fields.extend(["error_step = ?", "error_message = ?"])
        values.extend([step, error or "unspecified failure"])
        if failure_log_path is not None:
            fields.append("failure_log_path = ?")
            values.append(str(failure_log_path))
    else:
        # A successful retry of the same failed step supersedes its stale
        # diagnostic, while an error from a different step remains visible.
        # 同一步骤重试成功后清除该步骤的旧错误；其他步骤的错误仍然保留。
        fields.extend(
            [
                "error_message = CASE WHEN error_step = ? THEN NULL ELSE error_message END",
                "failure_log_path = CASE WHEN error_step = ? THEN NULL ELSE failure_log_path END",
                "error_step = CASE WHEN error_step = ? THEN NULL ELSE error_step END",
            ]
        )
        values.extend([step, step, step])
    where = ["source_name = ?", "sb_id = ?"]
    values.extend([source_name, sb_id])
    if beam is not None:
        where.append("beam = ?")
        values.append(beam)
    try:
        with _connect() as connection:
            cursor = connection.execute(
                f"UPDATE sb_processing SET {', '.join(fields)} WHERE {' AND '.join(where)}",
                values,
            )
            if cursor.rowcount == 0:
                raise StateDatabaseError(f"No SB status row for {source_name} {sb_id}")
    except sqlite3.Error as exc:
        raise StateDatabaseError(f"Cannot record {step} for {source_name} {sb_id}: {exc}") from exc


def retry_stage(
    source_name: str,
    sb_id: str,
    *,
    beam: str | None = None,
) -> str:
    """Return the persisted retry stage for one SB."""

    where = ["source_name = ?", "sb_id = ?"]
    values: list[Any] = [source_name, sb_id]
    if beam is not None:
        where.append("beam = ?")
        values.append(beam)
    try:
        with _connect() as connection:
            row = connection.execute(
                f"SELECT retry_stage FROM sb_processing WHERE {' AND '.join(where)}",
                values,
            ).fetchone()
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot read retry stage for {source_name} {sb_id}: {exc}"
        ) from exc
    if row is None:
        raise StateDatabaseError(f"No SB status row for {source_name} {sb_id}")
    return str(row[0] or "none")


def set_retry_stage(
    source_name: str,
    sb_id: str,
    stage: str,
    *,
    beam: str | None = None,
) -> None:
    """Set one SB retry stage without changing its scientific step record."""

    if stage not in RETRY_STAGES:
        raise ValueError(f"Unknown retry stage: {stage}")
    where = ["source_name = ?", "sb_id = ?"]
    values: list[Any] = [stage, utc_now(), utc_now(), source_name, sb_id]
    if beam is not None:
        where.append("beam = ?")
        values.append(beam)
    try:
        with _connect() as connection:
            cursor = connection.execute(
                "UPDATE sb_processing SET retry_stage = ?, retry_updated_at = ?, "
                f"updated_at = ? WHERE {' AND '.join(where)}",
                values,
            )
            if cursor.rowcount == 0:
                raise StateDatabaseError(f"No SB status row for {source_name} {sb_id}")
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot set retry stage for {source_name} {sb_id}: {exc}"
        ) from exc


def prepare_explicit_retry(source_name: str) -> dict[str, list[str]]:
    """Advance retryable failed SBs before login-node preparation.

    The first explicit retry clears processing checkpoints and reuses the
    existing MS after a container-side preprocess rollback. A later
    ``redownload_required`` row also clears download/extraction checkpoints;
    it stays in that stage until CASDA preparation succeeds.
    """

    now = utc_now()
    processing_assignments = [
        *(f"{column} = NULL" for column in PROCESSING_COLUMNS),
        *(f"{column}_seconds = NULL" for column in PROCESSING_COLUMNS),
        "error_step = NULL",
        "error_message = NULL",
        "failure_log_path = NULL",
        "retry_stage = 'processing_retry_ready'",
        "processing_retry_count = processing_retry_count + 1",
        "retry_updated_at = ?",
        "updated_at = ?",
    ]
    redownload_assignments = [
        "archive_download = NULL",
        "checksum_download = NULL",
        "extract_archive = NULL",
        "archive_download_seconds = NULL",
        "checksum_download_seconds = NULL",
        "extract_archive_seconds = NULL",
        *(f"{column} = NULL" for column in PROCESSING_COLUMNS),
        *(f"{column}_seconds = NULL" for column in PROCESSING_COLUMNS),
        "error_step = NULL",
        "error_message = NULL",
        "failure_log_path = NULL",
        "retry_updated_at = ?",
        "updated_at = ?",
    ]
    try:
        with _connect() as connection:
            processing = [
                str(row[0])
                for row in connection.execute(
                    "SELECT sb_id FROM sb_processing "
                    "WHERE source_name=? AND retry_stage='processing_failed'",
                    (source_name,),
                )
            ]
            redownload = [
                str(row[0])
                for row in connection.execute(
                    "SELECT sb_id FROM sb_processing "
                    "WHERE source_name=? AND retry_stage='redownload_required'",
                    (source_name,),
                )
            ]
            connection.execute(
                f"UPDATE sb_processing SET {', '.join(processing_assignments)} "
                "WHERE source_name=? AND retry_stage='processing_failed'",
                (now, now, source_name),
            )
            connection.execute(
                f"UPDATE sb_processing SET {', '.join(redownload_assignments)} "
                "WHERE source_name=? AND retry_stage='redownload_required'",
                (now, now, source_name),
            )
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot prepare explicit retry for {source_name}: {exc}"
        ) from exc
    return {"processing": processing, "redownload": redownload}


def mark_redownload_ready(source_name: str) -> list[str]:
    """Mark successfully re-downloaded SBs for their final processing try."""

    now = utc_now()
    try:
        with _connect() as connection:
            sb_ids = [
                str(row[0])
                for row in connection.execute(
                    "SELECT sb_id FROM sb_processing "
                    "WHERE source_name=? AND retry_stage='redownload_required'",
                    (source_name,),
                )
            ]
            connection.execute(
                "UPDATE sb_processing "
                "SET retry_stage='redownload_retry_ready', "
                "redownload_retry_count=redownload_retry_count + 1, "
                "retry_updated_at=?, updated_at=? "
                "WHERE source_name=? AND retry_stage='redownload_required'",
                (now, now, source_name),
            )
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot mark re-download ready for {source_name}: {exc}"
        ) from exc
    return sb_ids


def retry_sb_names(source_name: str, stages: Iterable[str]) -> set[str]:
    """Return ``SBxxxxx_beamyy`` names in any requested retry stage."""

    requested = tuple(stages)
    if not requested:
        return set()
    unknown = set(requested).difference(RETRY_STAGES)
    if unknown:
        raise ValueError(f"Unknown retry stage(s): {sorted(unknown)}")
    placeholders = ",".join("?" for _ in requested)
    try:
        with _connect() as connection:
            rows = connection.execute(
                "SELECT sb_id, beam FROM sb_processing "
                f"WHERE source_name=? AND retry_stage IN ({placeholders})",
                (source_name, *requested),
            )
            return {
                f"{row[0]}_{row[1]}" if row[1] else str(row[0])
                for row in rows
                if str(row[0]) != "__SOURCE__"
            }
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot read retry rows for {source_name}: {exc}"
        ) from exc


def source_has_retryable_failures(source_name: str) -> bool:
    """Return whether an explicit retry can still do useful work."""

    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM sb_processing
                WHERE source_name=?
                  AND COALESCE(product_copy, '') != 'Y'
                  AND error_step IS NOT NULL
                  AND retry_stage != 'exhausted'
                LIMIT 1
                """,
                (source_name,),
            ).fetchone()
            return row is not None
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot inspect retryable failures for {source_name}: {exc}"
        ) from exc


def exhaust_retry_stage(source_name: str, stage: str) -> list[str]:
    """Stop future retries for rows in ``stage`` after a terminal failure."""

    if stage not in RETRY_STAGES:
        raise ValueError(f"Unknown retry stage: {stage}")
    now = utc_now()
    try:
        with _connect() as connection:
            sb_ids = [
                str(row[0])
                for row in connection.execute(
                    "SELECT sb_id FROM sb_processing WHERE source_name=? AND retry_stage=?",
                    (source_name, stage),
                )
            ]
            connection.execute(
                "UPDATE sb_processing SET retry_stage='exhausted', "
                "retry_updated_at=?, updated_at=? "
                "WHERE source_name=? AND retry_stage=?",
                (now, now, source_name, stage),
            )
    except sqlite3.Error as exc:
        raise StateDatabaseError(
            f"Cannot exhaust retry stage for {source_name}: {exc}"
        ) from exc
    return sb_ids


def record_source_failure(
    source: dict[str, Any], error: str, *, advance_retry: bool = True
) -> None:
    """Mark every unfinished SB in a source as a non-blocking failure.

    Compute-worker failures advance the same bounded retry state machine as
    per-SB failures. Login-node planning/preparation callers can retain the
    current stage with ``advance_retry=False`` because no processing attempt
    occurred.
    """

    source_name = str(source["source_name"])
    retry_expression = (
        "CASE "
        "WHEN retry_stage='processing_retry_ready' THEN 'redownload_required' "
        "WHEN retry_stage='redownload_retry_ready' THEN 'exhausted' "
        "WHEN retry_stage='none' THEN 'processing_failed' "
        "ELSE retry_stage END"
        if advance_retry
        else "retry_stage"
    )
    try:
        with _connect() as connection:
            cursor = connection.execute(
                f"""
                UPDATE sb_processing
                SET worker_setup='N', error_step='worker_setup', error_message=?,
                    retry_stage={retry_expression},
                    retry_updated_at=?, updated_at=?
                WHERE source_name=? AND COALESCE(product_copy, '') != 'Y'
                """,
                (error, utc_now(), utc_now(), source_name),
            )
            if cursor.rowcount == 0:
                now = utc_now()
                initial_retry_stage = "processing_failed" if advance_retry else "none"
                connection.execute(
                    """
                    INSERT OR REPLACE INTO sb_processing
                    (ucs_number, source_name, sb_id, beam, worker_setup, error_step,
                     error_message, retry_stage, retry_updated_at, created_at, updated_at)
                    VALUES (?, ?, '__SOURCE__', '', 'N', 'worker_setup', ?, ?, ?, ?, ?)
                    """,
                    (
                        int(source["ucs_number"]),
                        source_name,
                        error,
                        initial_retry_stage,
                        now,
                        now,
                        now,
                    ),
                )
    except sqlite3.Error as exc:
        raise StateDatabaseError(f"Cannot record source failure for {source_name}: {exc}") from exc
