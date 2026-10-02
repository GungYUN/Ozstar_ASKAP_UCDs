#!/usr/bin/env python3
"""Display unresolved ASKAP SB failures from the durable SQLite state table.

The default output is deliberately compact::

    UCS1543_SB35282 —— create_model

Use ``--details`` when the beam, bounded-retry stage, counters, error text, and
retained log path are needed.  This command opens SQLite read-only and never
migrates or changes pipeline state.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

try:
    from . import config
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=config.STATE_ROOT / "sb_processing.sqlite",
        help="Override the read-only sb_processing.sqlite path.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Limit output to a source such as UCS1543; repeat when needed.",
    )
    parser.add_argument(
        "--retryable-only",
        action="store_true",
        help="Hide terminal exhausted failures.",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Show beam, retry state/counters, error text, and failure log path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of text.",
    )
    return parser.parse_args()


def _read_only_connection(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"SB status database does not exist: {resolved}")
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _available_columns(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(sb_processing)")
    }


def _select_expression(columns: set[str], name: str, fallback: str) -> str:
    return name if name in columns else f"{fallback} AS {name}"


def unresolved_failures(
    database: Path,
    *,
    sources: list[str] | None = None,
    retryable_only: bool = False,
) -> list[dict[str, Any]]:
    """Return unresolved failure rows without modifying the database."""

    requested_sources = [value.strip() for value in (sources or []) if value.strip()]
    with closing(_read_only_connection(database)) as connection:
        columns = _available_columns(connection)
        required = {"ucs_number", "source_name", "sb_id", "error_step"}
        missing = sorted(required.difference(columns))
        if missing:
            raise RuntimeError(
                "sb_processing is missing required column(s): " + ", ".join(missing)
            )

        selected = [
            "ucs_number",
            "source_name",
            "sb_id",
            _select_expression(columns, "beam", "''"),
            "error_step",
            _select_expression(columns, "error_message", "NULL"),
            _select_expression(columns, "failure_log_path", "NULL"),
            _select_expression(columns, "retry_stage", "'none'"),
            _select_expression(columns, "processing_retry_count", "0"),
            _select_expression(columns, "redownload_retry_count", "0"),
            _select_expression(columns, "updated_at", "NULL"),
        ]
        predicates = ["error_step IS NOT NULL"]
        values: list[Any] = []
        if "product_copy" in columns:
            predicates.append("COALESCE(product_copy, '') != 'Y'")
        if "retry_stage" in columns:
            predicates.append("retry_stage != 'complete'")
            if retryable_only:
                predicates.append("retry_stage != 'exhausted'")
        if requested_sources:
            placeholders = ",".join("?" for _ in requested_sources)
            predicates.append(f"source_name IN ({placeholders})")
            values.extend(requested_sources)

        rows = connection.execute(
            f"SELECT {', '.join(selected)} FROM sb_processing "
            f"WHERE {' AND '.join(predicates)} "
            "ORDER BY ucs_number, "
            "CASE WHEN sb_id='__SOURCE__' THEN -1 "
            "ELSE CAST(REPLACE(sb_id, 'SB', '') AS INTEGER) END, beam",
            values,
        ).fetchall()
    return [dict(row) for row in rows]


def _record_label(record: dict[str, Any]) -> str:
    source_name = str(record.get("source_name") or "").strip()
    if not source_name.startswith("UCS"):
        source_name = f"UCS{int(record['ucs_number'])}"
    sb_id = str(record.get("sb_id") or "")
    suffix = "SOURCE" if sb_id == "__SOURCE__" else sb_id
    return f"{source_name}_{suffix}"


def _print_text(records: list[dict[str, Any]], details: bool) -> None:
    if not records:
        print("No unresolved failure records.")
        return
    for record in records:
        print(f"{_record_label(record)} —— {record['error_step']}")
        if not details:
            continue
        print(
            "  "
            f"beam={record.get('beam') or '-'}; "
            f"retry_stage={record.get('retry_stage') or 'none'}; "
            f"processing_retry_count={int(record.get('processing_retry_count') or 0)}; "
            f"redownload_retry_count={int(record.get('redownload_retry_count') or 0)}"
        )
        if record.get("error_message"):
            print(f"  error={record['error_message']}")
        if record.get("failure_log_path"):
            print(f"  log={record['failure_log_path']}")


def main() -> None:
    args = _parse_args()
    try:
        records = unresolved_failures(
            args.database,
            sources=args.source,
            retryable_only=args.retryable_only,
        )
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        raise SystemExit(f"Cannot read failure records: {exc}") from exc
    if args.json:
        print(json.dumps(records, indent=2, ensure_ascii=False))
    else:
        _print_text(records, args.details)


if __name__ == "__main__":
    main()
