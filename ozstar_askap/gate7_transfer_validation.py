#!/usr/bin/env python3
"""Gate 7: validate an ada-initiated, SHA-256-verified Ozstar pull.

Run this driver on an ada login node after Gate 6 reports PASS. It reads the
Gate 6 result through SSH, pulls that isolated Gate 6 product triple into a new
test directory below ``/import/ada1/qhua0119/Combine_ImagePlot/Ozstar``, checks
all three files, records them in an isolated SQLite ledger, and uploads an
immutable validation receipt back to Ozstar. It never deletes either copy.
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import sqlite3
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import pull_askap_products


ADA_ROOT = Path("/import/ada1/qhua0119/Combine_ImagePlot/Ozstar")
DEFAULT_GATE7_ROOT = ADA_ROOT / "gate7-validation"
DEFAULT_PASSWORD_FILE = ADA_ROOT / "state/ozstar-transfer-password"
DEFAULT_REMOTE_RECEIPT_ROOT = (
    "/fred/oz299/qhuang/ASKAP-UCDs/state/transfer_receipts/gate7"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gate6-work", required=True, help="Absolute Gate 6 work path on Ozstar."
    )
    parser.add_argument(
        "--work",
        type=Path,
        help="New empty ada Gate 7 directory below the required ada root.",
    )
    parser.add_argument("--remote-user", default=pull_askap_products.OZSTAR_USER)
    parser.add_argument("--remote-host", default=pull_askap_products.OZSTAR_HOST)
    parser.add_argument("--remote-python", default=pull_askap_products.OZSTAR_PYTHON)
    parser.add_argument("--password-file", type=Path, default=DEFAULT_PASSWORD_FILE)
    parser.add_argument("--method", choices=("rsync", "scp"), default="rsync")
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Required authorization for the real validation pull.",
    )
    return parser.parse_args()


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _safe_gate6_work(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or "gate6-validation" not in path.parts
        or not path.name.startswith("gate6-UCS")
    ):
        raise ValueError("Unsafe or non-isolated Gate 6 path: %s" % value)
    required = PurePosixPath("/fred/oz299/qhuang/dstools/gate6-validation")
    try:
        path.relative_to(required)
    except ValueError:
        raise ValueError("Gate 6 path is outside %s: %s" % (required, path))
    return str(path)


def _safe_gate7_work(value: Path) -> Path:
    resolved = pull_askap_products._safe_ada_root(value)
    required = DEFAULT_GATE7_ROOT.resolve()
    if resolved == required or required not in resolved.parents:
        raise ValueError("Gate 7 work must be a child of %s" % required)
    if resolved.exists():
        if not resolved.is_dir():
            raise ValueError("Gate 7 work is not a directory: %s" % resolved)
        if any(resolved.iterdir()):
            raise FileExistsError("Gate 7 work is not empty: %s" % resolved)
    return resolved


def _remote_gate6_result(
    connection: pull_askap_products.RemoteConnection, gate6_work: str
) -> Dict[str, Any]:
    result_path = "%s/state/gate6-result.json" % gate6_work.rstrip("/")
    output = connection.ssh("cat -- %s" % shlex.quote(result_path))
    result = json.loads(output)
    if result.get("gate") != "gate6" or result.get("status") != "PASS":
        raise ValueError("Remote Gate 6 result is not PASS: %s" % result_path)
    recorded = str(PurePosixPath(str(result.get("work", ""))))
    if recorded != gate6_work:
        raise ValueError(
            "Gate 6 result belongs to %s, not %s" % (recorded, gate6_work)
        )
    return result


def _configure_logging(work: Path) -> Path:
    log_path = work / "logs/gate7-driver.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_path), encoding="utf-8"),
        ],
    )
    return log_path


def _run(
    args: argparse.Namespace, gate6_work: str, gate7_work: Path
) -> Dict[str, Any]:
    connection = pull_askap_products.RemoteConnection(
        args.remote_user,
        args.remote_host,
        args.password_file,
        pull_askap_products.SSH_CONNECT_TIMEOUT,
    )
    gate6_result = _remote_gate6_result(connection, gate6_work)
    source_root = "%s/products" % gate6_work.rstrip("/")
    transfer_args = argparse.Namespace(
        all=True,
        batch=[],
        dry_run=False,
        source_root=source_root,
        ada_root=gate7_work,
        database=gate7_work / "state/transfer_ledger.sqlite",
        remote_user=args.remote_user,
        remote_host=args.remote_host,
        remote_python=args.remote_python,
        receipt_remote_root=DEFAULT_REMOTE_RECEIPT_ROOT,
        password_file=args.password_file,
        method=args.method,
        connect_timeout=pull_askap_products.SSH_CONNECT_TIMEOUT,
        no_receipt_upload=False,
        receipt_kind="gate7-validation",
        _allow_test_root=False,
    )
    result = pull_askap_products.run_transfer(transfer_args)
    if result.get("status") != "PASS":
        raise RuntimeError("Gate 7 pull did not pass: %r" % result)
    if result.get("observation_count") != 1 or result.get("file_count") != 3:
        raise RuntimeError(
            "Gate 7 expected exactly one observation and three products: %r" % result
        )

    expected_source = gate6_result.get("source_name")
    expected_sb = gate6_result.get("sb_id")
    database = Path(result["database"])
    connection_db = sqlite3.connect(str(database))
    try:
        rows = connection_db.execute(
            """
            SELECT source_name, sb_id, ds_status, stokes_i_status,
                   stokes_v_status, overall_status
            FROM transfer_ledger
            """
        ).fetchall()
    finally:
        connection_db.close()
    expected = [(expected_source, expected_sb, "Y", "Y", "Y", "complete")]
    if rows != expected:
        raise RuntimeError(
            "Gate 7 ledger did not record the expected triple: %r" % (rows,)
        )

    return {
        "gate": "gate7",
        "status": "PASS",
        "gate6_work": gate6_work,
        "gate6_source": expected_source,
        "gate6_sb": expected_sb,
        "ada_work": str(gate7_work),
        "products_root": result["products_root"],
        "database": result["database"],
        "manifest": result["manifest"],
        "receipt": result["receipt"],
        "remote_receipt": result["remote_receipt"],
        "observation_count": 1,
        "product_file_count": 3,
        "source_deleted": False,
    }


def main() -> None:
    args = _parse_args()
    if not args.submit:
        raise SystemExit("Gate 7 performs a real ada pull; rerun with --submit.")
    gate6_work = _safe_gate6_work(args.gate6_work)
    gate7_work = _safe_gate7_work(
        args.work or DEFAULT_GATE7_ROOT / ("gate7-%s" % _timestamp())
    )
    gate7_work.mkdir(parents=True, exist_ok=False)
    log_path = _configure_logging(gate7_work)
    result_path = gate7_work / "state/gate7-result.json"
    try:
        result = _run(args, gate6_work, gate7_work)
    except Exception as exc:
        failure = {
            "gate": "gate7",
            "status": "FAIL",
            "gate6_work": gate6_work,
            "ada_work": str(gate7_work),
            "error": "%s: %s" % (type(exc).__name__, exc),
            "driver_log": str(log_path),
        }
        pull_askap_products._atomic_json(result_path, failure)
        logging.getLogger("askap.gate7").exception("Gate 7 failed")
        raise SystemExit(
            "Gate 7 failed; inspect %s and %s" % (result_path, log_path)
        )

    result["driver_log"] = str(log_path)
    pull_askap_products._atomic_json(result_path, result)
    print("Gate 7 ada-pull validation: PASS")
    print("Result: %s" % result_path)
    print("Ada products: %s" % result["products_root"])
    print("Ozstar receipt: %s" % result["remote_receipt"])
    print("Neither the Gate 6 source nor the ada validation copy was deleted.")


if __name__ == "__main__":
    main()
