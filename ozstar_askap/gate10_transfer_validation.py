#!/usr/bin/env python3
"""Gate10: Ada-initiated verified pull of the five real Gate8 product triples.

Uses the production pull implementation, without changing processing code,
Gate8 products/state, or production transfer records. Python 3.7 / stdlib only.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import shlex
import sqlite3
import stat
from pathlib import Path, PurePosixPath

import pull_askap_products as pull

ADA_ROOT = pull.ADA_ROOT
VALIDATION_ROOT = ADA_ROOT / "gate10-validation"
GATE8_PARENT = PurePosixPath("/fred/oz299/qhuang/dstools/shared-validation")
DEFAULT_GATE8_WORK = str(GATE8_PARENT / "gate8-central-20261009")
REMOTE_RECEIPT_ROOT = "/fred/oz299/qhuang/dstools/gate10-validation"
CASES = {
    "edge": {"sources": ["UCS225"], "sb_id": "SB67772", "beam": "beam18",
             "key": "6952057a1ddba073e679bf07eacf92186e68bba1648f1fa30bff8e8c028f794e"},
    "shared3": {"sources": ["UCS689", "UCS690", "UCS693"], "sb_id": "SB74372", "beam": "beam28",
                "key": "36d17f6f9b843c1d9e0c7aa0b9277258665fd02841a6c936f0728f79d844e2b7"},
    "hipass": {"sources": ["UCS1872"], "sb_id": "SB83483", "beam": "beam20",
               "key": "8569cbdc4f44fcf9d6ae301609abc53ec6a34600400692acd7ae8e867ba3b013"},
}
LOGGER = logging.getLogger("askap.gate10")

# Only read Gate8 processing evidence. No modules, SIF, astroquery or writes.
REMOTE_EVIDENCE_PROGRAM = r"""
import json, sqlite3, sys
from pathlib import Path
work = Path(sys.argv[1])
expected = json.loads(sys.argv[2])
controller = json.loads((work / 'state/shared-controller.json').read_text())
if controller.get('status') != 'complete' or len(controller.get('jobs', [])) != 1:
    raise SystemExit('Gate8 controller is not complete with exactly one job')
job = json.loads(Path(controller['jobs'][0]).read_text())
if job['observation']['key'] != expected['key']:
    raise SystemExit('Gate8 observation key mismatch')
if set(job.get('results', {})) != set(expected['sources']) or not all(
        r.get('status') == 'complete' for r in job['results'].values()):
    raise SystemExit('Gate8 target results are not complete')
db = sqlite3.connect((work / 'state/sb_processing.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
db.row_factory = sqlite3.Row
try:
    rows = [dict(row) for row in db.execute('SELECT * FROM sb_processing')]
finally:
    db.close()
steps = ('extract_archive', 'preprocess', 'create_model', 'insert_model',
         'subtract_model', 'extract_ds', 'crop_i', 'crop_v', 'product_copy', 'cleanup')
if len(rows) != len(expected['sources']) or {r['source_name'] for r in rows} != set(expected['sources']):
    raise SystemExit('Gate8 processing row count/targets mismatch')
for row in rows:
    if (row['sb_id'] != expected['sb_id'] or row['beam'] != expected['beam']
            or row['observation_key'] != expected['key']
            or any(row.get(step) != 'Y' for step in steps)
            or row.get('retry_stage') != 'complete' or row.get('error_step') is not None
            or row.get('model_reused') != 1 or not row.get('model_origin')):
        raise SystemExit('Gate8 processing evidence mismatch: ' + repr(row))
if len({r['model_origin'] for r in rows}) != 1:
    raise SystemExit('Gate8 targets did not share the same model origin')
print(json.dumps({'status': 'PASS', 'sources': expected['sources'], 'model_origin': rows[0]['model_origin']}))
"""


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def safe_gate8_work(value):
    path = PurePosixPath(pull._safe_remote_path(value, "Gate8 root"))
    if path.parent != GATE8_PARENT or not re.fullmatch(r"gate8-[A-Za-z0-9_.-]+", path.name):
        raise ValueError("Gate8 root must be one gate8-* child of %s" % GATE8_PARENT)
    return str(path)


def safe_work(value):
    work = pull._safe_ada_root(value)
    if work.parent != VALIDATION_ROOT.resolve() or not re.fullmatch(r"gate10-[A-Za-z0-9_.-]+", work.name):
        raise ValueError("Gate10 work must be one gate10-* child of %s" % VALIDATION_ROOT)
    if work.exists() and (not work.is_dir() or any(work.iterdir())):
        raise FileExistsError("Gate10 requires a new/empty work directory: %s" % work)
    return work


def transfer_args(args, work, gate8_work, case_name):
    return argparse.Namespace(
        all=True, batch=[], dry_run=False,
        source_root=gate8_work + "/" + case_name + "/products",
        ada_root=work / case_name, database=work / "state/transfer_ledger.sqlite",
        remote_user=args.remote_user, remote_host=args.remote_host,
        remote_python=args.remote_python, password_file=args.password_file,
        method="rsync", connect_timeout=args.connect_timeout,
        receipt_remote_root=REMOTE_RECEIPT_ROOT + "/" + work.name + "/receipts/" + case_name,
        no_receipt_upload=False, receipt_kind="gate10-validation", _allow_test_root=False,
    )


def ledger_rows(database):
    connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(
            "SELECT * FROM transfer_ledger ORDER BY batch, source_name, sb_id, beam")]
    finally:
        connection.close()


def verify_ledger(database, source_root, records):
    rows = [row for row in ledger_rows(database) if row["remote_source_root"] == source_root]
    expected = {pull._record_key(record): record for record in records}
    require(len(rows) == len(expected), "Ledger row count mismatch for " + source_root)
    for row in rows:
        key = (row["batch"], row["source_name"], row["sb_id"], row["beam"])
        require(key in expected, "Unexpected ledger observation: %r" % (key,))
        require(row["overall_status"] == "complete" and row["error"] is None,
                "Ledger is not complete: %r" % row)
        require(bool(row["first_verified_at"]), "Ledger has no first verification date")
        for role in pull.PRODUCT_ROLES:
            require(row[role + "_status"] == "Y", "Ledger has a non-Y product")
            require(row[role + "_sha256"] == expected[key]["files"][role]["sha256"],
                    "Ledger SHA256 mismatch")
    return {tuple(row[name] for name in ("batch", "source_name", "sb_id", "beam")):
            row["first_verified_at"] for row in rows}


def local_fingerprints(products_root, records):
    result = {}
    for record in records:
        require(all(value == "Y" for value in pull._local_file_status(products_root, record).values()),
                "Local DS/I/V verification failed")
        for item in record["files"].values():
            path = products_root / item["path"]
            result[item["path"]] = (pull._sha256(path), path.stat().st_mtime_ns)
    return result


def verified_pull(connection, args, records):
    result = pull.run_transfer(args)
    require(result.get("status") == "PASS" and result.get("observation_count") == len(records)
            and result.get("file_count") == 3 * len(records), "Unexpected pull result: %r" % result)
    manifest = json.loads(Path(result["manifest"]).read_text())
    require(manifest["records"] == records, "Source snapshot changed since Gate10 preflight")
    receipt = json.loads(Path(result["receipt"]).read_text())
    require(receipt["kind"] == "gate10-validation" and receipt["status"] == "complete"
            and receipt["source_root"] == args.source_root
            and receipt["observation_count"] == len(records)
            and receipt["file_count"] == 3 * len(records), "Receipt scope/count mismatch")
    observations = receipt["observations"]
    require(len(observations) == len(records), "Receipt observation count mismatch")
    for actual, expected in zip(observations, records):
        require({key: actual[key] for key in expected} == expected
                and actual["overall_status"] == "complete"
                and actual["transfer_status"] == dict.fromkeys(pull.PRODUCT_ROLES, "Y"),
                "Receipt does not certify a full DS/I/V triple")
    require(result.get("remote_receipt"), "No remote receipt was uploaded")
    remote_receipt = json.loads(connection.ssh("cat -- " + shlex.quote(result["remote_receipt"])))
    require(remote_receipt == receipt, "Ozstar receipt read-back differs from Ada receipt")
    verify_ledger(Path(args.database), args.source_root, records)
    local_fingerprints(Path(result["products_root"]), records)
    return result


def repair_probe(connection, args, records):
    """Change one byte only in an isolated Ada V copy; restore it even on failure."""
    record = next(record for record in records if record["source"] == "UCS690")
    path = args.ada_root / "products" / record["files"]["stokes_v"]["path"]
    original_sha = record["files"]["stokes_v"]["sha256"]
    require(pull._sha256(path) == original_sha and path.stat().st_size > 0,
            "Repair probe requires an intact, nonempty private V copy")
    saved = path.stat()
    mode = stat.S_IMODE(saved.st_mode)
    times = (saved.st_atime_ns, saved.st_mtime_ns)
    corrupted_sha = None
    probe_written = False
    try:
        path.chmod(mode | stat.S_IWUSR)
        with path.open("r+b") as handle:
            original_byte = handle.read(1)
            handle.seek(0)
            handle.write(bytes([original_byte[0] ^ 1]))
            probe_written = True
        os.utime(str(path), ns=times)  # Same size and mtime: hash must detect it.
        corrupted_sha = pull._sha256(path)
        require(corrupted_sha != original_sha, "Controlled local difference was not made")
        args.dry_run = True
        dry_run = pull.run_transfer(args)
        args.dry_run = False
        require(dry_run.get("files_needing_transfer") == 1, "Hash probe did not select exactly one file")
        result = verified_pull(connection, args, records)
        require(result["files_transferred"] == 1, "Repair must transfer only the changed V file")
        require(pull._sha256(path) == original_sha, "Private V copy was not repaired")
        return {"changed_file": str(path), "same_size_and_mtime": True,
                "dry_run_files_needing_transfer": 1, "pull": result}
    finally:
        args.dry_run = False
        if path.is_file():
            # If transfer failed without touching this file, undo our byte change.
            if probe_written and (corrupted_sha is None or pull._sha256(path) == corrupted_sha):
                path.chmod(mode | stat.S_IWUSR)
                with path.open("r+b") as handle:
                    handle.write(original_byte)
                os.utime(str(path), ns=times)
            path.chmod(mode)


def run(args, work, gate8_work):
    connection = pull.RemoteConnection(args.remote_user, args.remote_host, args.password_file, args.connect_timeout)
    records_by_case, evidence, settings = {}, {}, {}
    for name, case in CASES.items():
        LOGGER.info("Read-only Gate8 preflight: %s", name)
        command = " ".join(shlex.quote(value) for value in (
            args.remote_python, "-c", REMOTE_EVIDENCE_PROGRAM,
            gate8_work + "/" + name, json.dumps(case)))
        evidence[name] = json.loads(connection.ssh(command))
        require(evidence[name].get("status") == "PASS", "Gate8 evidence did not pass")
        settings[name] = transfer_args(args, work, gate8_work, name)
        records = pull._snapshot(connection, settings[name].source_root, args.remote_python, [])
        require(len(records) == len(case["sources"]) and {
            (record["source"], record["sb_id"], record["beam"]) for record in records
        } == {(source, case["sb_id"], case["beam"]) for source in case["sources"]},
                "Gate8 snapshot missing or unexpected DS/I/V triples: " + name)
        records_by_case[name] = records
    pull._atomic_json(work / "state/gate10-source-snapshot.json", records_by_case)

    results = {}
    for name in CASES:
        transfer = settings[name]
        records = records_by_case[name]
        LOGGER.info("Gate10 initial pull: %s (%s triples)", name, len(records))
        transfer.dry_run = True
        dry_run = pull.run_transfer(transfer)
        transfer.dry_run = False
        require(dry_run.get("files_needing_transfer") == 3 * len(records), "Initial dry-run count mismatch")
        initial = verified_pull(connection, transfer, records)
        require(initial["files_transferred"] == 3 * len(records), "Initial file count mismatch")
        first_verified = verify_ledger(Path(transfer.database), transfer.source_root, records)
        fingerprints = local_fingerprints(Path(initial["products_root"]), records)
        LOGGER.info("Gate10 repeat pull: %s; expected transferred files = 0", name)
        repeat = verified_pull(connection, transfer, records)
        require(repeat["manifest_id"] == initial["manifest_id"] and repeat["files_transferred"] == 0,
                "Repeated pull retransferred files or changed snapshot")
        require(local_fingerprints(Path(initial["products_root"]), records) == fingerprints,
                "Repeated pull modified local products")
        require(verify_ledger(Path(transfer.database), transfer.source_root, records) == first_verified,
                "Repeat changed first verification dates or duplicated records")
        results[name] = {"initial": initial, "repeat": repeat}
        pull._atomic_json(work / "state/gate10-progress.json", results)

    LOGGER.info("Gate10 controlled hash/repair probe: private Ada UCS690 Stokes-V copy only")
    repair = repair_probe(connection, settings["shared3"], records_by_case["shared3"])
    for name in CASES:
        require(pull._snapshot(connection, settings[name].source_root, args.remote_python, []) == records_by_case[name],
                "Gate8 source files changed during validation: " + name)
        verify_ledger(Path(settings[name].database), settings[name].source_root, records_by_case[name])
    database = work / "state/transfer_ledger.sqlite"
    require(len(ledger_rows(database)) == 5, "Expected exactly five unique ledger rows")
    return {"gate": "gate10", "status": "PASS", "ada_work": str(work),
            "gate8_work": gate8_work, "database": str(database), "cases": results,
            "gate8_evidence": evidence, "repair_probe": repair,
            "observation_count": 5, "product_file_count": 15,
            "initial_files_transferred": 15, "repeat_files_transferred": 0,
            "repair_files_transferred": 1, "source_snapshot_unchanged": True,
            "source_deleted": False, "production_ledger_modified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate8-work", default=DEFAULT_GATE8_WORK)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--remote-user", default=pull.OZSTAR_USER)
    parser.add_argument("--remote-host", default=pull.OZSTAR_HOST)
    parser.add_argument("--remote-python", default=pull.OZSTAR_PYTHON)
    parser.add_argument("--password-file", type=Path, default=ADA_ROOT / "state/ozstar-transfer-password")
    parser.add_argument("--connect-timeout", type=int, default=pull.SSH_CONNECT_TIMEOUT)
    parser.add_argument("--submit", action="store_true", help="Authorize real Ada pull, test-copy repair and receipt upload; no Slurm submission.")
    args = parser.parse_args()
    if not args.submit:
        parser.error("Gate10 performs real transfers; add --submit to authorize the test")
    gate8_work = safe_gate8_work(args.gate8_work)
    work = safe_work(args.work or VALIDATION_ROOT / ("gate10-" + pull._timestamp()))
    work.mkdir(parents=True, exist_ok=True)
    with (work / "gate10-driver.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        log_path = work / "logs/gate10-driver.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                            handlers=[logging.StreamHandler(), logging.FileHandler(str(log_path), encoding="utf-8")])
        result_path = work / "state/gate10-result.json"
        try:
            result = run(args, work, gate8_work)
        except (Exception, KeyboardInterrupt) as exc:
            pull._atomic_json(result_path, {"gate": "gate10", "status": "FAIL", "ada_work": str(work),
                              "gate8_work": gate8_work, "error": "%s: %s" % (type(exc).__name__, exc),
                              "driver_log": str(log_path)})
            LOGGER.exception("Gate10 failed")
            raise SystemExit("Gate10 failed; inspect %s and %s" % (result_path, log_path))
        result["driver_log"] = str(log_path)
        pull._atomic_json(result_path, result)
        print("Gate10 shared-product Ada pull validation: PASS")
        print("Result: %s" % result_path)
        print("Work: %s" % work)
        print("Five complete DS/I/V triples; repeat transferred 0 files; hash repair transferred 1 file.")
        print("No Gate8/production files were deleted or changed.")


if __name__ == "__main__":
    main()
