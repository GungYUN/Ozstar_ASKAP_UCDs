"""Offline Gate10 tests: real files/SQLite/hashes, simulated SSH/rsync only."""
import ast
import contextlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gate10_transfer_validation as gate10
import pull_askap_products as pull


class LocalConnection(pull.RemoteConnection):
    transfers = []
    omit_v = False
    fail_transfer = False
    mutate_source = False

    def __init__(self, user, host, password_file, connect_timeout):
        self.remote = user + "@" + host
        self.prefix = []
        self.environment = dict(os.environ)
        self.ssh_options = ["-4", "-x"]

    def ssh(self, command, dry_run=False):
        if dry_run:
            return ""
        result = subprocess.run(["/bin/bash", "-c", command], check=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout.strip()

    def upload_file(self, local_path, remote_absolute):
        shutil.copy2(str(local_path), remote_absolute)

    def run(self, command, capture=True, dry_run=False):
        if command[0] != "rsync":
            raise AssertionError("Unexpected fake transport command: %r" % command)
        if self.fail_transfer:
            raise RuntimeError("Synthetic rsync failure")
        file_list = Path(next(item.split("=", 1)[1] for item in command if item.startswith("--files-from=")))
        names = file_list.read_text().splitlines()
        source = Path(command[-2].split(":", 1)[1])
        destination = Path(command[-1])
        copied = []
        for name in names:
            if self.omit_v and name.endswith(pull.V_IMAGE):
                continue
            src, dst = source / name, destination / name
            same_size_time = (dst.is_file() and src.stat().st_size == dst.stat().st_size
                              and src.stat().st_mtime_ns == dst.stat().st_mtime_ns)
            # Simulate rsync's real quick-check so absence of --ignore-times
            # causes the same-size/mtime repair regression to fail.
            if same_size_time and "--ignore-times" not in command:
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
            copied.append(name)
        self.transfers.append({"requested": names, "copied": copied, "command": command})
        if self.mutate_source and names:
            with (source / names[0]).open("ab") as handle:
                handle.write(b"source changed")
        return ""


class Gate10Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.ada = root / "ada"
        self.remote_parent = root / "remote/shared-validation"
        self.gate8 = self.remote_parent / "gate8-central-test"
        self.work = self.ada / "gate10-validation/gate10-test"
        self.work.mkdir(parents=True)
        self.args = SimpleNamespace(remote_user="test", remote_host="local", remote_python=sys.executable,
                                    password_file=None, connect_timeout=30)
        LocalConnection.transfers = []
        LocalConnection.omit_v = LocalConnection.fail_transfer = LocalConnection.mutate_source = False
        for module, name, value in (
            (pull, "ADA_ROOT", self.ada), (pull, "RemoteConnection", LocalConnection),
            (gate10, "ADA_ROOT", self.ada), (gate10, "VALIDATION_ROOT", self.ada / "gate10-validation"),
            (gate10, "GATE8_PARENT", PurePosixPath(str(self.remote_parent))),
            (gate10, "REMOTE_RECEIPT_ROOT", str(root / "remote/gate10-validation")),
        ):
            changed = patch.object(module, name, value)
            changed.start()
            self.addCleanup(changed.stop)
        for name, case in gate10.CASES.items():
            case_work = self.gate8 / name
            state = case_work / "state"
            state.mkdir(parents=True)
            job = state / "job.json"
            pull._atomic_json(job, {"observation": {"key": case["key"]},
                                   "results": {source: {"status": "complete"} for source in case["sources"]}})
            pull._atomic_json(state / "shared-controller.json", {"status": "complete", "jobs": [str(job)]})
            connection = sqlite3.connect(str(state / "sb_processing.sqlite"))
            connection.execute("""CREATE TABLE sb_processing (
                source_name TEXT, sb_id TEXT, beam TEXT, observation_key TEXT,
                extract_archive TEXT, preprocess TEXT, create_model TEXT, insert_model TEXT,
                subtract_model TEXT, extract_ds TEXT, crop_i TEXT, crop_v TEXT, product_copy TEXT,
                cleanup TEXT, retry_stage TEXT, error_step TEXT, model_reused INTEGER, model_origin TEXT)""")
            for source in case["sources"]:
                values = [source, case["sb_id"], case["beam"], case["key"]] + ["Y"] * 10
                values += ["complete", None, 1, str(case_work / "staging/_shared/master")]
                connection.execute("INSERT INTO sb_processing VALUES (%s)" % ",".join("?" for _ in values), values)
                number = int(source[3:])
                start = (number - 1) // 50 * 50 + 1
                sb_name = case["sb_id"] + "_" + case["beam"]
                product = case_work / "products" / ("UCS%d-%d" % (start, start + 49)) / source / "LongObs" / sb_name
                for relative in (sb_name + ".ds", "wsclean_model/" + pull.I_IMAGE, "wsclean_model/" + pull.V_IMAGE):
                    path = product / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes((source + " " + relative + " fixture").encode())
            connection.commit()
            connection.close()

    def transfer(self, name="shared3"):
        return gate10.transfer_args(self.args, self.work, str(self.gate8), name)

    def records(self, args):
        return pull._snapshot(LocalConnection("test", "local", None, 30), args.source_root, sys.executable, [])

    def test_full_gate10_five_triples_repeat_repair_and_remote_receipts(self):
        before = {str(p): pull._sha256(p) for p in self.gate8.rglob("*") if p.is_file()}
        with contextlib.redirect_stdout(io.StringIO()):
            result = gate10.run(self.args, self.work, str(self.gate8))
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["observation_count"], 5)
        self.assertEqual(result["product_file_count"], 15)
        self.assertEqual([len(entry["copied"]) for entry in LocalConnection.transfers], [3, 9, 3, 1])
        self.assertTrue(all("--ignore-times" in entry["command"] for entry in LocalConnection.transfers))
        self.assertEqual(len(gate10.ledger_rows(Path(result["database"]))), 5)
        receipts = list(self.work.rglob("transfer-receipt-*.json"))
        self.assertEqual(len(receipts), 7)
        self.assertEqual({str(p): pull._sha256(p) for p in self.gate8.rglob("*") if p.is_file()}, before)
        for entry in result["cases"].values():
            self.assertEqual(entry["repeat"]["files_transferred"], 0)
            self.assertEqual(json.loads(Path(entry["repeat"]["remote_receipt"]).read_text())["kind"], "gate10-validation")

    def test_missing_v_not_complete_no_receipt_then_recover(self):
        args = self.transfer("edge")
        LocalConnection.omit_v = True
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "SHA-256 verification failed"):
            pull.run_transfer(args)
        rows = gate10.ledger_rows(args.database)
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["ds_status"], rows[0]["stokes_i_status"], rows[0]["stokes_v_status"]), ("Y", "Y", "N"))
        self.assertEqual(rows[0]["overall_status"], "failed")
        self.assertIsNone(rows[0]["first_verified_at"])
        self.assertEqual(list(self.work.rglob("transfer-receipt-*.json")), [])
        LocalConnection.omit_v = False
        with contextlib.redirect_stdout(io.StringIO()):
            result = pull.run_transfer(args)
        self.assertEqual(result["files_transferred"], 1)
        self.assertEqual(gate10.ledger_rows(args.database)[0]["overall_status"], "complete")

    def test_probe_failure_restores_private_byte_not_source(self):
        args = self.transfer()
        with contextlib.redirect_stdout(io.StringIO()):
            pull.run_transfer(args)
            records = self.records(args)
        fingerprints = gate10.local_fingerprints(args.ada_root / "products", records)
        LocalConnection.fail_transfer = True
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "Synthetic rsync failure"):
            gate10.repair_probe(LocalConnection("test", "local", None, 30), args, records)
        self.assertEqual(gate10.local_fingerprints(args.ada_root / "products", records), fingerprints)
        self.assertFalse(args.dry_run)
        # Ledger remains failed, not a false successful certification.
        self.assertTrue(all(row["overall_status"] == "failed" for row in gate10.ledger_rows(args.database)))

    def test_source_change_during_transfer_rejects_completion(self):
        args = self.transfer("edge")
        LocalConnection.mutate_source = True
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "products changed during transfer"):
            pull.run_transfer(args)
        self.assertEqual(gate10.ledger_rows(args.database)[0]["overall_status"], "failed")
        self.assertEqual(list(self.work.rglob("transfer-receipt-*.json")), [])

    def test_receipt_upload_failure_does_not_certify_complete(self):
        args = self.transfer("edge")
        with patch.object(pull, "_upload_receipt", side_effect=RuntimeError("Synthetic receipt upload failure")):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "receipt upload failure"):
                pull.run_transfer(args)
        row = gate10.ledger_rows(args.database)[0]
        self.assertEqual((row["ds_status"], row["stokes_i_status"], row["stokes_v_status"]), ("Y", "Y", "Y"))
        self.assertEqual(row["overall_status"], "failed")
        self.assertIsNone(row["first_verified_at"])
        self.assertIsNone(row["receipt_path"])

    def test_missing_source_triple_and_incomplete_processing_fail_before_pull(self):
        (next((self.gate8 / "hipass/products").rglob(pull.V_IMAGE))).unlink()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "snapshot missing"):
            gate10.run(self.args, self.work, str(self.gate8))
        self.assertEqual(LocalConnection.transfers, [])
        db = self.gate8 / "edge/state/sb_processing.sqlite"
        connection = sqlite3.connect(str(db))
        connection.execute("UPDATE sb_processing SET product_copy='N'")
        connection.commit()
        connection.close()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(subprocess.CalledProcessError):
            gate10.run(self.args, self.work, str(self.gate8))
        self.assertEqual(LocalConnection.transfers, [])

    def test_safe_paths_python37_syntax_and_cli(self):
        self.assertEqual(gate10.safe_gate8_work(str(self.gate8)), str(self.gate8))
        for value in (str(self.remote_parent), str(self.remote_parent / "production"), str(self.gate8 / "../gate8-other")):
            with self.assertRaises(ValueError):
                gate10.safe_gate8_work(value)
        safe = self.ada / "gate10-validation/gate10-new"
        self.assertEqual(gate10.safe_work(safe), safe.resolve())
        for value in (self.ada, self.ada / "gate10-validation", self.ada / "products/gate10-test"):
            with self.assertRaises(ValueError):
                gate10.safe_work(value)
        (self.work / "existing-file").touch()
        with self.assertRaises(FileExistsError):
            gate10.safe_work(self.work)
        # Parse the actual shipped Ada entry points with Python 3.7 grammar.
        for module in (gate10, pull):
            path = Path(module.__file__)
            ast.parse(path.read_text(), feature_version=(3, 7))
            result = subprocess.run([sys.executable, str(path), "--help"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
        program = Path(gate10.__file__)
        result = subprocess.run([sys.executable, str(program)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"--submit", result.stderr)


if __name__ == "__main__":
    unittest.main()
