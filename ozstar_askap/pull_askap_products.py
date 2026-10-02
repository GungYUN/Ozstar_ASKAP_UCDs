#!/usr/bin/env python3
"""Pull complete ASKAP product triples from Ozstar to ada.

Run this program on an ada login node.  Ozstar is treated as the read-only
source.  A transferable observation is exactly one ``SBxxxxx_beamyy``
directory containing all three named products: its ``.ds`` file and the MFS
Stokes-I and Stokes-V images.  Every file is SHA-256 checked before an
observation is marked complete in the ada-side SQLite ledger.

The default ada tree is entirely below
``/import/ada1/qhua0119/Combine_ImagePlot/Ozstar``.  A successful run also
writes an immutable JSON receipt locally and uploads that receipt to Ozstar.
No source product is ever deleted by this program.

The implementation intentionally uses only the Python standard library and
syntax supported by Python 3.7 so it can run in ada's older host environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


ADA_ROOT = Path("/import/ada1/qhua0119/Combine_ImagePlot/Ozstar")
OZSTAR_SOURCE_ROOT = "/fred/oz299/qhuang/ASKAP-UCDs/products"
OZSTAR_RECEIPT_ROOT = "/fred/oz299/qhuang/ASKAP-UCDs/state/transfer_receipts"
OZSTAR_USER = "qhuang"
OZSTAR_HOST = "ozstar.swin.edu.au"
# Remote inventory generation uses only the Python standard library.  Use the
# self-contained system interpreter because the project venv depends on module
# library paths that are absent in non-interactive SSH sessions.
OZSTAR_PYTHON = "/usr/bin/python3"
SSH_CONNECT_TIMEOUT = 30

PRODUCT_ROLES = ("ds", "stokes_i", "stokes_v")
I_IMAGE = "wsclean-MFS-I-image.fits"
V_IMAGE = "wsclean-MFS-V-image.fits"


# Executed by the standard-library Python interpreter on Ozstar.  It emits one
# compact JSON record for each complete DS/I/V triple and ignores incomplete
# SB directories.  Its output is a frozen, content-hashed source snapshot.
REMOTE_SNAPSHOT_PROGRAM = r"""
import hashlib
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
batches = set(sys.argv[2:])
if not root.is_dir():
    raise SystemExit("Ozstar product root does not exist: %s" % root)

def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            value.update(chunk)
    return value.hexdigest()

records = []
for sb_dir in root.rglob("SB*_beam*"):
    if (not sb_dir.is_dir() or sb_dir.is_symlink()
            or not re.fullmatch(r"SB\d+_beam\d+", sb_dir.name)):
        continue
    try:
        relative = sb_dir.relative_to(root)
    except ValueError:
        continue
    parts = relative.parts
    if len(parts) != 4:
        continue
    batch, source, longobs, sb_name = parts
    if not re.fullmatch(r"UCS\d+-\d+", batch):
        continue
    if batches and batch not in batches:
        continue
    if not re.fullmatch(r"UCS\d+", source) or longobs != "LongObs":
        continue
    match = re.fullmatch(r"SB(\d+)_beam(\d+)", sb_name)
    if not match:
        continue
    expected = {
        "ds": sb_dir / (sb_name + ".ds"),
        "stokes_i": sb_dir / "wsclean_model" / "wsclean-MFS-I-image.fits",
        "stokes_v": sb_dir / "wsclean_model" / "wsclean-MFS-V-image.fits",
    }
    if not all(path.is_file() and not path.is_symlink()
               for path in expected.values()):
        continue
    files = {}
    for role, path in expected.items():
        files[role] = {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": digest(path),
        }
    records.append({
        "batch": batch,
        "source": source,
        "sb_id": "SB" + match.group(1),
        "beam": "beam" + match.group(2),
        "sb_name": sb_name,
        "files": files,
    })

for record in sorted(records, key=lambda item: (
        item["batch"], item["source"], item["sb_id"], item["beam"])):
    print(json.dumps(record, sort_keys=True, separators=(",", ":")))
"""


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".%s.part.%s" % (path.name, os.getpid()))
    with temporary.open("wb") as handle:
        handle.write(_json_bytes(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(str(temporary), str(path))


def _safe_remote_path(value: str, label: str) -> str:
    if any(character in value for character in ("\n", "\r", "\x00")):
        raise ValueError("%s contains a control character" % label)
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or str(path) == "/":
        raise ValueError("%s is not a safe absolute path: %s" % (label, value))
    return str(path)


def _safe_ada_root(path: Path, allow_test_root: bool = False) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_absolute():
        raise ValueError("ada root must be absolute: %s" % resolved)
    if allow_test_root:
        return resolved
    required = ADA_ROOT.resolve()
    if resolved != required and required not in resolved.parents:
        raise ValueError(
            "Every ada output must remain below %s; received %s" % (required, resolved)
        )
    return resolved


def _safe_batch_names(values: Sequence[str]) -> List[str]:
    result = []
    for value in values:
        if not re.fullmatch(r"UCS\d+-\d+", value):
            raise ValueError("Unsafe batch name: %r" % value)
        result.append(value)
    return result


class RemoteConnection(object):
    """Password-safe SSH/rsync connection initiated from ada."""

    def __init__(
        self,
        user: str,
        host: str,
        password_file: Optional[Path],
        connect_timeout: int,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", user):
            raise ValueError("Unsafe Ozstar SSH user: %r" % user)
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", host):
            raise ValueError("Unsafe Ozstar SSH host: %r" % host)
        self.remote = "%s@%s" % (user, host)
        self.environment = os.environ.copy()
        self.prefix = []  # type: List[str]
        environment_password = os.environ.get("TRANSFER_PASSWORD")
        if environment_password:
            self._require_sshpass()
            self.environment["SSHPASS"] = environment_password
            self.prefix = ["sshpass", "-e"]
        elif password_file is not None and password_file.expanduser().exists():
            protected = password_file.expanduser().resolve()
            mode = stat.S_IMODE(protected.stat().st_mode)
            if mode & 0o077:
                raise RuntimeError(
                    "Refusing insecure password file %s; use chmod 600" % protected
                )
            if not protected.read_text(encoding="utf-8").strip():
                raise RuntimeError("Password file is empty: %s" % protected)
            self._require_sshpass()
            self.prefix = ["sshpass", "-f", str(protected)]
        self.ssh_options = [
            "-4",
            "-x",
            "-o",
            "ConnectTimeout=%d" % int(connect_timeout),
        ]
        self.scp_options = [
            "-4",
            "-o",
            "ConnectTimeout=%d" % int(connect_timeout),
        ]

    @staticmethod
    def _require_sshpass() -> None:
        if shutil.which("sshpass") is None:
            raise RuntimeError(
                "sshpass is required when TRANSFER_PASSWORD or a password file is used"
            )

    def _print(self, command: Sequence[str]) -> None:
        print("$ " + " ".join(shlex.quote(item) for item in command))

    def run(
        self,
        command: Sequence[str],
        capture: bool = True,
        dry_run: bool = False,
    ) -> str:
        full = self.prefix + list(command)
        self._print(full)
        if dry_run:
            return ""
        try:
            result = subprocess.run(
                full,
                env=self.environment,
                check=True,
                text=True,
                stdout=subprocess.PIPE if capture else None,
                stderr=subprocess.PIPE if capture else None,
            )
        except subprocess.CalledProcessError as exc:
            if not capture:
                raise
            details = (exc.stderr or exc.stdout or "").strip()
            if len(details) > 4000:
                details = details[-4000:]
            raise RuntimeError(
                "Command failed with exit %s%s"
                % (exc.returncode, ": " + details if details else "")
            )
        if capture and result.stderr:
            # SSH warnings are useful, but password data is never present here.
            print(result.stderr.rstrip(), file=sys.stderr)
        return result.stdout.strip() if capture and result.stdout else ""

    def ssh(self, remote_command: str, dry_run: bool = False) -> str:
        return self.run(
            ["ssh"] + self.ssh_options + [self.remote, remote_command],
            capture=True,
            dry_run=dry_run,
        )

    def rsync(
        self,
        source_root: str,
        destination_root: Path,
        files_from: Path,
        dry_run: bool,
    ) -> None:
        if shutil.which("rsync") is None:
            raise RuntimeError("rsync is required on ada for --method rsync")
        remote_shell = "ssh " + " ".join(
            shlex.quote(item) for item in self.ssh_options
        )
        command = [
            "rsync",
            "-a",
            "--partial",
            "--delay-updates",
            "--protect-args",
            "--files-from=%s" % files_from,
            "-e",
            remote_shell,
            "%s:%s/" % (self.remote, source_root.rstrip("/")),
            "%s/" % str(destination_root).rstrip("/"),
        ]
        self.run(command, capture=False, dry_run=dry_run)

    def scp_file(self, remote_absolute: str, local_path: Path, dry_run: bool) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            "scp",
            "-p",
        ] + self.scp_options + [
            "%s:%s" % (self.remote, remote_absolute),
            str(local_path),
        ]
        self.run(command, capture=False, dry_run=dry_run)

    def upload_file(self, local_path: Path, remote_absolute: str) -> None:
        command = ["scp", "-p"] + self.scp_options + [
            str(local_path),
            "%s:%s" % (self.remote, remote_absolute),
        ]
        self.run(command, capture=False, dry_run=False)


def _snapshot(
    connection: RemoteConnection,
    source_root: str,
    remote_python: str,
    batches: Sequence[str],
) -> List[Dict[str, Any]]:
    source_root = _safe_remote_path(source_root, "Ozstar product root")
    batch_names = _safe_batch_names(batches)
    command = " ".join(
        [
            shlex.quote(remote_python),
            "-c",
            shlex.quote(REMOTE_SNAPSHOT_PROGRAM),
            shlex.quote(source_root),
        ]
        + [shlex.quote(name) for name in batch_names]
    )
    output = connection.ssh(command)
    records = []
    seen = set()
    for line in output.splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        _validate_record(record)
        key = _record_key(record)
        if key in seen:
            raise RuntimeError("Duplicate Ozstar product record: %s" % (key,))
        seen.add(key)
        records.append(record)
    return sorted(records, key=_record_key)


def _record_key(record: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        record["batch"],
        record["source"],
        record["sb_id"],
        record["beam"],
    )


def _validate_record(record: Dict[str, Any]) -> None:
    if not re.fullmatch(r"UCS\d+-\d+", str(record.get("batch", ""))):
        raise RuntimeError("Invalid batch in remote snapshot: %r" % record)
    if not re.fullmatch(r"UCS\d+", str(record.get("source", ""))):
        raise RuntimeError("Invalid source in remote snapshot: %r" % record)
    if not re.fullmatch(r"SB\d+", str(record.get("sb_id", ""))):
        raise RuntimeError("Invalid SB in remote snapshot: %r" % record)
    if not re.fullmatch(r"beam\d+", str(record.get("beam", ""))):
        raise RuntimeError("Invalid beam in remote snapshot: %r" % record)
    sb_name = "%s_%s" % (record["sb_id"], record["beam"])
    if record.get("sb_name") != sb_name:
        raise RuntimeError("Remote snapshot has inconsistent SB name: %r" % record)
    files = record.get("files")
    if not isinstance(files, dict) or set(files) != set(PRODUCT_ROLES):
        raise RuntimeError("Remote snapshot does not contain exactly DS/I/V: %r" % record)
    base = "%s/%s/LongObs/%s" % (record["batch"], record["source"], sb_name)
    expected = {
        "ds": "%s/%s.ds" % (base, sb_name),
        "stokes_i": "%s/wsclean_model/%s" % (base, I_IMAGE),
        "stokes_v": "%s/wsclean_model/%s" % (base, V_IMAGE),
    }
    for role in PRODUCT_ROLES:
        item = files[role]
        if item.get("path") != expected[role]:
            raise RuntimeError("Unexpected %s path: %r" % (role, item.get("path")))
        if not re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256", ""))):
            raise RuntimeError("Invalid %s SHA-256" % role)
        if not isinstance(item.get("size"), int) or item["size"] < 0:
            raise RuntimeError("Invalid %s size" % role)


def _manifest_id(records: Sequence[Dict[str, Any]]) -> str:
    return hashlib.sha256(_json_bytes(list(records))).hexdigest()


def _local_file_status(
    products_root: Path, record: Dict[str, Any]
) -> Dict[str, str]:
    status = {}
    for role in PRODUCT_ROLES:
        item = record["files"][role]
        path = products_root / item["path"]
        if (
            path.is_file()
            and not path.is_symlink()
            and path.stat().st_size == item["size"]
            and _sha256(path) == item["sha256"]
        ):
            status[role] = "Y"
        else:
            status[role] = "N"
    return status


def _open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS transfer_ledger (
            batch TEXT NOT NULL,
            source_name TEXT NOT NULL,
            sb_id TEXT NOT NULL,
            beam TEXT NOT NULL,
            remote_source_root TEXT NOT NULL,
            local_sb_directory TEXT NOT NULL,
            manifest_id TEXT NOT NULL,
            ds_path TEXT NOT NULL,
            ds_size INTEGER NOT NULL,
            ds_sha256 TEXT NOT NULL,
            ds_status TEXT NOT NULL,
            stokes_i_path TEXT NOT NULL,
            stokes_i_size INTEGER NOT NULL,
            stokes_i_sha256 TEXT NOT NULL,
            stokes_i_status TEXT NOT NULL,
            stokes_v_path TEXT NOT NULL,
            stokes_v_size INTEGER NOT NULL,
            stokes_v_sha256 TEXT NOT NULL,
            stokes_v_status TEXT NOT NULL,
            overall_status TEXT NOT NULL,
            first_verified_at TEXT,
            last_checked_at TEXT NOT NULL,
            receipt_path TEXT,
            error TEXT,
            PRIMARY KEY (batch, source_name, sb_id, beam)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS transfer_attempts (
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            batch TEXT NOT NULL,
            source_name TEXT NOT NULL,
            sb_id TEXT NOT NULL,
            beam TEXT NOT NULL,
            attempted_at TEXT NOT NULL,
            result TEXT NOT NULL,
            error TEXT
        )
        """
    )
    connection.commit()
    return connection


def _write_ledger_row(
    connection: sqlite3.Connection,
    record: Dict[str, Any],
    source_root: str,
    products_root: Path,
    manifest_id: str,
    statuses: Dict[str, str],
    overall_status: str,
    checked_at: str,
    receipt_path: Optional[str],
    error: Optional[str],
) -> None:
    values = []
    for role in PRODUCT_ROLES:
        item = record["files"][role]
        values.extend([item["path"], item["size"], item["sha256"], statuses[role]])
    sb_directory = products_root / record["batch"] / record["source"] / "LongObs" / record["sb_name"]
    connection.execute(
        """
        INSERT INTO transfer_ledger (
            batch, source_name, sb_id, beam, remote_source_root,
            local_sb_directory, manifest_id,
            ds_path, ds_size, ds_sha256, ds_status,
            stokes_i_path, stokes_i_size, stokes_i_sha256, stokes_i_status,
            stokes_v_path, stokes_v_size, stokes_v_sha256, stokes_v_status,
            overall_status, first_verified_at, last_checked_at, receipt_path, error
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, CASE WHEN ? = 'complete' THEN ? ELSE NULL END, ?, ?, ?
        )
        ON CONFLICT(batch, source_name, sb_id, beam) DO UPDATE SET
            remote_source_root=excluded.remote_source_root,
            local_sb_directory=excluded.local_sb_directory,
            manifest_id=excluded.manifest_id,
            ds_path=excluded.ds_path,
            ds_size=excluded.ds_size,
            ds_sha256=excluded.ds_sha256,
            ds_status=excluded.ds_status,
            stokes_i_path=excluded.stokes_i_path,
            stokes_i_size=excluded.stokes_i_size,
            stokes_i_sha256=excluded.stokes_i_sha256,
            stokes_i_status=excluded.stokes_i_status,
            stokes_v_path=excluded.stokes_v_path,
            stokes_v_size=excluded.stokes_v_size,
            stokes_v_sha256=excluded.stokes_v_sha256,
            stokes_v_status=excluded.stokes_v_status,
            overall_status=excluded.overall_status,
            first_verified_at=COALESCE(transfer_ledger.first_verified_at,
                                       excluded.first_verified_at),
            last_checked_at=excluded.last_checked_at,
            receipt_path=excluded.receipt_path,
            error=excluded.error
        """,
        [
            record["batch"], record["source"], record["sb_id"], record["beam"],
            source_root, str(sb_directory), manifest_id,
        ] + values + [
            overall_status, overall_status, checked_at, checked_at,
            receipt_path, error,
        ],
    )


def _record_attempt(
    connection: sqlite3.Connection,
    run_id: str,
    record: Dict[str, Any],
    attempted_at: str,
    result: str,
    error: Optional[str],
) -> None:
    connection.execute(
        """
        INSERT INTO transfer_attempts (
            run_id, batch, source_name, sb_id, beam, attempted_at, result, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id, record["batch"], record["source"], record["sb_id"],
            record["beam"], attempted_at, result, error,
        ),
    )


def _write_transfer_list(path: Path, relative_paths: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    values = sorted(set(relative_paths))
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for value in values:
            if "\n" in value or "\r" in value or value.startswith("/") or ".." in PurePosixPath(value).parts:
                raise ValueError("Unsafe transfer-list path: %r" % value)
            handle.write(value + "\n")


def _upload_receipt(
    connection: RemoteConnection,
    local_receipt: Path,
    remote_root: str,
) -> str:
    remote_root = _safe_remote_path(remote_root, "Ozstar receipt root")
    final = "%s/%s" % (remote_root.rstrip("/"), local_receipt.name)
    part = final + ".part"
    connection.ssh("mkdir -p -- %s" % shlex.quote(remote_root))
    connection.upload_file(local_receipt, part)
    command = (
        "test ! -e {final} && mv -- {part} {final} && sha256sum -- {final}"
    ).format(final=shlex.quote(final), part=shlex.quote(part))
    output = connection.ssh(command)
    remote_digest = output.split()[0] if output else ""
    local_digest = _sha256(local_receipt)
    if remote_digest != local_digest:
        raise RuntimeError("Uploaded receipt SHA-256 verification failed")
    return final


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--all", action="store_true", help="Pull every complete product triple."
    )
    selection.add_argument(
        "--batch",
        action="append",
        default=[],
        help="Pull one batch such as UCS1501-1550; repeat as needed.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--source-root", default=OZSTAR_SOURCE_ROOT)
    parser.add_argument("--ada-root", type=Path, default=ADA_ROOT)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--remote-user", default=OZSTAR_USER)
    parser.add_argument("--remote-host", default=OZSTAR_HOST)
    parser.add_argument("--remote-python", default=OZSTAR_PYTHON)
    parser.add_argument("--receipt-remote-root", default=OZSTAR_RECEIPT_ROOT)
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--method", choices=("rsync", "scp"), default="rsync")
    parser.add_argument(
        "--connect-timeout", type=int, default=SSH_CONNECT_TIMEOUT
    )
    parser.add_argument(
        "--no-receipt-upload",
        action="store_true",
        help="Testing only: retain the local receipt without uploading it.",
    )
    return parser.parse_args()


def run_transfer(args: argparse.Namespace) -> Dict[str, Any]:
    """Execute one incremental pull and return its machine-readable result."""

    ada_root = _safe_ada_root(
        Path(args.ada_root), bool(getattr(args, "_allow_test_root", False))
    )
    source_root = _safe_remote_path(args.source_root, "Ozstar source root")
    batches = [] if args.all else _safe_batch_names(args.batch)
    password_file = args.password_file
    if password_file is None:
        password_file = ADA_ROOT / "state/ozstar-transfer-password"
    connection = RemoteConnection(
        args.remote_user,
        args.remote_host,
        Path(password_file) if password_file else None,
        args.connect_timeout,
    )

    print("Reading and hashing the complete-product snapshot on Ozstar...")
    before = _snapshot(connection, source_root, args.remote_python, batches)
    manifest_id = _manifest_id(before)
    if not before:
        result = {
            "status": "PASS",
            "message": "No complete DS/I/V product triples were found",
            "manifest_id": manifest_id,
            "observation_count": 0,
            "file_count": 0,
            "dry_run": bool(args.dry_run),
        }
        print(result["message"])
        return result

    products_root = ada_root / "products"
    state_root = ada_root / "state"
    run_id = _timestamp()
    manifest_path = state_root / "manifests" / ("ozstar-products-%s.json" % run_id)
    transfer_list = state_root / "manifests" / ("ozstar-products-%s.files" % run_id)
    database = Path(args.database) if args.database else state_root / "transfer_ledger.sqlite"
    if not database.is_absolute():
        database = ada_root / database
    database = database.expanduser().resolve()
    if not getattr(args, "_allow_test_root", False):
        _safe_ada_root(database.parent)

    initial_status = {}  # type: Dict[Tuple[str, str, str, str], Dict[str, str]]
    needed = []  # type: List[str]
    for record in before:
        statuses = _local_file_status(products_root, record)
        initial_status[_record_key(record)] = statuses
        for role in PRODUCT_ROLES:
            if statuses[role] != "Y":
                needed.append(record["files"][role]["path"])

    print(
        "Snapshot: %d complete observation(s), %d product file(s); %d file(s) need transfer"
        % (len(before), len(before) * 3, len(needed))
    )
    if args.dry_run:
        for record in before:
            statuses = initial_status[_record_key(record)]
            print(
                "%s_%s: DS=%s I=%s V=%s"
                % (
                    record["source"], record["sb_id"], statuses["ds"],
                    statuses["stokes_i"], statuses["stokes_v"],
                )
            )
        return {
            "status": "DRY_RUN",
            "manifest_id": manifest_id,
            "observation_count": len(before),
            "file_count": len(before) * 3,
            "files_needing_transfer": len(needed),
            "ada_root": str(ada_root),
            "dry_run": True,
        }

    ada_root.mkdir(parents=True, exist_ok=True)
    products_root.mkdir(parents=True, exist_ok=True)
    _atomic_json(
        manifest_path,
        {
            "created_at": run_id,
            "manifest_id": manifest_id,
            "remote": connection.remote,
            "source_root": source_root,
            "records": before,
        },
    )
    _write_transfer_list(transfer_list, needed)
    ledger = _open_ledger(database)
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        for record in before:
            statuses = initial_status[_record_key(record)]
            _write_ledger_row(
                ledger, record, source_root, products_root, manifest_id, statuses,
                "pending", checked_at, None, None,
            )
        ledger.commit()

        if needed:
            if args.method == "rsync":
                connection.rsync(
                    source_root, products_root, transfer_list, dry_run=False
                )
            else:
                for relative in sorted(needed):
                    connection.scp_file(
                        "%s/%s" % (source_root.rstrip("/"), relative),
                        products_root / relative,
                        dry_run=False,
                    )

        local_after = {}  # type: Dict[Tuple[str, str, str, str], Dict[str, str]]
        for record in before:
            local_after[_record_key(record)] = _local_file_status(products_root, record)
        local_failures = [
            key for key, values in local_after.items()
            if any(values[role] != "Y" for role in PRODUCT_ROLES)
        ]
        if local_failures:
            raise RuntimeError(
                "Ada SHA-256 verification failed for: %s" % local_failures
            )

        print("Re-reading the Ozstar snapshot to detect source changes...")
        after = _snapshot(connection, source_root, args.remote_python, batches)
        after_map = {_record_key(record): record for record in after}
        changed = [
            _record_key(record) for record in before
            if after_map.get(_record_key(record)) != record
        ]
        if changed:
            raise RuntimeError("Ozstar products changed during transfer: %s" % changed)

        receipt_observations = []
        for record in before:
            receipt_record = dict(record)
            receipt_record["transfer_status"] = {
                "ds": "Y", "stokes_i": "Y", "stokes_v": "Y"
            }
            receipt_record["overall_status"] = "complete"
            receipt_observations.append(receipt_record)
        receipt = {
            "schema_version": 1,
            "kind": getattr(args, "receipt_kind", "production-pull"),
            "status": "complete",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "manifest_id": manifest_id,
            "remote": connection.remote,
            "source_root": source_root,
            "ada_root": str(ada_root),
            "ledger": str(database),
            "observation_count": len(before),
            "file_count": len(before) * 3,
            "observations": receipt_observations,
        }
        receipt_name = "transfer-receipt-%s-%s.json" % (run_id, manifest_id[:12])
        receipt_path = state_root / "receipts" / receipt_name
        _atomic_json(receipt_path, receipt)
        remote_receipt = None
        if not args.no_receipt_upload:
            remote_receipt = _upload_receipt(
                connection, receipt_path, args.receipt_remote_root
            )

        completed_at = datetime.now(timezone.utc).isoformat()
        for record in before:
            statuses = local_after[_record_key(record)]
            _write_ledger_row(
                ledger, record, source_root, products_root, manifest_id, statuses,
                "complete", completed_at, str(receipt_path), None,
            )
            _record_attempt(
                ledger, run_id, record, completed_at, "complete", None
            )
        ledger.commit()
    except Exception as exc:
        failed_at = datetime.now(timezone.utc).isoformat()
        for record in before:
            statuses = _local_file_status(products_root, record)
            _write_ledger_row(
                ledger, record, source_root, products_root, manifest_id, statuses,
                "failed", failed_at, None, "%s: %s" % (type(exc).__name__, exc),
            )
            _record_attempt(
                ledger, run_id, record, failed_at, "failed",
                "%s: %s" % (type(exc).__name__, exc),
            )
        ledger.commit()
        raise
    finally:
        ledger.close()

    return {
        "status": "PASS",
        "run_id": run_id,
        "manifest_id": manifest_id,
        "observation_count": len(before),
        "file_count": len(before) * 3,
        "files_transferred": len(needed),
        "ada_root": str(ada_root),
        "products_root": str(products_root),
        "database": str(database),
        "manifest": str(manifest_path),
        "receipt": str(receipt_path),
        "remote_receipt": remote_receipt,
        "source_deleted": False,
        "dry_run": False,
    }


def main() -> None:
    args = _parse_args()
    try:
        result = run_transfer(args)
    except Exception as exc:
        raise SystemExit("ASKAP product pull failed: %s: %s" % (type(exc).__name__, exc))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
