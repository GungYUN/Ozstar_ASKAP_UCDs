#!/usr/bin/env python3
"""Gate9: isolated real shared-retry, exhaustion and controller-resume tests.

No Gate8/production MS, products or processing state are reused. Only the
published central index and its immutable TAP rows are read. Each controller
is a fresh subprocess so ASKAP_WORK never leaks between the two test cases.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import math
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROGRAM = Path(__file__).resolve().parent
VALIDATION_ROOT = Path("/fred/oz299/qhuang/dstools/gate9-validation")
PRODUCTION = Path("/fred/oz299/qhuang/ASKAP-UCDs")
CASES = {
    "shared-recovery": {
        "seed": 689, "sources": ["UCS689", "UCS690", "UCS693"], "target": "UCS690",
        "sb_id": "SB74372", "beam": "beam28",
        "key": "36d17f6f9b843c1d9e0c7aa0b9277258665fd02841a6c936f0728f79d844e2b7",
        "faults": [("none", "insert_model"), ("processing_retry_ready", "create_model")],
    },
    "exhaustion": {
        "seed": 225, "sources": ["UCS225"], "target": "UCS225",
        "sb_id": "SB67772", "beam": "beam18",
        "key": "6952057a1ddba073e679bf07eacf92186e68bba1648f1fa30bff8e8c028f794e",
        "faults": [(stage, "create_model") for stage in
                   ("none", "processing_retry_ready", "redownload_retry_ready")],
    },
}
PHASES = ("recovery_initial", "normal_skip_failed", "explicit_recovery", "success_idempotence",
          "exhaustion_initial", "explicit_exhaustion", "exhaustion_idempotence")
STEPS = ("preprocess", "create_model", "insert_model", "subtract_model", "extract_ds",
         "crop_i", "crop_v", "product_copy", "cleanup")
LOGGER = logging.getLogger("askap.gate9")


def read_json(path: Path, default=None):
    return json.loads(path.read_text()) if path.is_file() else default


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def isolated_work(path: Path) -> Path:
    work = path.expanduser().resolve()
    if "gate9" not in str(work).lower() or "validation" not in str(work).lower():
        raise ValueError("Work path must identify an isolated gate9 validation tree")
    protected = (PRODUCTION, Path("/fred/oz299/qhuang/ASKAP-UCDs-shared"), PROGRAM.parent)
    if any(work.is_relative_to(p.resolve()) or p.resolve().is_relative_to(work) for p in protected):
        raise ValueError(f"Gate9 work overlaps a program/production tree: {work}")
    return work


def environment(work: Path, settings: dict) -> dict:
    env = dict(os.environ)
    env.pop("ASKAP_GATE6_FAULT_CONFIG", None)
    env.update({
        "ASKAP_WORK": str(work), "ASKAP_CATALOGUE": settings["catalogue"],
        "ASKAP_TIME_CSV": settings["time_csv"], "DSTOOLS_CONTAINER": settings["container"],
        "ASKAP_PYTHON_BIN": sys.executable, "ASKAP_CASDA_RADIUS_FREQUENCY_MODE": "central",
        "ASKAP_GATE9_FAULT_CONFIG": str(work / "state/gate9-fault-config.json"),
        "ASKAP_PARALLEL_SLOTS": "1", "ASKAP_CPU_PER_SLOT": "8", "ASKAP_CREATE_MODEL_THREADS": "8",
        "ASKAP_SBATCH_MEM": "40G", "ASKAP_SBATCH_TMP": "10G", "ASKAP_MAX_MS_PER_JOB": "1",
        "ASKAP_HOURS_PER_OBS": "2", "ASKAP_SHARED_TARGET_HOURS": "0.5",
        "ASKAP_SOURCE_OVERHEAD_HOURS": "2", "ASKAP_MAX_JOB_WALLTIME_HOURS": "48",
    })
    return env


def rows(work: Path) -> dict:
    database = work / "state/sb_processing.sqlite"
    if not database.is_file():
        return {}
    connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return {r["source_name"]: dict(r) for r in connection.execute("SELECT * FROM sb_processing")}
    finally:
        connection.close()


def submissions(work: Path) -> dict:
    return {str(p): read_json(p)["job_id"] for p in
            sorted((work / "state/jobs").glob("*.json.submission.json"))}


def slurm_job_ids(work: Path) -> list[str]:
    return sorted({p.stem.removeprefix("slurm-") for p in (work / "logs").glob("slurm-*.err")
                   if p.stem.removeprefix("slurm-").isdigit()})


def download_count(work: Path) -> int:
    return len(list((work / "staging").rglob("download-ready.json")))


def controller_complete(work: Path) -> bool:
    return read_json(work / "state/shared-controller.json", {}).get("status") == "complete"


def product_files(work: Path, case: dict, name: str) -> list[Path]:
    number = int(name[3:])
    start = (number - 1) // 50 * 50 + 1
    sb_name = case["sb_id"] + "_" + case["beam"]
    root = work / "products" / f"UCS{start}-{start + 49}" / name / "LongObs" / sb_name
    return [root / (sb_name + ".ds"), root / "wsclean_model/wsclean-MFS-I-image.fits",
            root / "wsclean_model/wsclean-MFS-V-image.fits"]


def fingerprints(work: Path, case: dict, names: list[str]) -> dict:
    return {str(p.relative_to(work)): digest(p) for name in names for p in product_files(work, case, name)}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


class Runner:
    def __init__(self, root: Path, settings: dict, state: dict, poll: float, timeout: float):
        self.root, self.settings, self.state = root, settings, state
        self.poll, self.timeout = poll, timeout

    def save(self):
        write_json(self.root / "state/gate9-checkpoint.json", self.state)

    def run(self, case_name: str, retry: bool = False, interrupt: bool = False):
        work, case = self.root / case_name, CASES[case_name]
        command = [sys.executable, str(PROGRAM / "ozstar_main.py"),
            "--begin", str(case["seed"]), "--end", str(case["seed"]),
            "--shared-observation-key", case["key"], "--parallel-slots", "1",
            "--controller-poll-interval", str(self.poll), "--max-resubmits", "2", "--submit"]
        if retry:
            command.append("--retry-existing")
        log = work / "logs/controller.log"
        started = time.monotonic()
        with log.open("a") as handle:
            child = subprocess.Popen(command, cwd=PROGRAM, env=environment(work, self.settings),
                                     stdout=handle, stderr=subprocess.STDOUT)
            write_json(self.root / "state/gate9-child.json", {
                "host": socket.gethostname(), "pid": child.pid, "case": case_name,
                "command": command, "started_at": datetime.now(timezone.utc).isoformat()})
            try:
                while child.poll() is None:
                    if interrupt and submissions(work):
                        self.state["interruption"] = {"submission": submissions(work),
                            "host": socket.gethostname(), "controller_pid": child.pid,
                            "signal": "SIGINT (controller only)"}
                        self.save()  # Persist BEFORE signalling; resume never injects this twice.
                        LOGGER.info("Controlled interruption: stopping controller PID %s; Slurm jobs retained", child.pid)
                        child.send_signal(signal.SIGINT)
                        child.wait(timeout=30)
                        require(child.returncode != 0, "Controller finished normally before SIGINT was exercised")
                        return
                    if time.monotonic() - started > self.timeout:
                        raise TimeoutError(f"Controller wait limit reached; resume later: {log}")
                    LOGGER.info("Waiting: %s retry=%s; host=%s pid=%s; log=%s",
                                case_name, retry, socket.gethostname(), child.pid, log)
                    time.sleep(self.poll)
                require(child.returncode == 0, f"Controller exit={child.returncode}; inspect {log}")
                require(not interrupt, "Initial job finished before controlled interruption; use a fresh Gate9 work")
            finally:
                if child.poll() is None:
                    # Only our known isolated controller child, never any Slurm job.
                    child.send_signal(signal.SIGINT)
                    try:
                        child.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        child.terminate()
                        child.wait(timeout=30)

    def phase(self, phase: str):
        recovery = phase in PHASES[:4]
        label = "shared-recovery" if recovery else "exhaustion"
        work, case = self.root / label, CASES[label]
        target = case["target"]
        current = rows(work)
        row = current.get(target, {})
        if phase == "recovery_initial":
            if not self.state.get("interruption"):
                self.run(label, interrupt=True)
            before = self.state["interruption"]["submission"]
            require(len(before) == 1, "Initial shared test must have exactly one submission")
            if row.get("retry_stage") != "processing_failed" or not controller_complete(work):
                self.run(label)
            require(submissions(work) == before, "Controller resume duplicated the initial submission")
            current = rows(work)
            require(current[target]["error_step"] == "insert_model", "Initial controlled insert failure missing")
            require(current[target]["retry_stage"] == "processing_failed", "Wrong initial retry stage")
            others = [s for s in case["sources"] if s != target]
            require(all(current[s]["retry_stage"] == "complete" for s in others), "Shared partners did not complete")
            require(len({current[s]["model_origin"] for s in case["sources"]}) == 1, "Initial targets did not share one master")
            require(bool(list((work / "staging/_shared").rglob("model-ready.json"))), "Failed target lost the retained public master")
            require(bool(list((work / "staging" / target).rglob("*.ms"))), "Failed target input MS was deleted")
            self.state["protected_products"] = fingerprints(work, case, others)
        elif phase in ("normal_skip_failed", "success_idempotence", "exhaustion_idempotence"):
            before = (submissions(work), download_count(work), rows(work))
            self.run(label)
            if phase != "normal_skip_failed":
                self.run(label, retry=True)
            after = (submissions(work), download_count(work), rows(work))
            require(before == after, f"{phase} changed submissions/downloads/SB state")
        elif phase == "exhaustion_initial":
            if row.get("retry_stage") != "processing_failed" or not controller_complete(work):
                self.run(label)
            current = rows(work)
            require(current[target]["error_step"] == "create_model", "Initial master failure missing")
            require(current[target]["retry_stage"] == "processing_failed", "Wrong initial exhaustion stage")
            before = (submissions(work), download_count(work))
            self.run(label)
            require(before == (submissions(work), download_count(work)), "Normal run retried the failed master")
        else:
            terminal = "complete" if recovery else "exhausted"
            if row.get("retry_stage") != terminal or not controller_complete(work):
                self.run(label, retry=True)
            current = rows(work)
            row = current[target]
            require(row["retry_stage"] == terminal, f"Expected {terminal}, found {row}")
            require(row["processing_retry_count"] == row["redownload_retry_count"] == 1, "Retry counters are not bounded at one each")
            require(len(submissions(work)) == 3, "Expected initial + processing retry + redownload retry")
            require(len(set(submissions(work).values())) == 3, "Submission IDs are not unique")
            logged_ids = slurm_job_ids(work)
            if logged_ids:
                require(set(logged_ids) == set(submissions(work).values()),
                        "Additional Slurm attempts occurred; inspect partial/failed worker logs")
            require(download_count(work) == 2, "Expected exactly initial and fresh-redownload archives")
            events = read_json(work / "state/gate9-fault-events.json", [])
            require([(e["retry_stage"], e["step"]) for e in events] == [tuple(p) for p in case["faults"]], "Unexpected controlled fault events")
            logs = sorted((work / "logs/failures").glob("*.log"))
            require(len(logs) == len(case["faults"]), "Controlled failure logs missing or unexpected failures occurred")
            require(any("reset_askap_ms.py" in p.read_text(errors="replace") for p in logs), "Processing retry did not reset the preprocessed MS")
            if recovery:
                require(all(current[s]["retry_stage"] == "complete" for s in case["sources"]), "Final recovery incomplete")
                require(all(row[s] == "Y" for s in STEPS), "Recovered target has unsuccessful steps")
                require(row["model_reused"] == 0 and row["create_model_seconds"] > 0 and row["preprocess_seconds"] > 0,
                        "Retry did not rebuild a private preprocessed model")
                require(not list((work / "staging").rglob("*.ms")), "Successful shared recovery did not release MS inputs")
                others = [s for s in case["sources"] if s != target]
                require(fingerprints(work, case, others) == self.state["protected_products"], "Retry changed a successful partner's products")
                fingerprints(work, case, case["sources"])  # All nine filenames must exist.
            else:
                require(row["error_step"] == "create_model", "Exhaustion did not retain its exact failure step")
                require(not any(p.exists() for p in product_files(work, case, target)), "Exhausted case unexpectedly produced products")
                require(bool(list((work / "staging" / target).rglob("*.ms"))), "Final failed MS was not retained")
        if recovery and phase != "recovery_initial":
            others = [s for s in case["sources"] if s != target]
            require(fingerprints(work, case, others) == self.state["protected_products"], "Successful partner products changed")


def setup_cases(root: Path, settings: dict):
    for name, case in CASES.items():
        work = root / name
        (work / "state").mkdir(parents=True, exist_ok=True)
        (work / "logs").mkdir(exist_ok=True)
        snapshot = work / "state/crosscatalogue.json"
        if not snapshot.exists():
            shutil.copy2(root / "state/frozen-crosscatalogue.json", snapshot)
        fault = work / "state/gate9-fault-config.json"
        payload = {"gate": "gate9", "work": str(work), "rules": [
            {"source_name": case["target"], "sb_id": case["sb_id"], "beam": case["beam"],
             "retry_stage": stage, "step": step} for stage, step in case["faults"]]}
        if fault.exists():
            require(read_json(fault) == payload, f"Fault configuration changed: {fault}")
        else:
            write_json(fault, payload)
        check = (
            "import json,sys; from pathlib import Path; import config,crosscatalogue; "
            "s=json.loads(Path(sys.argv[1]).read_text()); "
            "assert s['signature']==crosscatalogue.selection_signature(), 'Index/config mismatch'; "
            "assert s.get('finished_at'); "
            "assert all(x['query_status'] in ('available','no_observations') for x in s['sources']); "
            "g=[g for g in crosscatalogue.groups_for_range(s,int(sys.argv[2]),int(sys.argv[2])) "
            "if g['observation']['key']==sys.argv[3]]; assert len(g)==1; "
            "assert {x['source_name'] for x in g[0]['sources']}==set(sys.argv[4].split(',')); "
            "assert all(Path(x['query_cache']).is_file() for x in g[0]['sources']); "
            "print('Gate9 frozen-index preflight: PASS')"
        )
        subprocess.run([sys.executable, "-c", check, str(snapshot), str(case["seed"]),
                        case["key"], ",".join(case["sources"])], cwd=PROGRAM,
                       env=environment(work, settings), check=True)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--index-work", type=Path, default=Path("/fred/oz299/qhuang/dstools/shared-validation/central-frequency-20261008"))
    parser.add_argument("--catalogue", type=Path, default=PRODUCTION / "catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv")
    parser.add_argument("--time-csv", type=Path, default=PRODUCTION / "catalogue/All_combine_data_unrepetition.csv")
    parser.add_argument("--container", type=Path, default=Path("/fred/oz299/qhuang/dstools/dstools-260925.sif"))
    parser.add_argument("--poll-interval", type=float, default=30)
    parser.add_argument("--timeout-hours", type=float, default=48, help="Per-controller driver wait bound, not a Slurm time request")
    parser.add_argument("--resume", action="store_true", help="Resume the same isolated work and phase checkpoint")
    parser.add_argument("--submit", action="store_true", help="Authorize real downloads, Slurm jobs and controlled controller SIGINT")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.submit or (args.resume and args.work is None):
        raise SystemExit("Use --submit; --resume also requires the original --work")
    if os.environ.get("SLURM_JOB_ID") or shutil.which("sbatch") is None:
        raise SystemExit("Run Gate9 on an Ozstar login node")
    if not math.isfinite(args.poll_interval) or not math.isfinite(args.timeout_hours) or args.poll_interval < 1 or args.timeout_hours <= 0:
        raise SystemExit("Poll interval must be >=1 and timeout must be positive")
    work = isolated_work(args.work or VALIDATION_ROOT / ("gate9-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")))
    if not args.resume and work.exists() and any(work.iterdir()):
        raise SystemExit(f"Work already contains data; use --resume or a new directory: {work}")
    checkpoint = work / "state/gate9-checkpoint.json"
    if args.resume and not checkpoint.is_file():
        raise SystemExit(f"No Gate9 checkpoint to resume: {checkpoint}")
    (work / "state").mkdir(parents=True, exist_ok=True)
    (work / "logs").mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(work / "logs/gate9-driver.log")])
    with (work / "state/gate9-driver.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"Gate9 driver is already running; do not delete its lock. Inspect {work / 'state/gate9-runtime.json'}")
        write_json(work / "state/gate9-runtime.json", {"host": socket.gethostname(), "pid": os.getpid()})
        if args.resume:
            state = read_json(checkpoint)
            require(state.get("gate") == "gate9", "Not a Gate9 checkpoint")
            require(state.get("completed_phases") == list(PHASES[:len(state.get("completed_phases", []))]),
                    "Gate9 checkpoint phases are inconsistent")
            settings = state["settings"]
        else:
            settings = {"catalogue": str(args.catalogue.expanduser().resolve()),
                "time_csv": str(args.time_csv.expanduser().resolve()),
                "container": str(args.container.expanduser().resolve())}
            for value in settings.values():
                require(Path(value).is_file(), f"Required input missing: {value}")
            source = args.index_work.expanduser().resolve() / "state/crosscatalogue.json"
            require(source.is_file(), f"Published central index missing: {source}")
            shutil.copy2(source, work / "state/frozen-crosscatalogue.json")
            state = {"gate": "gate9", "settings": settings, "completed_phases": [],
                     "container_sha256": digest(Path(settings["container"])),
                     "frozen_index_sha256": digest(work / "state/frozen-crosscatalogue.json")}
            write_json(checkpoint, state)
        runner = Runner(work, settings, state, args.poll_interval, args.timeout_hours * 3600)
        result_path = work / "state/gate9-result.json"
        try:
            require(digest(Path(settings["container"])) == state["container_sha256"], "SIF changed since Gate9 started")
            require(digest(work / "state/frozen-crosscatalogue.json") == state["frozen_index_sha256"], "Gate9 frozen index changed")
            subprocess.run(["sha256sum", "-c", settings["container"] + ".sha256"], check=True)
            setup_cases(work, settings)
            for phase in PHASES:
                if phase in state["completed_phases"]:
                    continue
                LOGGER.info("Gate9 phase: %s; work=%s; host=%s", phase, work, socket.gethostname())
                runner.phase(phase)
                state["completed_phases"].append(phase)
                runner.save()
            result = {"gate": "gate9", "status": "PASS", "work": str(work),
                "completed_phases": state["completed_phases"], "interruption": state["interruption"],
                "cases": {name: {"sb_id": case["sb_id"], "target": case["target"],
                    "rows": rows(work / name), "submissions": submissions(work / name),
                    "slurm_job_ids": slurm_job_ids(work / name),
                    "successful_archive_downloads": download_count(work / name),
                    "fault_events": read_json(work / name / "state/gate9-fault-events.json", [])}
                    for name, case in CASES.items()},
                "resources": {"cpus": 8, "memory": "40G", "tmp": "10G",
                              "shared_walltime_hours": 5.5, "private_walltime_hours": 4.5}}
            write_json(result_path, result)
        except (Exception, KeyboardInterrupt) as exc:
            write_json(result_path, {"gate": "gate9", "status": "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAIL",
                "work": str(work), "completed_phases": state["completed_phases"],
                "error": f"{type(exc).__name__}: {exc}"})
            LOGGER.exception("Gate9 stopped; retain all state; use --resume after inspecting %s", result_path)
            raise SystemExit(130 if isinstance(exc, KeyboardInterrupt) else 1) from exc
    print(f"Gate9 shared retry validation: PASS\nResult: {result_path}\nWork: {work}")


if __name__ == "__main__":
    def stop_driver(signum, frame):
        raise KeyboardInterrupt(f"Driver received signal {signum}; Slurm jobs are not cancelled")
    for interrupt_signal in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(interrupt_signal, stop_driver)
    main()
