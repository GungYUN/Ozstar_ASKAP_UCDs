"""Offline Slurm worker: parallel observations, serial targets per observation.

The master is never passed to insert-model, uvsub, extract-ds or crop. Every
target gets a real independent copy (no hard links), so masks/subtraction cannot
accumulate. Failed targets keep their input; retries build a private model.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from urllib.parse import unquote

try:
    from . import config, process_job as pipeline, sb_status
    from .crosscatalogue import digest_file, exclusive_lock
    from .pipeline_utils import (atomic_write_json, read_json, utc_now, directory_size,
        source_product_root, read_t_min_map)
except ImportError:
    import config, process_job as pipeline, sb_status
    from crosscatalogue import digest_file, exclusive_lock
    from pipeline_utils import (atomic_write_json, read_json, utc_now, directory_size,
        source_product_root, read_t_min_map)

logger = logging.getLogger("askap.shared_worker")


def model_settings(sif_sha256: str) -> dict:
    return {"format": 1, "sif_sha256": sif_sha256, "band": config.BAND,
        "iterations": config.CREATE_MODEL_ITERATIONS, "threads": config.CREATE_MODEL_THREADS,
        "min_uv_metres": config.MIN_UV_METRES, "multiscale": config.CREATE_MODEL_MULTISCALE}


def product_dir(source: dict, obs: dict) -> Path:
    return source_product_root(source["ucs_number"], source["source_name"]) / obs["sb_name"]


def products_exist(path: Path, sb_name: str) -> bool:
    return ((path / (sb_name + ".ds")).is_file()
        and all((path / "wsclean_model" / f"wsclean-MFS-{pol}-image.fits").is_file() for pol in ("I", "V")))


def already_complete(source: dict, obs: dict) -> bool:
    """Never attribute another product revision's files to this observation."""
    path = product_dir(source, obs)
    if not products_exist(path, obs["sb_name"]):
        return False
    provenance = read_json(path / "observation.json", {})
    if provenance:
        if provenance.get("observation_key") != obs["key"]:
            raise RuntimeError(f"Existing products belong to another data product: {path}")
        return True
    # Older products have no sidecar. Adopt only with matching database evidence.
    status = sb_status.observation_status(source["source_name"], obs["sb_id"], obs["beam"])
    if unquote(status.get("ms_name") or "") != obs["filename"]:
        raise RuntimeError(f"Legacy products need an identity check before reuse: {path}")
    if status.get("observation_key") not in (None, obs["key"]):
        raise RuntimeError(f"Observation identity conflict: {path}")
    return True


def check_copy_space(master: Path) -> None:
    needed = directory_size(master)
    if shutil.disk_usage(config.WORK).free < needed:
        raise RuntimeError(f"Not enough persistent space to copy {master} ({needed} bytes)")
    if directory_size(config.STAGING_ROOT) + needed > config.STAGING_BUDGET_BYTES:
        raise RuntimeError("Independent MS/model copy would exceed the staging budget")


def _record(source, obs, step, status, **kwargs):
    sb_status.record_step(source["source_name"], obs["sb_id"], step, status, beam=obs["beam"], **kwargs)


def _record_failure(source, obs, step, error, log_path=None):
    _record(source, obs, step, "N", error=str(error), failure_log_path=log_path)
    stage = sb_status.retry_stage(source["source_name"], obs["sb_id"], beam=obs["beam"])
    next_stage = {"processing_retry_ready": "redownload_required", "redownload_retry_ready": "exhausted",
                  "redownload_required": "exhausted", "exhausted": "exhausted"}.get(stage, "processing_failed")
    sb_status.set_retry_stage(source["source_name"], obs["sb_id"], next_stage, beam=obs["beam"])


def extract_archive(manifest: dict, log_path: Path, deadline=None) -> Path:
    root = Path(manifest["shared_root"])
    obs = manifest["observation"]
    extracted = root / "extracted"
    marker = root / "extracted.json"
    previous = read_json(marker, {})
    if previous and Path(previous["ms_path"]).is_dir():
        return Path(previous["ms_path"])
    archive = Path(manifest["archive_path"])
    if not archive.is_file():
        raise FileNotFoundError(f"Required staged archive is absent: {archive}")
    # Validate member paths, but perform extraction with command-line tar.
    with tarfile.open(archive) as handle:
        for member in handle:
            path = Path(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise RuntimeError(f"Unsafe archive member: {member.name}")
            if member.issym() or member.islnk():
                link = Path(member.linkname)
                if link.is_absolute() or ".." in link.parts:
                    raise RuntimeError(f"Unsafe archive link: {member.name}")
    if extracted.exists():
        # Incomplete extraction only; the intact tar is still present here.
        shutil.rmtree(extracted)
    extracted.mkdir(parents=True)
    with log_path.open("a") as log:
        command = ["tar", "-xf", str(archive), "-C", str(extracted)]
        log.write(f"{utc_now()} {command!r}\n")
        log.flush()
        try:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True,
                           timeout=pipeline._command_timeout(deadline))
        except subprocess.TimeoutExpired as exc:
            raise pipeline.NotEnoughTime("Archive extraction reached walltime reserve") from exc
    expected_ms = obs["filename"]
    for suffix in (".tar.gz", ".tgz", ".tar"):
        if expected_ms.endswith(suffix):
            expected_ms = expected_ms[:-len(suffix)]
            break
    matches = [p for p in extracted.rglob(expected_ms) if p.is_dir()]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one {expected_ms} in {archive}; found {len(matches)}")
    atomic_write_json(marker, {"ms_path": str(matches[0]), "observation_key": obs["key"]})
    archive.unlink()  # Keep the neighbouring checksum file.
    return matches[0]


def prepare_master(manifest: dict, deadline: float, log_path: Path) -> Path:
    """Publish only after preprocess and all model filenames are available."""
    root = Path(manifest["shared_root"])
    ready = read_json(root / "model-ready.json", {})
    if ready.get("model_key") == manifest["model_key"]:
        master = Path(ready["master"])
        if master.is_dir() and pipeline._find_ms(master) and _model_files_exist(master):
            manifest["shared_model_built_this_job"] = False
            return master
    manifest["active_step"] = "extract_archive"
    raw_ms = extract_archive(manifest, log_path, deadline)
    master = root / "master"
    if master.exists():
        # A killed preprocess/create-model must not become a reusable master.
        shutil.rmtree(master)
    # Serialize only input copies, not imaging. Otherwise all four processes
    # could pass a free-space check before any of their copies is visible.
    with exclusive_lock(config.STATE_ROOT / "shared-copy.lock", wait=True):
        check_copy_space(raw_ms)
        master.mkdir()
        shutil.copytree(raw_ms, master / raw_ms.name)
    ms = master / raw_ms.name
    manifest["active_step"] = "preprocess"
    step_started = time.monotonic()
    pipeline._run_dstools(pipeline._container_command("dstools-askap-preprocess", str(ms)), master, log_path, deadline)
    timings = {"preprocess": time.monotonic() - step_started}
    manifest["active_step"] = "create_model"
    temp = root / "wsclean_tmp"
    temp.mkdir(exist_ok=True)
    step_started = time.monotonic()
    command = pipeline.create_model_command(ms, temp)
    if os.environ.get("ASKAP_GATE9_FAULT_CONFIG"):
        source, obs = manifest["sources"][0], manifest["observation"]
        pipeline._inject_gate9_failure(source["source_name"], obs["sb_id"], obs["beam"],
            sb_status.retry_stage(source["source_name"], obs["sb_id"], beam=obs["beam"]),
            "create_model", command, log_path)
    pipeline._run_dstools(command, master, log_path, deadline)
    timings["create_model"] = time.monotonic() - step_started
    if not _model_files_exist(master):
        raise RuntimeError("Shared create-model did not produce MFS I/V and channel models")
    atomic_write_json(root / "model-ready.json", {"model_key": manifest["model_key"],
        "master": str(master), "timings": timings, "finished_at": utc_now()})
    manifest["shared_model_built_this_job"] = True
    return master


def _model_files_exist(master: Path) -> bool:
    model = master / "wsclean_model"
    return (all((model / name).is_file() for name in (
        "wsclean-MFS-I-model.fits", "wsclean-MFS-I-image.fits", "wsclean-MFS-V-image.fits"))
        and any("MFS" not in path.name for path in model.glob("*-model.fits")))


def _private_copy(source: dict, obs: dict, master: Path, root: Path, reuse: bool) -> Path:
    target = config.STAGING_ROOT / source["source_name"] / "LongObs" / obs["sb_name"]
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        # Preserve old failed/partial work when replacing it with a fresh copy.
        backup = target.with_name(target.name + ".previous-" + str(time.time_ns()))
        target.rename(backup)
    target.mkdir()
    ms = pipeline._find_ms(master) if reuse else master
    if ms is None or not ms.is_dir():
        raise FileNotFoundError(f"Shared input MS missing: {master}")
    with exclusive_lock(config.STATE_ROOT / "shared-copy.lock", wait=True):
        check_copy_space(master)
        shutil.copytree(ms, target / ms.name)
        if reuse:
            shutil.copytree(master / "wsclean_model", target / "wsclean_model")
    for checksum in (root / "download").glob("*.checksum"):
        shutil.copy2(checksum, target.parent / checksum.name)
    return target


def run_manifest(manifest: dict, deadline: float | None = None) -> int:
    if "tasks" in manifest:
        return run_batch(manifest)
    root = Path(manifest["shared_root"])
    root.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(root / "worker.lock"):
        try:
            result = _run_manifest(manifest, deadline)
        except (sb_status.StateDatabaseError, sqlite3.Error) as exc:
            manifest["fatal_state_error"] = str(exc)
            atomic_write_json(Path(manifest["manifest_path"]), manifest)
            raise
        manifest["outcome"] = "partial" if result == config.PARTIAL_EXIT_CODE else "finished"
        atomic_write_json(Path(manifest["manifest_path"]), manifest)
        return result


def _run_manifest(manifest: dict, deadline: float | None = None) -> int:
    root = Path(manifest["shared_root"])
    obs = manifest["observation"]
    if deadline is None:
        deadline = time.monotonic() + manifest["job_walltime_hours"] * 3600
    manifest_path = Path(manifest["manifest_path"])
    # Frozen inputs are part of model identity. A changed SIF must never reuse it.
    if model_settings(digest_file(config.CONTAINER)) != manifest["model_settings"]:
        raise RuntimeError("Worker model settings/SIF differ from the frozen manifest")
    results = manifest.setdefault("results", {})
    log_path = config.LOG_ROOT / ".sb_tmp" / (manifest_path.stem + "-shared.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pending = [s for s in manifest["sources"] if results.get(s["source_name"], {}).get("status") not in {"complete", "failed"}]
    master = None
    if pending and manifest["mode"] == "shared":
        try:
            master = prepare_master(manifest, deadline, log_path)
        except pipeline.NotEnoughTime:
            atomic_write_json(manifest_path, manifest)
            return config.PARTIAL_EXIT_CODE
        except sb_status.StateDatabaseError:
            raise
        except Exception as exc:
            failure = config.LOG_ROOT / "failures" / (manifest_path.stem + "-shared.log")
            failure.parent.mkdir(parents=True, exist_ok=True)
            if log_path.exists():
                shutil.move(str(log_path), failure)
            for source in pending:
                _record_failure(source, obs, manifest.get("active_step", "worker_setup"), exc, failure)
                results[source["source_name"]] = {"status": "failed", "error": str(exc)}
            manifest["finished_at"] = utc_now()
            atomic_write_json(manifest_path, manifest)
            return 0  # Observational failure: continue other groups.
    epochs = read_t_min_map(config.TIME_CSV_PATH)
    if obs.get("t_min") is not None:
        epochs[int(obs["sb_id"][2:])] = obs["t_min"]
    for source in pending:
        if time.monotonic() + config.WALLTIME_RESERVE_MINUTES * 60 >= deadline:
            atomic_write_json(manifest_path, manifest)
            return config.PARTIAL_EXIT_CODE
        name = source["source_name"]
        target = config.STAGING_ROOT / name / "LongObs" / obs["sb_name"]
        try:
            manifest["active_step"] = "worker_setup"
            if already_complete(source, obs):
                results[name] = {"status": "complete"}
                continue
            reuse = manifest["mode"] == "shared"
            if reuse:
                target = _private_copy(source, obs, master, root, True)
                for step in ("extract_archive", "preprocess", "create_model"):
                    _record(source, obs, step, "Y", seconds=0)
                sb_status.set_model_origin(name, obs["sb_id"], obs["beam"], obs["key"], str(master), True)
            else:
                # Explicit retry: always rebuild a private model. Existing MS is
                # rolled back by process_sb; final retries start from a fresh tar.
                if not pipeline._find_ms(target):
                    manifest["active_step"] = "extract_archive"
                    fallback = manifest.get("fallback_raw_ms")
                    raw = Path(fallback) if fallback else extract_archive(manifest, log_path, deadline)
                    target = _private_copy(source, obs, raw, root, False)
                    _record(source, obs, "extract_archive", "Y")
                sb_status.set_model_origin(name, obs["sb_id"], obs["beam"], obs["key"], str(target), False)
            result = pipeline.process_sb(source, target, product_dir(source, obs), epochs, deadline, shared_model=reuse)
            results[name] = result
            if result["status"] == "complete":
                atomic_write_json(product_dir(source, obs) / "observation.json", {
                    "observation_key": obs["key"], "model_key": manifest["model_key"],
                    "model_reused": reuse, "index_revision": manifest["crosscatalogue_revision"],
                    "source_name": name, "filename": obs["filename"], "completed_at": utc_now()})
            atomic_write_json(manifest_path, manifest)
            if result["status"] == "partial":
                return config.PARTIAL_EXIT_CODE
        except pipeline.NotEnoughTime:
            atomic_write_json(manifest_path, manifest)
            return config.PARTIAL_EXIT_CODE
        except sb_status.StateDatabaseError:
            raise
        except Exception as exc:
            logger.exception("Shared target failed: %s %s", name, obs["sb_name"])
            _record_failure(source, obs, manifest.get("active_step", "worker_setup"), exc, log_path if log_path.exists() else None)
            results[name] = {"status": "failed", "error": str(exc)}
            atomic_write_json(manifest_path, manifest)
    manifest["finished_at"] = utc_now()
    manifest["shared_model_seconds"] = read_json(root / "model-ready.json", {}).get("timings", {})
    atomic_write_json(manifest_path, manifest)
    # Public inputs survive failures and pending companions. Delete only when
    # every indexed companion has its own DS/I/V, and only derived directories.
    if all(already_complete(s, obs) for s in manifest["all_sources"]):
        # A private retry can be the last successful companion. It must also
        # release the retained public model, but never other retry directories.
        for cleanup_root in {root, Path(manifest.get("public_root", str(root)))}:
            for name in ("master", "extracted", "wsclean_tmp"):
                path = cleanup_root / name
                if path.exists():
                    shutil.rmtree(path)
            (cleanup_root / "model-ready.json").unlink(missing_ok=True)
            (cleanup_root / "extracted.json").unlink(missing_ok=True)
    if log_path.exists() and all(r["status"] == "complete" for r in results.values()):
        log_path.unlink()
    return 0


def slot_environment(slots: int) -> dict[str, str]:
    """Subprocess isolation: each WSClean sees only its share of job resources."""
    environment = dict(os.environ)
    environment["ASKAP_PARALLEL_SLOTS"] = "1"
    environment["SLURM_CPUS_PER_TASK"] = str(config.CPU_PER_SLOT)
    memory = os.environ.get("SLURM_MEM_PER_NODE")
    if memory is None:
        match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)", config.SBATCH_MEM.upper())
        if not match:
            raise ValueError("Cannot divide ASKAP_SBATCH_MEM into per-slot memory budgets")
        memory = str(int(float(match[1]) * {"": 1, "K": 1/1024, "M": 1, "G": 1024, "T": 1024**2}[match[2]]))
    budget = int(memory) // slots
    if budget < 1:
        raise ValueError("Per-slot memory must be at least 1 MiB")
    environment["SLURM_MEM_PER_NODE"] = str(budget)
    # DStools uses the Slurm hint for MS chunking; WSClean additionally needs
    # its explicit limit, otherwise it detects all host RAM. Leave headroom.
    environment["ASKAP_WSCLEAN_ABS_MEM"] = str(max(1, int(budget / 1024 * 0.75)))
    environment["ASKAP_WSCLEAN_SLOT_THREADS"] = str(config.CPU_PER_SLOT)
    environment["ASKAP_WSCLEAN_SLOT_BIN"] = str(config.PROGRAM_DIR / "wsclean_slot")
    return environment


def _run_child(path: Path, deadline: float, environment: dict, log_path: Path) -> int:
    with log_path.open("w") as log:
        return subprocess.run([sys.executable, str(Path(__file__).resolve()),
            "--manifest", str(path), "--deadline", str(deadline)], env=environment,
            stdout=log, stderr=subprocess.STDOUT, check=False).returncode


def run_batch(job: dict) -> int:
    """One Slurm job, independent observation processes, serial targets per lane."""
    slots = job["parallel_slots"]
    if slots < 1 or slots * config.CPU_PER_SLOT > 32:
        raise ValueError("Invalid shared-job CPU layout")
    allocated = int(os.environ.get("SLURM_CPUS_PER_TASK", slots * config.CPU_PER_SLOT))
    if allocated < slots * config.CPU_PER_SLOT:
        raise ValueError("Slurm allocated fewer CPUs than the frozen shared-job layout")
    environment = slot_environment(slots)
    deadline = time.monotonic() + job["job_walltime_hours"] * 3600
    stopped = Event()
    lanes = [[entry for entry in job["tasks"] if entry["lane"] == lane] for lane in range(slots)]

    def run_lane(entries):
        for entry in entries:
            if stopped.is_set() or time.monotonic() + config.WALLTIME_RESERVE_MINUTES * 60 >= deadline:
                return
            path = Path(entry["manifest_path"])
            task = read_json(path)
            if task.get("outcome") == "finished":
                continue
            logfile = config.LOG_ROOT / ".sb_tmp" / (path.stem + "-process.log")
            logfile.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Starting observation %s (%d targets)", task["observation"]["sb_name"], len(task["sources"]))
            try:
                rc = _run_child(path, deadline, environment, logfile)
            except OSError as exc:
                logfile.write_text(str(exc))
                rc = 1
            task = read_json(path)
            if task.get("fatal_state_error"):
                stopped.set()
                raise sb_status.StateDatabaseError(task["fatal_state_error"])
            if rc == config.PARTIAL_EXIT_CODE and task.get("outcome") == "partial":
                logfile.unlink(missing_ok=True)
                return
            if rc != 0 or task.get("outcome") != "finished":
                failure = config.LOG_ROOT / "failures" / logfile.name
                failure.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(logfile), failure)
                for source in task["sources"]:
                    if task["results"].get(source["source_name"], {}).get("status") not in {"complete", "failed"}:
                        _record_failure(source, task["observation"], task.get("active_step", "worker_setup"),
                            f"Observation worker exit={rc} without a finished outcome", failure)
                        task["results"][source["source_name"]] = {"status": "failed"}
                task.update(outcome="finished", finished_at=utc_now())
                atomic_write_json(path, task)
            else:
                logfile.unlink(missing_ok=True)
            logger.info("Finished observation %s: %s", task["observation"]["sb_name"], task["results"])

    def guarded_lane(entries):
        try:
            run_lane(entries)
        except Exception:
            stopped.set()
            raise

    try:
        with ThreadPoolExecutor(max_workers=slots) as pool:
            futures = [pool.submit(guarded_lane, entries) for entries in lanes]
            for future in futures:
                future.result()
    except sb_status.StateDatabaseError as exc:
        stopped.set()
        job["fatal_state_error"] = str(exc)
        atomic_write_json(Path(job["manifest_path"]), job)
        raise
    finished = all(read_json(Path(entry["manifest_path"])).get("outcome") == "finished" for entry in job["tasks"])
    job["outcome"] = "finished" if finished else "partial"
    atomic_write_json(Path(job["manifest_path"]), job)
    return 0 if finished else config.PARTIAL_EXIT_CODE


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--deadline", type=float, help=argparse.SUPPRESS)
    args = parser.parse_args()
    config.ensure_directories()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    manifest = read_json(args.manifest)
    manifest["manifest_path"] = str(args.manifest)
    if "tasks" not in manifest and args.deadline is None:
        os.environ.update(slot_environment(1))
    raise SystemExit(run_manifest(manifest, args.deadline))


if __name__ == "__main__":
    main()
