"""Login-node planner/downloader: one submitted job, parallel observation slots.

Range endpoints select seeds, not companions. An active plan freezes one
crosscatalogue revision and is resumed before a new plan is made. Only this
controller downloads; workers never need CASDA access.
"""
from __future__ import annotations

import logging
import math
import os
import re
import shutil
import sqlite3
import subprocess
import tarfile
import time
from pathlib import Path
from urllib.parse import unquote

from astropy.table import Table

try:
    from . import config, casda_query, sb_status, shared_worker
    from .crosscatalogue import (ensure_catalogue, exclusive_lock,
        groups_for_range, digest_file, fingerprint, observation)
    from .pipeline_utils import atomic_write_json, read_json, utc_now, directory_size, write_source_state
except ImportError:
    import config, casda_query, sb_status, shared_worker
    from crosscatalogue import (ensure_catalogue, exclusive_lock,
        groups_for_range, digest_file, fingerprint, observation)
    from pipeline_utils import atomic_write_json, read_json, utc_now, directory_size, write_source_state

logger = logging.getLogger("askap.shared_controller")


def _selected_row(source: dict, key: str) -> Table:
    rows = Table.read(source["query_cache"], format="ascii.ecsv")
    indexes = [i for i, row in enumerate(rows) if observation(row)["key"] == key]
    if len(indexes) != 1:
        raise RuntimeError(f"Expected one frozen TAP row for {source['source_name']} {key}")
    return rows[indexes]


def select_targets(group: dict, retry_existing: bool) -> list[dict]:
    obs = group["observation"]
    targets = []
    for source in group["sources"]:
        if shared_worker.already_complete(source, obs):
            continue
        state = sb_status.observation_status(source["source_name"], obs["sb_id"], obs["beam"])
        stage = state.get("retry_stage") or "none"
        failed = bool(state.get("error_step")) or stage in {"processing_failed", "redownload_required", "exhausted"}
        if retry_existing:
            if stage == "exhausted" or (not failed and stage not in {"processing_retry_ready", "redownload_retry_ready"}):
                continue
        elif failed or stage in {"processing_retry_ready", "redownload_retry_ready"}:
            continue
        if state.get("observation_key") not in (None, obs["key"]):
            raise RuntimeError(f"Changed data product for {source['source_name']} {obs['sb_name']}; preserve old products/state before reprocessing")
        if state.get("ms_name") and unquote(state["ms_name"]) != obs["filename"]:
            raise RuntimeError(f"Changed filename for {source['source_name']} {obs['sb_name']}; old state must be archived explicitly")
        targets.append(source)
    return targets


def _download(manifest: dict) -> None:
    """One SB's full URL list in one download_files call, checksum included."""
    obs = manifest["observation"]
    root = Path(manifest["shared_root"])
    downloaded = root / "download-ready.json"
    extraction = read_json(root / "extracted.json", {})
    archive = Path(manifest["archive_path"])
    if (downloaded.is_file() and archive.is_file()) or (
        extraction and Path(extraction["ms_path"]).is_dir()
    ):
        manifest["prepared_ms_units"] = 1
        for source in manifest["sources"]:
            for step in ("archive_download", "checksum_download"):
                sb_status.record_step(source["source_name"], obs["sb_id"], step, "Y", beam=obs["beam"], seconds=0)
        return
    root.mkdir(parents=True, exist_ok=True)
    needed = obs["estimated_bytes"]
    if directory_size(config.STAGING_ROOT) + needed > config.STAGING_BUDGET_BYTES or shutil.disk_usage(config.WORK).free < needed:
        raise RuntimeError("Shared archive would exceed staging budget or available space")
    directory = archive.parent
    directory.mkdir(exist_ok=True)
    rows = Table.read(manifest["query_cache"], format="ascii.ecsv")
    client = casda_query.build_casda_client()
    for attempt in range(1, config.CASDA_SOURCE_DOWNLOAD_RETRIES + 1):
        urls = []
        try:
            urls = list(client.stage_data(rows))
            pairs = {}
            for name, pair in casda_query._pair_urls(urls).items():
                pairs.setdefault(unquote(name), {}).update(pair)
            matches = [pair for name, pair in pairs.items() if name == obs["filename"]]
            if len(matches) != 1 or "data" not in matches[0] or "checksum" not in matches[0]:
                raise RuntimeError("CASDA did not return the expected archive/checksum pair")
            # Retry an incomplete download from scratch, using freshly staged URLs.
            for path in directory.iterdir():
                if path.is_file():
                    path.unlink()
            started = time.monotonic()
            client.download_files(urls, savedir=str(directory))
            # Older astroquery may use URL-encoded filenames on disk.
            for path in list(directory.iterdir()):
                if path.is_file() and unquote(path.name) != path.name:
                    decoded = directory / unquote(path.name)
                    if decoded.name != unquote(path.name):
                        raise RuntimeError("Unsafe downloaded basename")
                    path.rename(decoded)
            if not archive.is_file() or not Path(str(archive) + ".checksum").is_file():
                raise RuntimeError("download_files returned without the archive and checksum filenames")
            # Time requests reflect staged MS count, never a CASDA-only guess.
            with tarfile.open(archive) as handle:
                units = {part for member in handle for part in Path(member.name).parts if part.endswith(".ms")}
            if len(units) != 1:
                raise RuntimeError(f"Shared product must contain exactly one MS; found {len(units)}")
            manifest["prepared_ms_units"] = len(units)
            atomic_write_json(downloaded, {"observation_key": obs["key"], "time": utc_now(), "attempt": attempt})
            for source in manifest["sources"]:
                for step in ("archive_download", "checksum_download"):
                    sb_status.record_step(source["source_name"], obs["sb_id"], step, "Y", beam=obs["beam"], seconds=time.monotonic()-started)
            return
        except sb_status.StateDatabaseError:
            raise
        except Exception as exc:
            message = casda_query._redact_download_error(exc, urls)
            logger.warning("Shared download attempt %d/%d: %s", attempt, config.CASDA_SOURCE_DOWNLOAD_RETRIES, message)
            if attempt == config.CASDA_SOURCE_DOWNLOAD_RETRIES:
                raise RuntimeError(message) from None
            time.sleep(config.CASDA_SOURCE_RETRY_DELAY_SECONDS)


def make_manifest(group: dict, targets: list[dict], settings: dict, retry: bool) -> dict:
    obs = group["observation"]
    model_key = fingerprint({"observation_key": obs["key"], "model_settings": settings})
    stamp = str(time.time_ns())
    tag = f"shared-{obs['sb_name']}-{stamp}"
    root = config.STAGING_ROOT / "_shared" / model_key
    public_root = root
    # Retry copies are private: a failed source cannot invalidate others' master.
    if retry:
        source = targets[0]
        stage = sb_status.prepare_observation_retry(source["source_name"], obs["sb_id"], obs["beam"])
        root = root / "retries" / (source["source_name"] + "-" + stamp)
    else:
        stage = "none"
    manifest_path = config.STATE_ROOT / "jobs" / (tag + ".json")
    query_cache = manifest_path.with_suffix(".ecsv")
    _selected_row(targets[0], obs["key"]).write(query_cache, format="ascii.ecsv", overwrite=False)
    compact_fields = ("source_name", "ucs_number", "ra", "dec", "pmra", "pmdec", "batch")
    compact = lambda source: {key: source[key] for key in compact_fields}
    manifest = {
        "schema": 1, "job_tag": tag, "manifest_path": str(manifest_path), "mode": "private_retry" if retry else "shared",
        "observation": obs, "model_key": model_key, "model_settings": settings,
        "sources": [compact(s) for s in targets], "all_sources": [compact(s) for s in group["sources"]],
        "crosscatalogue_revision": group["crosscatalogue_revision"],
        "shared_root": str(root), "query_cache": str(query_cache),
        "public_root": str(public_root),
        "archive_path": str(root / "download" / obs["filename"]),
        "job_walltime_hours": min(config.MAX_JOB_WALLTIME_HOURS,
            config.HOURS_PER_OBS + config.SOURCE_OVERHEAD_HOURS + len(targets) * config.SHARED_TARGET_HOURS),
        "retry_stage": stage, "results": {}, "created_at": utc_now(), "status": "planned",
    }
    atomic_write_json(manifest_path, manifest)
    return manifest


def prepare(manifest: dict) -> None:
    obs = manifest["observation"]
    source = manifest["sources"][0]
    private = config.STAGING_ROOT / source["source_name"] / "LongObs" / obs["sb_name"]
    need_download = manifest["mode"] == "shared" or not shared_worker.pipeline._find_ms(private)
    if manifest["mode"] == "private_retry" and manifest["retry_stage"] != "redownload_required" and need_download:
        retained = read_json(Path(manifest["public_root"]) / "extracted.json", {})
        if retained and Path(retained["ms_path"]).is_dir():
            manifest["fallback_raw_ms"] = retained["ms_path"]
            need_download = False
    if manifest["retry_stage"] == "redownload_required":
        # Delete this target's old input/checksum only; shared inputs stay intact.
        shared_worker.pipeline._discard_sb_for_redownload(source["source_name"], private,
            shared_worker.product_dir(source, obs), config.WSCLEAN_TEMP_ROOT / f"{source['source_name']}_{obs['sb_name']}")
        need_download = True
        manifest.pop("fallback_raw_ms", None)
    if need_download:
        _download(manifest)
    else:
        manifest["prepared_ms_units"] = 1  # Existing exact-identity MS, not a TAP estimate.
    if manifest["retry_stage"] == "redownload_required":
        with sb_status._connect() as connection:
            connection.execute("UPDATE sb_processing SET retry_stage='redownload_retry_ready', "
                "redownload_retry_count=redownload_retry_count+1 WHERE source_name=? AND sb_id=? AND beam=? "
                "AND retry_stage='redownload_required'", (source["source_name"], obs["sb_id"], obs["beam"]))
        manifest["retry_stage"] = "redownload_retry_ready"
    manifest["job_walltime_hours"] = min(config.MAX_JOB_WALLTIME_HOURS,
        manifest.get("prepared_ms_units", 1) * config.HOURS_PER_OBS + config.SOURCE_OVERHEAD_HOURS
        + len(manifest["sources"]) * config.SHARED_TARGET_HOURS)
    manifest["status"] = "prepared"
    atomic_write_json(Path(manifest["manifest_path"]), manifest)


def task_hours(manifest: dict) -> float:
    """Estimate unfinished work only; a retained ready master needs no imaging."""
    pending = sum(result.get("status") not in {"complete", "failed"}
                  for result in (manifest.get("results", {}).get(s["source_name"], {})
                                 for s in manifest["sources"]))
    if not pending:
        return 0.0
    ready = read_json(Path(manifest["shared_root"]) / "model-ready.json", {})
    master = Path(ready.get("master", "/nonexistent"))
    reusable = (manifest["mode"] == "shared" and ready.get("model_key") == manifest["model_key"]
                and shared_worker.pipeline._find_ms(master) is not None
                and shared_worker._model_files_exist(master))
    return (0 if reusable else manifest.get("prepared_ms_units", 1) * config.HOURS_PER_OBS) + pending * config.SHARED_TARGET_HOURS


def job_tasks(job: dict):
    """Old single-observation manifests remain resumable after this update."""
    if "tasks" not in job:
        return [job]
    return [read_json(Path(task["manifest_path"])) for task in job["tasks"]]


def schedule_job(job: dict) -> None:
    """Greedy lane assignment; walltime is the busiest lane plus one reserve."""
    if "tasks" not in job:
        job["job_walltime_hours"] = min(config.MAX_JOB_WALLTIME_HOURS,
            task_hours(job) + config.SOURCE_OVERHEAD_HOURS)
        return
    tasks = job_tasks(job)
    job["parallel_slots"] = max(1, min(job["parallel_slots"], sum(task_hours(task) > 0 for task in tasks)))
    loads = [0.0] * job["parallel_slots"]
    for entry, task in zip(job["tasks"], tasks):
        lane = min(range(len(loads)), key=loads.__getitem__)
        entry["lane"] = lane
        entry["estimated_work_hours"] = task_hours(task)
        loads[lane] += entry["estimated_work_hours"]
    job["slot_load_hours"] = loads
    job["job_walltime_hours"] = min(config.MAX_JOB_WALLTIME_HOURS,
        max(loads, default=0) + config.SOURCE_OVERHEAD_HOURS)


def plan_job(controller: dict, args) -> dict:
    """Pack distinct products; never run two chunks of one model concurrently."""
    tasks = [read_json(Path(path)) for path in controller.get("planning_tasks", [])]
    loads = [0.0] * config.PARALLEL_SLOTS
    for task in tasks:
        lane = min(range(len(loads)), key=loads.__getitem__)
        loads[lane] += task_hours(task)
    start = tasks[-1]["group_index"] + 1 if tasks else controller["group_index"]
    for index in range(start, len(controller["groups"])):
        if len(tasks) >= config.MAX_MS_PER_JOB:
            break
        stored = controller["groups"][index]
        group = {**stored, "sources": [controller["source_index"][name] for name in stored["source_names"]]}
        targets = [s for s in select_targets(group, args.retry_existing)
                   if s["source_name"] not in stored.get("attempted_without_progress", [])]
        if not targets:
            continue
        lane = min(range(len(loads)), key=loads.__getitem__)
        available = config.MAX_JOB_WALLTIME_HOURS - config.SOURCE_OVERHEAD_HOURS - loads[lane]
        capacity = int((available - config.HOURS_PER_OBS) / config.SHARED_TARGET_HOURS)
        if capacity < 1 and tasks and loads[lane] > 0:
            break
        targets = targets[:1 if args.retry_existing else max(1, capacity)]
        for source in targets:
            sb_status.initialize_source_rows(source, _selected_row(source, group["observation"]["key"]))
        task = make_manifest(group, targets, controller["model_settings"], args.retry_existing)
        task["group_index"] = index
        task["worker_environment"] = controller["worker_environment"]
        atomic_write_json(Path(task["manifest_path"]), task)
        loads[lane] += task_hours(task)
        tasks.append(task)
        # Persist each reservation: an interrupted multi-observation plan must
        # not advance private retry counters again when the controller resumes.
        controller["planning_tasks"] = [task["manifest_path"] for task in tasks]
        atomic_write_json(config.STATE_ROOT / "shared-controller.json", controller)
    if len(tasks) == 1:
        return tasks[0]
    tag = "shared-batch-" + str(time.time_ns())
    job = {"schema": 2, "job_tag": tag,
        "manifest_path": str(config.STATE_ROOT / "jobs" / (tag + ".json")),
        "tasks": [{"manifest_path": task["manifest_path"], "group_index": task["group_index"]} for task in tasks],
        "parallel_slots": min(config.PARALLEL_SLOTS, len(tasks)),
        "worker_environment": controller["worker_environment"], "status": "planned",
        "created_at": utc_now()}
    schedule_job(job)
    atomic_write_json(Path(job["manifest_path"]), job)
    return job


def prepare_job(job: dict) -> None:
    for task in job_tasks(job):
        if task["status"] != "planned":
            continue
        try:
            prepare(task)
        except (sb_status.StateDatabaseError, sqlite3.Error):
            raise
        except Exception as exc:
            for source in task["sources"]:
                shared_worker._record_failure(source, task["observation"], "archive_download", exc)
                task["results"][source["source_name"]] = {"status": "failed", "error": str(exc)}
            task.update(status="complete", outcome="finished", finished_at=utc_now())
            atomic_write_json(Path(task["manifest_path"]), task)
        if "tasks" not in job:
            job.update(task)
    if "tasks" in job:
        job["status"] = "complete" if all(t.get("outcome") == "finished" for t in job_tasks(job)) else "prepared"
    if job["status"] != "complete":
        schedule_job(job)
    atomic_write_json(Path(job["manifest_path"]), job)


def _fail_worker(manifest: dict, message: str, logfile: Path | None = None) -> dict:
    """Record failures, not successes, for an abnormal/incomplete worker exit."""
    if manifest.get("fatal_state_error"):
        raise sb_status.StateDatabaseError(manifest["fatal_state_error"])
    if manifest.get("outcome") == "finished":
        return manifest
    if logfile is None:
        job = read_json(Path(manifest["manifest_path"] + ".submission.json"), {})
        logfile = config.LOG_ROOT / f"slurm-{job.get('job_id', 'unknown')}.err"
    if "tasks" in manifest:
        for task in job_tasks(manifest):
            _fail_worker(task, message, logfile)
        manifest.update(outcome="finished", finished_at=utc_now())
        atomic_write_json(Path(manifest["manifest_path"]), manifest)
        return manifest
    obs = manifest["observation"]
    for source in manifest["sources"]:
        if manifest["results"].get(source["source_name"], {}).get("status") in {"complete", "failed"}:
            continue
        shared_worker._record_failure(source, obs, "worker_setup", message, logfile)
        manifest["results"][source["source_name"]] = {"status": "failed", "error": message}
    manifest.update(outcome="finished", finished_at=utc_now())
    atomic_write_json(Path(manifest["manifest_path"]), manifest)
    return manifest


def _terminal_scheduler_state(job_id: str) -> str | None:
    # An empty queue is NOT proof of success. Only explicit accounting terminal
    # states can close a job whose shell was killed before writing its markers.
    try:
        result = subprocess.run(["sacct", "-j", job_id, "-n", "-P", "-o", "JobIDRaw,State"],
            check=True, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    terminals = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "PREEMPTED", "DEADLINE", "REVOKED"}
    for line in result.stdout.splitlines():
        fields = line.split("|")
        if len(fields) >= 2 and fields[0] == job_id:
            state = fields[1].split()[0].rstrip("+")
            return state if state in terminals else None
    return None


def wait_for_job(manifest_path: Path, poll_interval: float) -> dict:
    terminal_seen = None
    while True:
        manifest = read_json(manifest_path)
        failure = Path(str(manifest_path) + ".worker_failure")
        finished = Path(str(manifest_path) + ".worker_finished")
        submission = read_json(Path(str(manifest_path) + ".submission.json"), {})
        if manifest.get("fatal_state_error"):
            raise sb_status.StateDatabaseError(manifest["fatal_state_error"])
        if failure.exists():
            return _fail_worker(manifest, f"Worker exit={failure.read_text().strip()}; see {submission}")
        if finished.exists() and manifest.get("outcome") in {"partial", "finished"}:
            return manifest
        terminal = _terminal_scheduler_state(submission["job_id"]) if submission.get("job_id") else None
        if terminal and terminal_seen == terminal:
            return _fail_worker(manifest, f"Slurm {terminal} without a terminal worker outcome")
        terminal_seen = terminal
        logger.info("Waiting for shared job %s (Slurm %s)", manifest["job_tag"], submission.get("job_id", "pending"))
        time.sleep(max(1.0, poll_interval))


def _source_summary(group: dict) -> None:
    """Completion applies to ALL selected observations, never just a companion SB."""
    for source in group["sources"]:
        states = []
        for obs in source["observations"]:
            row = sb_status.observation_status(source["source_name"], obs["sb_id"], obs["beam"])
            if shared_worker.already_complete(source, obs):
                states.append("complete")
            elif row.get("error_step") or row.get("retry_stage") in {"processing_failed", "redownload_required", "exhausted"}:
                states.append("failed")
            else:
                states.append("pending")
        status = "pending" if "pending" in states else ("complete_with_failures" if "failed" in states else "complete")
        write_source_state(source["source_name"], status=status, shared_processing=True,
            expected_sb=sorted({int(o["sb_id"][2:]) for o in source["observations"]}),
            expected_sb_names=[o["sb_name"] for o in source["observations"]],
            crosscatalogue_revision=group["crosscatalogue_revision"])


def run(args) -> dict:
    try:
        from . import ozstar_main as legacy
    except ImportError:
        import ozstar_main as legacy
    if os.environ.get("SLURM_JOB_ID"):
        raise RuntimeError("Run the crosscatalogue/controller on a login node, not a compute node")
    config.ensure_directories()
    if not 1 <= config.CREATE_MODEL_THREADS <= config.CPU_PER_SLOT:
        raise ValueError("Shared worker requires 1 <= CREATE_MODEL_THREADS <= CPU_PER_SLOT")
    if config.SHARED_TARGET_HOURS <= 0:
        raise ValueError("ASKAP_SHARED_TARGET_HOURS must be positive")
    if not math.isfinite(config.MAX_JOB_WALLTIME_HOURS) or not 0 < config.MAX_JOB_WALLTIME_HOURS <= 48:
        raise ValueError("Shared jobs require 0 < ASKAP_MAX_JOB_WALLTIME_HOURS <= 48")
    if not math.isfinite(config.HOURS_PER_OBS) or config.HOURS_PER_OBS <= 0 or not math.isfinite(config.SHARED_TARGET_HOURS):
        raise ValueError("Processing time estimates must be finite and positive")
    if not math.isfinite(config.SOURCE_OVERHEAD_HOURS) or config.SOURCE_OVERHEAD_HOURS < 0:
        raise ValueError("ASKAP_SOURCE_OVERHEAD_HOURS must be finite and nonnegative")
    if config.WALLTIME_RESERVE_MINUTES < 0:
        raise ValueError("ASKAP_WALLTIME_RESERVE_MINUTES must not be negative")
    if config.MAX_JOB_WALLTIME_HOURS * 60 <= config.WALLTIME_RESERVE_MINUTES + 1:
        raise ValueError("Walltime must exceed the reserved minutes plus one minute")
    with exclusive_lock(config.STATE_ROOT / "shared-controller.lock"):
        return _run_locked(args, legacy)


def _run_locked(args, legacy) -> dict:
    controller_path = config.STATE_ROOT / "shared-controller.json"
    controller = read_json(controller_path, {})
    active = controller and controller.get("status") != "complete"
    observation_key = getattr(args, "shared_observation_key", None)
    if observation_key and not re.fullmatch(r"[0-9a-f]{64}", observation_key):
        raise ValueError("--shared-observation-key must be the 64-character key from crosscatalogue_observations.csv")
    if active and (controller["begin"], controller["end"], controller["retry_existing"]) != (args.begin, args.end, args.retry_existing):
        raise RuntimeError("An unfinished shared plan exists; resume its original range/retry mode before starting another")
    if active and controller.get("observation_key") != observation_key:
        raise RuntimeError("Resume using the same --shared-observation-key as the active plan")
    if active and (args.update_crosscatalogue or args.refresh_cache or args.crosscatalogue_only
                   or getattr(args, "retry_crosscatalogue_failures", False)):
        raise RuntimeError("Finish the frozen active plan before updating the crosscatalogue")
    if active and int(controller["worker_environment"]["ASKAP_PARALLEL_SLOTS"]) != config.PARALLEL_SLOTS:
        raise RuntimeError("Resume with the original --parallel-slots; change concurrency only after this plan completes")
    if not active:
        snapshot = ensure_catalogue(legacy._load_catalogue(), legacy._source_from_row,
                                    update=args.update_crosscatalogue or args.refresh_cache,
                                    retry_failed=getattr(args, "retry_crosscatalogue_failures", False))
        if args.crosscatalogue_only:
            return snapshot
        # Do not mix a shared run with an unfinished legacy worker.
        for state_path in (config.STATE_ROOT / "sources").glob("*.json"):
            state = read_json(state_path, {})
            if state.get("status") in legacy.ACTIVE_STATUSES and not state.get("shared_processing"):
                raise RuntimeError(f"Unfinished legacy source state: {state_path}; finish the old run before shared processing")
        groups = groups_for_range(snapshot, args.begin, args.end)
        if observation_key:
            groups = [group for group in groups if group["observation"]["key"] == observation_key]
            if not groups:
                raise ValueError("Requested product is not selected by any seed source in this range")
        settings = shared_worker.model_settings(digest_file(config.CONTAINER))
        # Store each source's full observation list once, not once per group.
        source_index = {s["source_name"]: s for group in groups for s in group["sources"]}
        groups = [{**{k:v for k,v in group.items() if k != "sources"},
                   "source_names": [s["source_name"] for s in group["sources"]]} for group in groups]
        controller = {"begin": args.begin, "end": args.end, "retry_existing": args.retry_existing,
            "crosscatalogue_revision": snapshot["revision"], "groups": groups, "group_index": 0,
            "source_index": source_index,
            "model_settings": settings, "status": "running", "jobs": [], "active_manifest": None,
            "worker_environment": legacy._worker_runtime_environment(), "observation_key": observation_key,
            "created_at": utc_now()}
        atomic_write_json(controller_path, controller)
    while controller["group_index"] < len(controller["groups"]):
        stored_group = controller["groups"][controller["group_index"]]
        group = {**stored_group, "sources": [controller["source_index"][name] for name in stored_group["source_names"]]}
        # A frozen manifest is resumed first; never advance retry counters again.
        if controller.get("active_manifest"):
            manifest = read_json(Path(controller["active_manifest"]))
        else:
            if not controller.get("planning_tasks"):
                targets = [s for s in select_targets(group, args.retry_existing)
                           if s["source_name"] not in group.get("attempted_without_progress", [])]
                if not targets:
                    controller["group_index"] += 1
                    atomic_write_json(controller_path, controller)
                    continue
            manifest = plan_job(controller, args)
            atomic_write_json(Path(manifest["manifest_path"]), manifest)
            controller["active_manifest"] = manifest["manifest_path"]
            controller["jobs"].append(manifest["manifest_path"])
            controller.pop("planning_tasks", None)
            atomic_write_json(controller_path, controller)
        path = Path(manifest["manifest_path"])
        if manifest["status"] == "planned" and (args.submit or args.prepare_download):
            prepare_job(manifest)
        if manifest["status"] in {"planned", "prepared"}:
            slots = manifest.get("parallel_slots", 1)
            runtime = dict(manifest["worker_environment"], ASKAP_PARALLEL_SLOTS=str(slots))
            script = legacy._write_sbatch(path, manifest["job_tag"], "ASKAP-shared",
                max_resubmits=args.max_resubmits, worker_script="shared_worker.py",
                walltime_hours=manifest["job_walltime_hours"], cpus=slots * int(runtime["ASKAP_CPU_PER_SLOT"]),
                runtime_environment_override=runtime)
            manifest["script_path"] = str(script)
            atomic_write_json(path, manifest)
            logger.info("Shared plan: %d observation(s), %d slot(s), %.2f h, %d CPUs; manifest=%s",
                len(manifest.get("tasks", [manifest])), slots, manifest["job_walltime_hours"], slots * int(runtime["ASKAP_CPU_PER_SLOT"]), path)
            if not args.submit:
                return controller
            manifest["status"] = "submitting"
            manifest.pop("outcome", None)
            manifest.pop("finished_at", None)
            for suffix in (".worker_failure", ".worker_finished", ".submission.json"):
                Path(str(path) + suffix).unlink(missing_ok=True)
            atomic_write_json(path, manifest)
            # Failure/interrupt here is deliberately ambiguous: never submit twice.
            job_id = legacy._submit(script)
            # From sbatch onward the worker owns the manifest. Keep submission
            # metadata separate so a fast worker cannot have its results erased.
            atomic_write_json(Path(str(path) + ".submission.json"), {"job_id": job_id, "submitted_at": utc_now()})
        if manifest["status"] == "submitting":
            if not Path(str(path) + ".submission.json").is_file():
                raise RuntimeError(f"Submission outcome is unknown: {path}; inspect the queue before changing this state")
        if manifest["status"] in {"submitting", "submitted"}:
            if not args.submit:
                logger.info("Existing submitted job retained: %s", path)
                return controller
            manifest = wait_for_job(path, args.controller_poll_interval)
            if manifest.get("outcome") == "partial":
                continuation = manifest.get("continuation_count", 0) + 1
                if continuation <= args.max_resubmits:
                    manifest.update(status="prepared", continuation_count=continuation)
                    schedule_job(manifest)
                    atomic_write_json(path, manifest)
                    continue
                manifest = _fail_worker(manifest, "Partial worker reached continuation limit")
            manifest["status"] = "complete"
            atomic_write_json(path, manifest)
        if manifest["status"] == "complete":
            summary_sources = {}
            for task in job_tasks(manifest):
                index = task.get("group_index", controller["group_index"])
                stored = controller["groups"][index]
                completed_group = {**stored, "sources": [controller["source_index"][name] for name in stored["source_names"]]}
                summary_sources.update({s["source_name"]: s for s in completed_group["sources"]})
                if args.retry_existing:
                    for source in task["sources"]:
                        obs = task["observation"]
                        state = sb_status.observation_status(source["source_name"], obs["sb_id"], obs["beam"])
                        if state.get("retry_stage") == "processing_failed":
                            stored.setdefault("attempted_without_progress", []).append(source["source_name"])
            _source_summary({"sources": list(summary_sources.values()),
                "crosscatalogue_revision": controller["crosscatalogue_revision"]})
            controller["active_manifest"] = None
            # The next loop skips completed/failed groups or plans remaining
            # companions and the one explicit final re-download retry.
            atomic_write_json(controller_path, controller)
    _source_summary({"sources": list(controller["source_index"].values()),
        "crosscatalogue_revision": controller["crosscatalogue_revision"]})
    controller.update(status="complete", finished_at=utc_now())
    atomic_write_json(controller_path, controller)
    logger.info("Shared plan completed for seeds UCS%d–UCS%d", args.begin, args.end)
    return controller
