#!/usr/bin/env python3
"""Gate 6: validate the complete bounded failed-SB retry state machine.

This login-node driver creates a new, isolated ASKAP_WORK containing exactly
one real CASDA observation.  It submits three compute attempts through the
normal controller:

1. a controlled create-model failure records ``processing_failed``;
2. a normal rerun proves failures are not retried implicitly;
3. an explicit retry reuses/reset-preprocesses the MS, fails once more, forces
   a fresh CASDA download, then completes without the controlled fault.

The controlled failure is restricted to this Gate 6 validation tree by
independent path, source, SB, step, and retry-stage checks in ``process_job``.
Production products, staging, controllers, and SQLite state are not reused.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_VALIDATION_ROOT = Path(
    "/fred/oz299/qhuang/dstools/gate6-validation"
)
DEFAULT_PRODUCTION_ROOT = Path("/fred/oz299/qhuang/ASKAP-UCDs")
DEFAULT_CONTAINER = Path("/fred/oz299/qhuang/dstools/dstools-260925.sif")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ucs", type=int, required=True, help="Catalogue UCS number.")
    parser.add_argument(
        "--obs-id",
        type=int,
        required=True,
        help="Exactly one numeric ASKAP SB/observation identifier.",
    )
    parser.add_argument(
        "--work",
        type=Path,
        help="New empty isolated work directory (default: gate6-validation timestamp).",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        help="Read-only production TAP ECSV; defaults to state/tap_cache/UCS<ucs>.ecsv.",
    )
    parser.add_argument(
        "--catalogue",
        type=Path,
        default=DEFAULT_PRODUCTION_ROOT
        / "catalogue/UltracoolSheet_Main_index_unbinaryUCD.csv",
    )
    parser.add_argument(
        "--time-csv",
        type=Path,
        default=DEFAULT_PRODUCTION_ROOT
        / "catalogue/All_combine_data_unrepetition.csv",
    )
    parser.add_argument("--container", type=Path, default=DEFAULT_CONTAINER)
    parser.add_argument(
        "--initial-delay",
        type=float,
        default=10.0,
        help="Seconds before the first worker-state poll.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=30.0,
        help="Seconds between filesystem-state polls.",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Required explicit authorization to download and submit Gate 6 jobs.",
    )
    return parser.parse_args()


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _default_work(ucs: int, obs_id: int) -> Path:
    return DEFAULT_VALIDATION_ROOT / f"gate6-UCS{ucs}-SB{obs_id}-{_timestamp()}"


def _assert_isolated_work(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    lowered = str(resolved).lower()
    if "gate6" not in lowered or "validation" not in lowered:
        raise ValueError(
            "Gate 6 work path must contain both 'gate6' and 'validation': "
            f"{resolved}"
        )
    production = DEFAULT_PRODUCTION_ROOT.resolve()
    try:
        resolved.relative_to(production)
    except ValueError:
        pass
    else:
        raise ValueError(
            f"Gate 6 work must not be inside the production ASKAP tree: {resolved}"
        )
    if resolved.exists() and any(resolved.iterdir()):
        raise FileExistsError(
            f"Gate 6 work directory is not empty; choose a new path: {resolved}"
        )
    return resolved


def _require_file(path: Path, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} does not exist: {resolved}")
    return resolved


def _verify_container(container: Path) -> None:
    sidecar = Path(f"{container}.sha256")
    if not sidecar.is_file():
        raise FileNotFoundError(f"Container checksum sidecar is missing: {sidecar}")
    fields = sidecar.read_text(encoding="utf-8").strip().split()
    if len(fields) < 2 or Path(fields[-1]).expanduser().resolve() != container:
        raise ValueError(
            f"Checksum sidecar does not name the selected container: {sidecar}"
        )
    subprocess.run(["sha256sum", "-c", str(sidecar)], check=True)


def _configure_environment(
    work: Path,
    catalogue: Path,
    time_csv: Path,
    container: Path,
) -> Path:
    fault_config = work / "state/gate6-fault-config.json"
    settings = {
        "ASKAP_WORK": str(work),
        "ASKAP_CATALOGUE": str(catalogue),
        "ASKAP_TIME_CSV": str(time_csv),
        "DSTOOLS_CONTAINER": str(container),
        "ASKAP_GATE6_FAULT_CONFIG": str(fault_config),
        # Gate 6 uses one eight-thread DStools process in a two-hour worker.
        "ASKAP_JOB_WALLTIME_HOURS": "2.0",
        "ASKAP_MAX_JOB_WALLTIME_HOURS": "2.0",
        "ASKAP_HOURS_PER_OBS": "0.5",
        # 0.5 h for one observation plus the normal fixed overhead is capped
        # to the requested two hours.  Keeping the overhead non-zero also
        # gives process_job's walltime guard enough room to start the wave.
        "ASKAP_SOURCE_OVERHEAD_HOURS": "2.0",
        "ASKAP_PARALLEL_SLOTS": "1",
        "ASKAP_CPU_PER_SLOT": "8",
        "ASKAP_CREATE_MODEL_THREADS": "8",
        "ASKAP_MAX_MS_PER_JOB": "1",
        "ASKAP_SBATCH_MEM": "32G",
        "ASKAP_SBATCH_TMP": "10G",
        "ASKAP_MAX_AUTO_RESUBMITS": "0",
    }
    os.environ.update(settings)
    return fault_config


def _configure_logging(work: Path) -> Path:
    log_path = work / "logs/gate6-driver.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [
        logging.StreamHandler(),
        logging.FileHandler(log_path, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
    return log_path


def _text(value: Any) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else str(value)


def _copy_one_cache_row(
    source_name: str,
    obs_id: int,
    source_cache: Path,
    isolated_cache: Path,
) -> tuple[str, str]:
    from astropy.table import Table

    from pipeline_utils import obs_number

    table = Table.read(source_cache, format="ascii.ecsv")
    if "obs_id" not in table.colnames or "filename" not in table.colnames:
        raise ValueError(f"TAP cache lacks obs_id/filename columns: {source_cache}")
    indexes = [
        index
        for index, value in enumerate(table["obs_id"])
        if obs_number(value) == obs_id
    ]
    if len(indexes) != 1:
        raise ValueError(
            f"Expected exactly one SB{obs_id} row in {source_cache}; found {len(indexes)}"
        )
    selected = Table(table[indexes], copy=True)
    if "access_estsize" not in selected.colnames:
        raise ValueError(f"TAP cache lacks access_estsize: {source_cache}")

    isolated_cache.parent.mkdir(parents=True, exist_ok=True)
    temporary = isolated_cache.with_name(f".{isolated_cache.name}.tmp")
    selected.write(temporary, format="ascii.ecsv", overwrite=True)
    os.replace(temporary, isolated_cache)

    filename = _text(selected["filename"][0])
    match = re.search(r"(SB\d+).*?(beam\d+)", filename, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"Cannot derive SB/beam from cached filename: {filename}")
    sb_id = match.group(1).upper()
    beam = match.group(2).lower()
    if sb_id != f"SB{obs_id}":
        raise ValueError(f"Cached filename SB mismatch: expected SB{obs_id}, found {sb_id}")
    logging.getLogger("askap.gate6").info(
        "%s: copied one read-only TAP cache row for %s_%s",
        source_name,
        sb_id,
        beam,
    )
    return sb_id, beam


def _read_sb_row(database: Path, source_name: str, sb_id: str, beam: str) -> dict[str, Any]:
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM sb_processing WHERE source_name=? AND sb_id=? AND beam=?",
            (source_name, sb_id, beam),
        ).fetchone()
    if row is None:
        raise AssertionError(f"Missing SQLite row for {source_name}_{sb_id}_{beam}")
    return dict(row)


def _job_ids(controller: dict[str, Any]) -> list[str]:
    return [
        str(block["job_id"])
        for block in controller.get("blocks", [])
        if block.get("job_id")
    ]


def _download_count(download_state: dict[str, Any], sb_id: str) -> int:
    entry = download_state.get("sbs", {}).get(sb_id, {})
    return sum(
        event.get("status") == "downloaded"
        for event in entry.get("history", [])
    )


def _run(args: argparse.Namespace, work: Path, fault_config: Path) -> dict[str, Any]:
    # These imports must occur after ASKAP_WORK and every Gate 6 resource
    # override is frozen in the process environment.
    import config
    from casda_query import cache_path
    from ozstar_main import _load_catalogue, _source_from_row, run_controller
    from pipeline_utils import (
        atomic_write_json,
        download_state_path,
        read_json,
        source_product_root,
    )

    logger = logging.getLogger("askap.gate6")
    config.ensure_directories()
    if config.WORK.resolve() != work:
        raise AssertionError(f"Imported config uses the wrong ASKAP_WORK: {config.WORK}")
    if config.SBATCH_MEM != "32G" or config.SBATCH_TMP != "10G":
        raise AssertionError(
            f"Unexpected Gate 6 Slurm storage: mem={config.SBATCH_MEM}, tmp={config.SBATCH_TMP}"
        )
    if config.PARALLEL_SLOTS != 1 or config.CREATE_MODEL_THREADS != 8:
        raise AssertionError("Gate 6 must use one slot and eight create-model threads")

    catalogue = _load_catalogue()
    selected = catalogue[catalogue["UCS"] == args.ucs]
    if len(selected) != 1:
        raise ValueError(
            f"Expected exactly one catalogue row for UCS{args.ucs}; found {len(selected)}"
        )
    source = _source_from_row(selected.iloc[0])
    source_name = str(source["source_name"])
    if source_name != f"UCS{args.ucs}":
        raise ValueError(
            f"Gate 6 requires UCS_name UCS{args.ucs}, catalogue contains {source_name}"
        )

    production_cache = (
        args.cache.expanduser().resolve()
        if args.cache
        else DEFAULT_PRODUCTION_ROOT.resolve()
        / f"state/tap_cache/{source_name}.ecsv"
    )
    _require_file(production_cache, "Production TAP cache")
    sb_id, beam = _copy_one_cache_row(
        source_name,
        args.obs_id,
        production_cache,
        cache_path(source_name),
    )
    sb_name = f"{sb_id}_{beam}"

    atomic_write_json(
        fault_config,
        {
            "gate": "gate6",
            "work": str(work),
            "source_name": source_name,
            "sb_id": sb_id,
            "fail_step": "create_model",
            "fail_retry_stages": ["none", "processing_retry_ready"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    logger.info(
        "Gate 6 target: %s_%s; Slurm 2h, 8 CPUs, 32G RAM, 10G tmp",
        source_name,
        sb_id,
    )

    controller_args = dict(
        start=args.ucs,
        end=args.ucs,
        submit=True,
        prepare_requested=True,
        refresh_cache=False,
        allow_compute_download=False,
        max_resubmits=0,
        initial_delay=max(0.0, args.initial_delay),
        poll_interval=max(1.0, args.poll_interval),
    )

    logger.info("Gate 6 phase 1/3: controlled initial processing failure")
    first = run_controller(retry_existing=False, **controller_args)
    database = config.STATE_ROOT / "sb_processing.sqlite"
    initial_row = _read_sb_row(database, source_name, sb_id, beam)
    assert first.get("status") == "complete", first
    assert initial_row["retry_stage"] == "processing_failed", initial_row
    assert initial_row["error_step"] == "create_model", initial_row
    assert int(initial_row["processing_retry_count"]) == 0, initial_row
    assert int(initial_row["redownload_retry_count"]) == 0, initial_row

    staged_sb = config.STAGING_ROOT / source_name / "LongObs" / sb_name
    ms_paths = sorted(staged_sb.glob("*.ms"))
    assert len(ms_paths) == 1, f"Initial failure did not retain one MS: {ms_paths}"
    assert (ms_paths[0] / "FIELD_OLD").is_dir(), (
        "Controlled failure must occur after preprocess and retain FIELD_OLD"
    )
    product_sb = source_product_root(args.ucs, source_name) / sb_name
    assert not any(product_sb.glob("*.ds")), "Initial failure unexpectedly produced a DS"
    first_job_ids = _job_ids(first)
    assert len(first_job_ids) == 1, first
    first_block = first["blocks"][0]
    first_manifest = read_json(Path(first_block["manifest_path"]), {}) or {}
    assert float(first_manifest.get("job_walltime_hours", 0)) == 2.0, first_manifest
    sbatch_text = Path(first_block["script_path"]).read_text(encoding="utf-8")
    for required_line in (
        "#SBATCH --time=2:00:00",
        "#SBATCH --cpus-per-task=8",
        "#SBATCH --mem=32G",
        "#SBATCH --tmp=10G",
        f"export ASKAP_GATE6_FAULT_CONFIG={fault_config}",
    ):
        assert required_line in sbatch_text, (
            f"Gate 6 sbatch is missing {required_line!r}: {first_block['script_path']}"
        )

    logger.info("Gate 6 phase 2/3: normal rerun must not retry the failed SB")
    history_root = config.STATE_ROOT / "controllers/history"
    history_before = sorted(history_root.glob("*.json")) if history_root.exists() else []
    normal = run_controller(retry_existing=False, **controller_args)
    normal_row = _read_sb_row(database, source_name, sb_id, beam)
    history_after = sorted(history_root.glob("*.json")) if history_root.exists() else []
    assert _job_ids(normal) == first_job_ids, "Normal rerun submitted another job"
    assert history_after == history_before, "Normal rerun unexpectedly archived/restarted controller"
    assert normal_row["retry_stage"] == "processing_failed", normal_row
    assert int(normal_row["processing_retry_count"]) == 0, normal_row
    assert int(normal_row["redownload_retry_count"]) == 0, normal_row

    logger.info(
        "Gate 6 phase 3/3: explicit retry, reset/preprocess, fresh download, final success"
    )
    final = run_controller(retry_existing=True, **controller_args)
    final_row = _read_sb_row(database, source_name, sb_id, beam)
    assert final.get("status") == "complete", final
    assert final_row["retry_stage"] == "complete", final_row
    assert final_row["error_step"] is None, final_row
    assert final_row["product_copy"] == "Y", final_row
    assert int(final_row["processing_retry_count"]) == 1, final_row
    assert int(final_row["redownload_retry_count"]) == 1, final_row

    ds_files = sorted(product_sb.glob("*.ds"))
    image_i = product_sb / "wsclean_model/wsclean-MFS-I-image.fits"
    image_v = product_sb / "wsclean_model/wsclean-MFS-V-image.fits"
    assert len(ds_files) == 1, f"Expected one final DS, found {ds_files}"
    assert image_i.is_file(), f"Missing final Stokes-I image: {image_i}"
    assert image_v.is_file(), f"Missing final Stokes-V image: {image_v}"
    assert not staged_sb.exists(), "Completed SB staging was not cleaned"

    fault_events = read_json(config.STATE_ROOT / "gate6-fault-events.json", []) or []
    event_stages = [event.get("retry_stage") for event in fault_events]
    assert event_stages == ["none", "processing_retry_ready"], fault_events

    failure_logs = sorted(
        (config.LOG_ROOT / "failures").glob(f"{source_name}_{sb_name}.*.log")
    )
    assert len(failure_logs) == 2, (
        f"Expected two retained controlled-failure logs, found {failure_logs}"
    )
    assert any(
        "reset_askap_ms.py" in path.read_text(encoding="utf-8", errors="replace")
        for path in failure_logs
    ), "Processing retry did not record the required raw-MS reset"

    download_state = read_json(download_state_path(source_name), {}) or {}
    archive_downloads = _download_count(download_state, sb_id)
    assert archive_downloads >= 2, download_state
    source_state = read_json(
        config.STATE_ROOT / f"sources/{source_name}.json", {}
    ) or {}
    assert source_state.get("status") == "complete", source_state

    slurm_errs = sorted(config.LOG_ROOT.glob("slurm-*.err"))
    assert len(slurm_errs) >= 3, (
        f"Expected initial, processing-retry and redownload-retry workers; found {slurm_errs}"
    )
    history_final = sorted(history_root.glob("*.json"))
    assert len(history_final) == len(history_before) + 1, history_final

    return {
        "gate": "gate6",
        "status": "PASS",
        "work": str(work),
        "source_name": source_name,
        "sb_id": sb_id,
        "beam": beam,
        "resources": {
            "walltime": "2:00:00",
            "cpus": 8,
            "memory": "32G",
            "tmp": "10G",
        },
        "initial_job_id": first_job_ids[0],
        "final_job_ids": _job_ids(final),
        "fault_retry_stages": event_stages,
        "processing_retry_count": int(final_row["processing_retry_count"]),
        "redownload_retry_count": int(final_row["redownload_retry_count"]),
        "archive_download_count": archive_downloads,
        "products": {
            "ds": str(ds_files[0]),
            "stokes_i": str(image_i),
            "stokes_v": str(image_v),
        },
        "failure_logs": [str(path) for path in failure_logs],
        "slurm_error_logs": [str(path) for path in slurm_errs],
    }


def main() -> None:
    args = _parse_args()
    if not args.submit:
        raise SystemExit(
            "Gate 6 performs real CASDA downloads and sbatch submissions; rerun with --submit."
        )
    if args.ucs < 1 or args.obs_id < 1:
        raise SystemExit("--ucs and --obs-id must be positive integers")
    if shutil.which("sbatch") is None:
        raise SystemExit("sbatch is not available; run Gate 6 on an Ozstar login node")

    work = _assert_isolated_work(args.work or _default_work(args.ucs, args.obs_id))
    catalogue = _require_file(args.catalogue, "Catalogue")
    time_csv = _require_file(args.time_csv, "Proper-motion time CSV")
    container = _require_file(args.container, "DStools container")
    source_cache = (
        args.cache.expanduser().resolve()
        if args.cache
        else DEFAULT_PRODUCTION_ROOT.resolve()
        / f"state/tap_cache/UCS{args.ucs}.ecsv"
    )
    _require_file(source_cache, "Production TAP cache")
    _verify_container(container)

    work.mkdir(parents=True, exist_ok=True)
    fault_config = _configure_environment(work, catalogue, time_csv, container)
    log_path = _configure_logging(work)
    result_path = work / "state/gate6-result.json"
    try:
        result = _run(args, work, fault_config)
    except Exception as exc:
        failure = {
            "gate": "gate6",
            "status": "FAIL",
            "work": str(work),
            "error": f"{type(exc).__name__}: {exc}",
            "driver_log": str(log_path),
        }
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            json.dumps(failure, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        logging.getLogger("askap.gate6").exception("Gate 6 failed")
        raise SystemExit(f"Gate 6 failed; inspect {result_path} and {log_path}") from exc

    result["driver_log"] = str(log_path)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print("Gate 6 retry validation: PASS")
    print(f"Result: {result_path}")
    print(f"Work:   {work}")


if __name__ == "__main__":
    main()
