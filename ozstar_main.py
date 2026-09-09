"""Plan and submit an Ozstar ASKAP batch.

This is the only file normally edited by the user.  Change ``UCS_START`` and
``UCS_END`` below, then run this script on the Ozstar login node.  It performs
only TAP queries and Slurm submission; all DStools work is done by
``process_job.py`` on a compute node.
"""

from __future__ import annotations

import argparse
import logging
import re
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from . import config
    from .casda_query import read_or_query_source
    from .pipeline_utils import (
        atomic_write_json,
        read_json,
        safe_source_name,
        utc_now,
        write_source_state,
    )
except ImportError:  # Script execution from the deployed program directory.
    import config
    from casda_query import read_or_query_source
    from pipeline_utils import (
        atomic_write_json,
        read_json,
        safe_source_name,
        utc_now,
        write_source_state,
    )

logger = logging.getLogger("askap.ozstar_main")


# ---------------------------------------------------------------------------
# User controls.  Environment variables and CLI arguments can override these,
# but a normal run only requires changing UCS_START/UCS_END and SUBMIT.
# ---------------------------------------------------------------------------
UCS_START = config.UCS_START
UCS_END = config.UCS_END
SUBMIT = False
RETRY_EXISTING = False


def _float_or_zero(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if not np.isfinite(number) else number


def _coordinate_value(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not numeric") from exc
    if not np.isfinite(number):
        raise ValueError(f"{label} is not finite")
    return number


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def _load_catalogue() -> pd.DataFrame:
    if not config.CATALOGUE_PATH.exists():
        raise FileNotFoundError(
            "Source catalogue not found. Copy it to "
            f"{config.CATALOGUE_PATH} or set ASKAP_CATALOGUE."
        )
    catalogue = pd.read_csv(config.CATALOGUE_PATH)
    required = {
        "UCS",
        "UCS_name",
        "ra_j2000_formula",
        "dec_j2000_formula",
        "pmra_formula",
        "pmdec_formula",
        "sptnumabs_formula",
    }
    missing = sorted(required.difference(catalogue.columns))
    if missing:
        raise ValueError(f"Catalogue is missing columns: {', '.join(missing)}")
    return catalogue


def _source_from_row(row: pd.Series) -> dict[str, Any]:
    ucs_number = int(row["UCS"])
    source_name = safe_source_name(_text(row["UCS_name"]))
    ra = _coordinate_value(row["ra_j2000_formula"], "RA")
    dec = _coordinate_value(row["dec_j2000_formula"], "Dec")
    if not 0 <= ra < 360 or not -90 <= dec <= 90:
        raise ValueError(f"Invalid coordinates for {source_name}: RA={ra}, Dec={dec}")
    return {
        "ucs_number": ucs_number,
        "source_name": source_name,
        "ra": ra,
        "dec": dec,
        "pmra": _float_or_zero(row["pmra_formula"]),
        "pmdec": _float_or_zero(row["pmdec_formula"]),
        "sptnum": _float_or_zero(row["sptnumabs_formula"]),
        "batch": config.batch_name(ucs_number),
    }


def _source_is_finished(source_name: str, retry_existing: bool) -> bool:
    if retry_existing:
        return False
    state = read_json(config.STATE_ROOT / "sources" / f"{source_name}.json", {}) or {}
    return state.get("status") in {
        "complete",
        "no_data",
        "skipped",
        "queued",
        "running",
    }


def _wall_hours(observations: int, sources: int) -> float:
    if observations <= 0:
        return 0.0
    waves = int(np.ceil(observations / config.PARALLEL_SLOTS))
    return waves * config.HOURS_PER_OBS + sources * config.SOURCE_OVERHEAD_HOURS


def _time_string(hours: float) -> str:
    seconds = max(60, int(round(hours * 3600)))
    hour, remainder = divmod(seconds, 3600)
    minute, second = divmod(remainder, 60)
    return f"{hour}:{minute:02d}:{second:02d}"


def _job_name(start: int, end: int) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"askap-{start}-{end}-{stamp}"


def _write_sbatch(manifest_path: Path, job_name: str) -> Path:
    script_path = config.STATE_ROOT / "jobs" / f"{job_name}.sbatch"
    time_limit = _time_string(config.JOB_WALLTIME_HOURS)
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={job_name}",
        f"#SBATCH --output={config.LOG_ROOT}/slurm-%j.out",
        f"#SBATCH --error={config.LOG_ROOT}/slurm-%j.err",
        f"#SBATCH --time={time_limit}",
        "#SBATCH --ntasks=1",
        f"#SBATCH --cpus-per-task={config.PARALLEL_SLOTS * config.CPU_PER_SLOT}",
        f"#SBATCH --mem={config.SBATCH_MEM}",
        f"#SBATCH --tmp={config.SBATCH_TMP}",
    ]
    if config.SBATCH_PARTITION:
        lines.append(f"#SBATCH --partition={config.SBATCH_PARTITION}")
    lines.extend(
        [
            "",
            "set -Eeuo pipefail",
            "module load apptainer",
            f"module load {shlex.quote(config.PYTHON_MODULE)}",
            f"command -v {shlex.quote(config.PYTHON_BIN)} >/dev/null",
            f"{shlex.quote(config.PYTHON_BIN)} -c "
            "'import astropy, astroquery, h5py, numpy, pandas'",
            f"cd {shlex.quote(str(config.PROGRAM_DIR))}",
            "exec "
            f"{shlex.quote(config.PYTHON_BIN)} process_job.py --manifest "
            f"{shlex.quote(str(manifest_path))}",
            "",
        ]
    )
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("\n".join(lines), encoding="utf-8")
    script_path.chmod(0o750)
    return script_path


def _submit(script_path: Path) -> str:
    result = subprocess.run(
        ["sbatch", str(script_path)],
        check=True,
        text=True,
        capture_output=True,
    )
    match = re.search(r"(\d+)", result.stdout)
    return match.group(1) if match else result.stdout.strip()


def build_manifest(
    start: int,
    end: int,
    retry_existing: bool,
    refresh_cache: bool = False,
) -> dict[str, Any]:
    config.ensure_directories()
    catalogue = _load_catalogue()
    selected_rows = catalogue[
        (catalogue["UCS"] >= start) & (catalogue["UCS"] <= end)
    ]
    selected_rows = selected_rows.sort_values("UCS")

    sources: list[dict[str, Any]] = []
    total_observations = 0
    budget = config.JOB_WALLTIME_HOURS * config.SAFETY_FACTOR

    for _, row in selected_rows.iterrows():
        try:
            source = _source_from_row(row)
        except ValueError as exc:
            name = f"UCS{int(row['UCS'])}"
            write_source_state(name, status="skipped", reason=str(exc))
            logger.warning("Skipping invalid catalogue row %s: %s", name, exc)
            continue
        name = source["source_name"]
        if source["sptnum"] < config.SPTNUM_THRESHOLD:
            write_source_state(
                name,
                status="skipped",
                reason=f"sptnumabs_formula < {config.SPTNUM_THRESHOLD}",
            )
            continue
        if _source_is_finished(name, retry_existing):
            logger.info("Skipping %s because its state is already active/finished", name)
            continue

        rows, cache = read_or_query_source(
            name,
            source["ra"],
            source["dec"],
            refresh=refresh_cache,
        )
        if len(rows) == 0:
            write_source_state(name, status="no_data", ucs_number=source["ucs_number"])
            logger.info("%s: no usable CASDA observations", name)
            continue

        source["obs_ids"] = [_text(value) for value in rows["obs_id"]]
        source["obs_count"] = len(source["obs_ids"])
        source["query_cache"] = str(cache)
        new_total = total_observations + source["obs_count"]
        new_wall = _wall_hours(new_total, len(sources) + 1)

        exceeds_budget = new_total > config.TARGET_OBS or new_wall > budget
        if sources and (len(sources) >= config.MAX_SOURCES_PER_JOB or exceeds_budget):
            logger.info(
                "Stopping pack before %s: %d observations, %.2f estimated hours",
                name,
                new_total,
                new_wall,
            )
            break

        # Always include the first source, even if it is individually larger
        # than TARGET_OBS. process_job.py will split it across later jobs.
        sources.append(source)
        total_observations = new_total
        write_source_state(
            name,
            status="planned",
            ucs_number=source["ucs_number"],
            obs_count=source["obs_count"],
            query_cache=str(cache),
        )

        if len(sources) >= config.MAX_SOURCES_PER_JOB:
            break

    return {
        "version": 1,
        "created_at": utc_now(),
        "ucs_start": start,
        "ucs_end": end,
        "job_walltime_hours": config.JOB_WALLTIME_HOURS,
        "estimated_observations": total_observations,
        "estimated_wall_hours": _wall_hours(total_observations, len(sources)),
        "sources": sources,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--begin", type=int, default=UCS_START)
    parser.add_argument("--end", type=int, default=UCS_END)
    parser.add_argument("--submit", action="store_true", default=SUBMIT)
    parser.add_argument("--no-submit", action="store_true")
    parser.add_argument("--retry-existing", action="store_true", default=RETRY_EXISTING)
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Re-query TAP instead of reusing the per-source ECSV cache.",
    )
    args = parser.parse_args()

    if args.begin > args.end:
        raise SystemExit("--begin must not be larger than --end")
    if args.no_submit:
        args.submit = False

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    manifest = build_manifest(
        args.begin,
        args.end,
        args.retry_existing,
        refresh_cache=args.refresh_cache,
    )
    if not manifest["sources"]:
        logger.info("No source was selected for this job")
        return

    job_name = _job_name(args.begin, args.end)
    manifest_path = config.STATE_ROOT / "jobs" / f"{job_name}.json"
    manifest["manifest_path"] = str(manifest_path)
    atomic_write_json(manifest_path, manifest)
    script_path = _write_sbatch(manifest_path, job_name)

    print(json_summary(manifest))
    print(f"Manifest: {manifest_path}")
    print(f"SBATCH:   {script_path}")

    if args.submit:
        job_id = _submit(script_path)
        logger.info("Submitted %s as Slurm job %s", script_path, job_id)
        for source in manifest["sources"]:
            state_path = config.STATE_ROOT / "sources" / f"{source['source_name']}.json"
            current = read_json(state_path, {}) or {}
            current["job_id"] = job_id
            current["manifest"] = str(manifest_path)
            if current.get("status") in {None, "planned"}:
                current["status"] = "queued"
            current["updated_at"] = utc_now()
            atomic_write_json(state_path, current)
    else:
        logger.info("Dry plan only; use --submit or set SUBMIT=True to call sbatch")


def json_summary(manifest: dict[str, Any]) -> str:
    return (
        f"Selected {len(manifest['sources'])} source(s), "
        f"{manifest['estimated_observations']} observation(s), "
        f"estimated walltime {manifest['estimated_wall_hours']:.2f} h"
    )


if __name__ == "__main__":
    main()
