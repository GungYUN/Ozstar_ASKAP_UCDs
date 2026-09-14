"""Plan, stage and sequentially submit an Ozstar ASKAP UCS range.

English: The login-node controller partitions the inclusive UCS range into
sequential ada-compatible blocks, prepares one block at a time, and submits
one compute worker per block. The worker and controller communicate through
manifests and filesystem state; Slurm queue/accounting commands are not used
to decide whether scientific processing is complete.

中文：登录节点 controller 将包含端点的 UCS 范围顺序切成 ada-compatible block，
一次准备一个 block，并为每个 block 提交一个计算 worker。worker 和 controller
通过 manifest 与文件系统状态通信；不会使用 Slurm 队列/记账命令判断科学处理是否完成。
"""

from __future__ import annotations

import argparse
import logging
import re
import shlex
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from . import config
    from .casda_query import estimated_access_bytes, read_or_query_source
    from .prepare_download import prepare_manifest
    from .pipeline_utils import (
        atomic_write_json,
        completed_product_sb_numbers,
        directory_size,
        obs_number,
        persistent_usage,
        read_json,
        safe_source_name,
        source_product_root,
        utc_now,
        write_source_state,
    )
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config
    from casda_query import estimated_access_bytes, read_or_query_source
    from prepare_download import prepare_manifest
    from pipeline_utils import (
        atomic_write_json,
        completed_product_sb_numbers,
        directory_size,
        obs_number,
        persistent_usage,
        read_json,
        safe_source_name,
        source_product_root,
        utc_now,
        write_source_state,
    )


logger = logging.getLogger("askap.ozstar_main")


# ---------------------------------------------------------------------------
# User controls. Environment variables and CLI arguments can override these,
# but a normal run only requires the inclusive range and --submit.
# 用户控制项。环境变量和 CLI 参数可以覆盖这些默认值；正常运行只需要包含端点的
# 范围和 --submit。
# ---------------------------------------------------------------------------
UCS_START = config.UCS_START
UCS_END = config.UCS_END
SUBMIT = False
RETRY_EXISTING = False

TERMINAL_STATUSES = {"complete", "no_data", "skipped", "download_failed"}
ACTIVE_STATUSES = {"planned", "staged", "queued", "running", "partial"}
FAILURE_STATUSES = {"failed", "stage_failed", "budget_failed"}


class StagingBudgetError(RuntimeError):
    """Signal that a source/block cannot enter persistent staging.

    English: The controller fails closed when the estimated CASDA archive size
    cannot fit the configured budget or current persistent free space.

    中文：当 CASDA archive 估计大小无法放入配置预算或当前持久化剩余空间时，
    controller 以失败关闭，而不是继续下载。
    """


def _float_or_zero(value: Any) -> float:
    """Convert a catalogue value to a finite float, otherwise return zero.

    中文：把 catalogue 值转换为有限浮点数；无效值统一返回零。
    """

    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if not np.isfinite(number) else number


def _coordinate_value(value: Any, label: str) -> float:
    """Validate one finite coordinate from the source catalogue.

    中文：验证 source catalogue 中的一个有限坐标值。
    """

    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not numeric") from exc
    if not np.isfinite(number):
        raise ValueError(f"{label} is not finite")
    return number


def _text(value: Any) -> str:
    """Decode byte-like catalogue values without changing normal text.

    中文：解码 bytes 类型的 catalogue 值，同时保持普通文本不变。
    """

    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def _load_catalogue() -> pd.DataFrame:
    """Load and validate the catalogue columns needed for planning.

    中文：读取并验证规划阶段所需的 catalogue 列。
    """

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
    """Convert one catalogue row into a safe controller source record.

    English: The `batch` field remains the original ada-compatible 50-source
    name, even if storage later makes the controller block smaller.

    中文：将一行 catalogue 转成安全的 controller source record；即使后续因存储
    限制缩小 block，`batch` 仍是原 ada-compatible 50-source 名称。
    """

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
        # Keep the original ada directory name, not a new block name. It remains
        # correct when storage causes an early 2 TB split.
        # 保留原 ada 目录名而不是新造 block 名；即使因 2 TB 限制提前拆分也仍正确。
        "batch": config.batch_name(ucs_number),
    }


def _source_state(source_name: str) -> dict[str, Any]:
    """Read the persistent state for one source, or an empty record.

    中文：读取一个源的持久化状态；不存在时返回空 record。
    """

    return read_json(config.STATE_ROOT / "sources" / f"{source_name}.json", {}) or {}


def _wall_hours(observations: int, sources: int) -> float:
    """Estimate wall time from 3-hour observation waves and source overhead.

    中文：根据每个 observation 的 3 小时 wave 估计和每源额外开销计算 wall time。
    """

    if observations <= 0:
        return 0.0
    waves = int(np.ceil(observations / config.PARALLEL_SLOTS))
    return waves * config.HOURS_PER_OBS + sources * config.SOURCE_OVERHEAD_HOURS


def _time_string(hours: float) -> str:
    """Format an hour value as the Slurm `H:MM:SS` time limit.

    中文：把小时数格式化为 Slurm 使用的 `H:MM:SS` 时间限制。
    """

    seconds = max(60, int(round(hours * 3600)))
    hour, remainder = divmod(seconds, 3600)
    minute, second = divmod(remainder, 60)
    return f"{hour}:{minute:02d}:{second:02d}"


def _job_name(start: int, end: int) -> str:
    """Return a compact Slurm name using the historical U<start>-<count> form.

    中文：使用历史上的 U<start>-<count> 形式返回短 Slurm 名称。
    """

    count = end - start + 1
    name = f"U{start}-{count}"
    if len(name) <= 8:
        return name
    # Keep Slurm names compact; the timestamped job tag makes script filenames
    # unique if the compact job name collides.
    # Slurm 名称有意保持短小；带时间戳的 job tag 可避免脚本文件名冲突。
    return f"U{start % 1000:03d}-{count % 100:02d}"


def _job_tag(start: int, end: int) -> str:
    """Add a UTC timestamp to the compact job name for unique artifacts.

    中文：在短 job 名称后加入 UTC 时间戳，为生成的 artifact 提供唯一标识。
    """

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{_job_name(start, end)}-{stamp}"


def _write_sbatch(
    manifest_path: Path,
    job_tag: str,
    job_name: str,
    allow_compute_download: bool = False,
    max_resubmits: int | None = None,
) -> Path:
    """Write the worker sbatch script, including bounded partial resubmission.

    English: The generated worker requests the configured 4 x 8 CPU layout,
    memory, scratch, and walltime. A dedicated partial exit code causes the
    same script to resubmit itself up to the configured limit.

    中文：生成的 worker 请求配置的 4 x 8 CPU 布局、内存、scratch 和 walltime。
    worker 返回专用 partial code 时，同一个脚本会在配置次数内自动重新提交。
    """

    script_path = config.STATE_ROOT / "jobs" / f"{job_tag}.sbatch"
    time_limit = _time_string(config.JOB_WALLTIME_HOURS)
    worker_command = (
        f"{shlex.quote(config.PYTHON_BIN)} process_job.py --manifest "
        f"{shlex.quote(str(manifest_path))}"
    )
    if allow_compute_download:
        worker_command += " --allow-compute-download"
    bounded_resubmits = config.MAX_AUTO_RESUBMITS if max_resubmits is None else max_resubmits
    if bounded_resubmits < 0:
        raise ValueError("max_resubmits must not be negative")
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
        lines.append(f"#SBATCH --partition={shlex.quote(config.SBATCH_PARTITION)}")
    lines.extend(
        [
            "",
            "set -Eeuo pipefail",
            "module load apptainer",
            f"module load {shlex.quote(config.PYTHON_PARENT_MODULE)}",
            f"module load {shlex.quote(config.PYTHON_MODULE)}",
            f"command -v {shlex.quote(config.PYTHON_BIN)} >/dev/null",
            f"{shlex.quote(config.PYTHON_BIN)} -c "
            "'import astropy, astroquery, h5py, keyring, numpy, pandas'",
            f"cd {shlex.quote(str(config.PROGRAM_DIR))}",
            f"MAX_AUTO_RESUBMITS={int(bounded_resubmits)}",
            "AUTO_RESUBMIT_COUNT=\"${ASKAP_AUTO_RESUBMIT_COUNT:-0}\"",
            "set +e",
            worker_command,
            "worker_status=$?",
            "set -e",
            f"if [[ \"$worker_status\" -eq {config.PARTIAL_EXIT_CODE} ]]; then",
            "  if (( AUTO_RESUBMIT_COUNT >= MAX_AUTO_RESUBMITS )); then",
            "    echo \"partial worker reached automatic resubmit limit\" >&2",
            "    exit \"$worker_status\"",
            "  fi",
            "  next_resubmit=$((AUTO_RESUBMIT_COUNT + 1))",
            "  echo \"worker returned partial; submitting attempt $next_resubmit\"",
            "  sbatch \"--export=ALL,ASKAP_AUTO_RESUBMIT_COUNT=$next_resubmit\" \"$0\"",
            "  exit 0",
            "fi",
            "exit \"$worker_status\"",
            "",
        ]
    )
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("\n".join(lines), encoding="utf-8")
    script_path.chmod(0o750)
    return script_path


def _submit(script_path: Path) -> str:
    """Submit one generated sbatch script and return its Slurm identifier.

    中文：提交一个生成的 sbatch 脚本，并返回 Slurm 标识符。
    """

    result = subprocess.run(
        ["sbatch", str(script_path)],
        check=True,
        text=True,
        capture_output=True,
    )
    match = re.search(r"(\d+)", result.stdout)
    return match.group(1) if match else result.stdout.strip()


def _source_record(source: dict[str, Any], status: str, reason: str | None = None) -> dict[str, Any]:
    """Create the small source-state record stored in a block manifest.

    中文：创建写入 block manifest 的精简源状态 record。
    """

    record = {
        "source_name": source["source_name"],
        "ucs_number": source["ucs_number"],
        "status": status,
    }
    if "expected_sb" in source:
        record["expected_sb"] = source["expected_sb"]
    if reason:
        record["reason"] = reason
    return record


def _plan_source(
    row: pd.Series,
    retry_existing: bool,
    refresh_cache: bool,
) -> tuple[dict[str, Any], str, str | None]:
    """Query one catalogue source and return its data and planning state.

    English: Full filtered TAP rows are cached by `casda_query`; their
    deduplicated `access_estsize` controls staging admission.

    中文：完整的过滤后 TAP 行由 `casda_query` 缓存；去重后的 `access_estsize` 控制
    staging 准入。
    """

    ucs_number = int(row["UCS"])
    fallback_name = f"UCS{ucs_number}"
    try:
        source = _source_from_row(row)
    except ValueError as exc:
        write_source_state(fallback_name, status="skipped", reason=str(exc), ucs_number=ucs_number)
        return {
            "source_name": fallback_name,
            "ucs_number": ucs_number,
            "expected_sb": [],
        }, "skipped", str(exc)

    name = source["source_name"]
    current = _source_state(name)
    current_status = current.get("status")
    if not retry_existing and current_status in TERMINAL_STATUSES:
        if current.get("expected_sb") is not None:
            source["expected_sb"] = current["expected_sb"]
        return source, str(current_status), None
    if not retry_existing and current_status in ACTIVE_STATUSES:
        # A controller restart resumes this block from its state file. Avoid a
        # second worker while an older run is active.
        # controller 重启会从状态文件恢复；旧运行仍 active 时不要提交第二个 worker。
        source["expected_sb"] = current.get("expected_sb", [])
        source["active_status"] = str(current_status)
        return source, "active", None

    if source["sptnum"] < config.SPTNUM_THRESHOLD:
        reason = f"sptnumabs_formula < {config.SPTNUM_THRESHOLD}"
        write_source_state(
            name,
            status="skipped",
            reason=reason,
            ucs_number=source["ucs_number"],
        )
        return source, "skipped", reason

    rows, cache = read_or_query_source(
        name,
        source["ra"],
        source["dec"],
        refresh=refresh_cache,
    )
    source["query_cache"] = str(cache)
    source["query_columns"] = list(rows.colnames)
    source["retains_t_min"] = "t_min" in rows.colnames
    source["retains_t_max"] = "t_max" in rows.colnames
    if len(rows) == 0:
        write_source_state(
            name,
            status="no_data",
            ucs_number=source["ucs_number"],
            query_cache=str(cache),
            estimated_staging_bytes=0,
        )
        return source, "no_data", None

    source["estimated_staging_bytes"] = estimated_access_bytes(rows)
    source["estimated_access_estsize_kb"] = source["estimated_staging_bytes"] / 1024.0

    source["obs_ids"] = [_text(value) for value in rows["obs_id"]]
    source["obs_count"] = len(source["obs_ids"])
    source["expected_sb"] = sorted(
        number for value in source["obs_ids"] if (number := obs_number(value)) is not None
    )
    estimate = int(source["estimated_staging_bytes"])
    if estimate > config.STAGING_BUDGET_BYTES:
        message = (
            f"{name}: estimated staging size {estimate} bytes exceeds the "
            f"{config.STAGING_BUDGET_BYTES}-byte block budget"
        )
        write_source_state(
            name,
            status="budget_failed",
            ucs_number=source["ucs_number"],
            query_cache=str(cache),
            estimated_staging_bytes=estimate,
            error=message,
        )
        raise StagingBudgetError(message)

    return source, "planned", None


def _ada_batch_end(ucs_number: int) -> int:
    """Return the end of the original 50-number ada batch.

    中文：返回原始 50 编号 ada batch 的结束编号。
    """

    return ((ucs_number - 1) // config.BLOCK_SIZE) * config.BLOCK_SIZE + config.BLOCK_SIZE


def _build_block(
    start: int,
    requested_end: int,
    catalogue: pd.DataFrame,
    retry_existing: bool,
    refresh_cache: bool,
) -> dict[str, Any]:
    """Build one at-most-50-UCS manifest before a storage-budget overflow.

    English: Both the configured 2 TB estimate and actual staging/free-space
    state participate in admission. A block keeps the original ada batch name.

    中文：准入同时考虑配置的 2 TB 估计预算以及实际 staging/剩余空间状态，
    并保留原 ada batch 名称。
    """

    candidate_end = min(requested_end, _ada_batch_end(start))
    staging_before = directory_size(config.STAGING_ROOT)
    _total, work_used, work_free = persistent_usage(config.WORK)
    available_budget = min(
        config.STAGING_BUDGET_BYTES - staging_before,
        work_free,
    )
    selected = catalogue[
        (catalogue["UCS"] >= start) & (catalogue["UCS"] <= candidate_end)
    ].sort_values("UCS")

    sources: list[dict[str, Any]] = []
    source_records: list[dict[str, Any]] = []
    total_observations = 0
    estimated_bytes = 0
    block_end = candidate_end

    for _, row in selected.iterrows():
        source, status, reason = _plan_source(row, retry_existing, refresh_cache)
        if status in TERMINAL_STATUSES:
            source_records.append(_source_record(source, status, reason))
            continue
        if status == "active":
            source_records.append(
                _source_record(source, source.get("active_status", "active"), reason)
            )
            # Do not start later UCSs while an earlier source belongs to another
            # worker/controller.
            # 较早的源仍由其他 worker/controller 负责时，不启动后面的 UCS。
            block_end = int(row["UCS"])
            break
        if status != "planned":
            raise RuntimeError(f"Unexpected planning state {status!r} for {source['source_name']}")

        source_estimate = int(source["estimated_staging_bytes"])
        if source_estimate > config.STAGING_BUDGET_BYTES:
            raise StagingBudgetError(
                f"{source['source_name']}: estimated staging size "
                f"{source_estimate} bytes exceeds the "
                f"{config.STAGING_BUDGET_BYTES}-byte block budget"
            )
        if estimated_bytes + source_estimate > available_budget:
            if not sources:
                raise StagingBudgetError(
                    f"{source['source_name']}: estimated staging size "
                    f"{source_estimate} bytes cannot fit in the available "
                    f"persistent staging budget ({available_budget} bytes; "
                    f"staging used={staging_before}, WORK free={work_free})"
                )
            block_end = int(row["UCS"]) - 1
            break

        sources.append(source)
        source_records.append(_source_record(source, "planned"))
        write_source_state(
            source["source_name"],
            status="planned",
            ucs_number=source["ucs_number"],
            obs_count=source["obs_count"],
            expected_sb=source["expected_sb"],
            query_cache=source["query_cache"],
            query_columns=source["query_columns"],
            retains_t_min=source["retains_t_min"],
            retains_t_max=source["retains_t_max"],
            estimated_staging_bytes=source_estimate,
        )
        estimated_bytes += source_estimate
        total_observations += int(source["obs_count"])

    return {
        "version": 2,
        "created_at": utc_now(),
        "block_start": start,
        "block_end": block_end,
        "ucs_start": start,
        "ucs_end": block_end,
        "batch": config.batch_name(start),
        "staging_budget_bytes": config.STAGING_BUDGET_BYTES,
        "staging_usage_before_bytes": staging_before,
        "work_used_before_bytes": work_used,
        "work_free_before_bytes": work_free,
        "estimated_staging_bytes": estimated_bytes,
        "estimated_observations": total_observations,
        "estimated_wall_hours": _wall_hours(total_observations, len(sources)),
        "query_rows_are_full_ecsv": True,
        "sources": sources,
        "source_records": source_records,
    }


def build_manifest(
    start: int,
    end: int,
    retry_existing: bool,
    refresh_cache: bool = False,
) -> dict[str, Any]:
    """Build one block manifest for callers using the single-block API.

    中文：为使用单 block API 的调用者生成一个 block manifest。
    """

    if end < start:
        raise ValueError("end must not be smaller than start")
    if end - start + 1 > config.BLOCK_SIZE:
        raise ValueError("build_manifest accepts at most one 50-UCS block")
    config.ensure_directories()
    return _build_block(
        start,
        end,
        _load_catalogue(),
        retry_existing,
        refresh_cache,
    )


def _controller_path(start: int, end: int) -> Path:
    """Return the persistent controller JSON path for an inclusive range.

    中文：返回一个包含端点范围对应的持久化 controller JSON 路径。
    """

    return config.STATE_ROOT / "controllers" / f"UCS{start}-{end}.json"


def _new_controller(start: int, end: int) -> dict[str, Any]:
    """Create the initial resumable controller record for a range.

    中文：为一个范围创建初始的可恢复 controller record。
    """

    return {
        "version": 2,
        "created_at": utc_now(),
        "ucs_start": start,
        "ucs_end": end,
        "next_ucs": start,
        "status": "running",
        "blocks": [],
    }


def _reset_incomplete_controller_for_retry(
    controller: dict[str, Any],
    controller_path: Path,
) -> None:
    """Discard only unsubmitted blocks while preserving existing products.

    English: This is intentionally used only for an explicit retry request.

    中文：该操作只在明确请求 retry 时使用，并保留已有产物。
    """

    blocks = controller.get("blocks", [])
    first_incomplete = next(
        (index for index, block in enumerate(blocks) if block.get("status") != "complete"),
        None,
    )
    if first_incomplete is None:
        return

    incomplete = blocks[first_incomplete:]
    if any(block.get("status") == "submitted" for block in incomplete):
        raise RuntimeError(
            f"Cannot retry {controller_path} while a controller block is submitted; "
            "wait for it to finish or inspect its state first."
        )

    for block in incomplete:
        for record in block.get("source_records", []):
            source_name = record.get("source_name")
            if not source_name:
                continue
            state = _source_state(source_name)
            if state.get("status") in {
                "planned",
                "staged",
                "queued",
                "running",
                "partial",
                "stage_failed",
            }:
                write_source_state(
                    source_name,
                    status="planned",
                    retry_reason="reset incomplete controller for retry",
                )

    controller["blocks"] = blocks[:first_incomplete]
    if controller["blocks"]:
        controller["next_ucs"] = controller["blocks"][-1]["block_end"] + 1
    else:
        controller["next_ucs"] = controller["ucs_start"]
    controller["status"] = "running"
    controller.pop("completed_at", None)
    atomic_write_json(controller_path, controller)


def _staging_residue(source_name: str) -> list[str]:
    """List archives, MS trees, and partial markers left in source staging.

    中文：列出源 staging 中遗留的 archive、MS 树和 partial 标记。
    """

    root = config.STAGING_ROOT / source_name
    if not root.exists():
        return []
    residue: list[str] = []
    for path in root.rglob("*"):
        name = path.name.lower()
        if (
            (path.is_dir() and name.endswith(".ms"))
            or name.endswith((".tar", ".tar.gz", ".tgz", ".checksum"))
            or name.endswith(".ds.part")
            or name.startswith(".extract-")
        ):
            residue.append(str(path.relative_to(root)))
    return sorted(residue)


def _verify_complete_source(record: dict[str, Any]) -> None:
    """Verify products and absence of unprocessed staging residue for a source.

    中文：验证一个源的产物，并确认 staging 中没有未处理残留。
    """

    source_name = safe_source_name(str(record["source_name"]))
    state = _source_state(source_name)
    expected = {
        int(value)
        for value in state.get("expected_sb", record.get("expected_sb", []))
    }
    product_root = source_product_root(int(record["ucs_number"]), source_name)
    completed = completed_product_sb_numbers(product_root)
    missing = sorted(expected.difference(completed))
    residue = _staging_residue(source_name)
    if missing or residue:
        details = []
        if missing:
            details.append("missing valid products for " + ", ".join(f"SB{number}" for number in missing))
        if residue:
            details.append("unprocessed staging entries: " + ", ".join(residue[:20]))
        raise RuntimeError(f"{source_name}: complete state failed filesystem verification ({'; '.join(details)})")


def _inspect_block(block: dict[str, Any]) -> bool:
    """Return true only when every source has a verified terminal state.

    中文：只有所有源都处于经过验证的终态时才返回 true。
    """

    pending: list[str] = []
    for record in block.get("source_records", []):
        name = safe_source_name(str(record["source_name"]))
        state = _source_state(name)
        status = state.get("status")
        if status == "complete":
            _verify_complete_source(record)
        elif status in {"no_data", "skipped"}:
            continue
        elif status == "download_failed":
            logger.warning(
                "%s: download retries exhausted; continuing with the next source",
                name,
            )
            continue
        elif status in FAILURE_STATUSES:
            raise RuntimeError(
                f"{name}: worker/preparation failed: {state.get('error', status)}"
            )
        else:
            pending.append(f"{name}={status or 'missing'}")
    if pending:
        logger.info("Block %s-%s still pending: %s", block["block_start"], block["block_end"], ", ".join(pending))
        return False
    return True


def _create_block(
    controller: dict[str, Any],
    catalogue: pd.DataFrame,
    retry_existing: bool,
    refresh_cache: bool,
    allow_compute_download: bool,
    max_resubmits: int,
) -> dict[str, Any]:
    """Create and persist the next sequential controller block.

    中文：创建并持久化下一个顺序 controller block。
    """

    start = int(controller["next_ucs"])
    end = int(controller["ucs_end"])
    manifest = _build_block(start, end, catalogue, retry_existing, refresh_cache)
    job_tag = _job_tag(manifest["block_start"], manifest["block_end"])
    manifest_path = config.STATE_ROOT / "jobs" / f"{job_tag}.json"
    manifest["manifest_path"] = str(manifest_path)
    manifest["controller_path"] = str(_controller_path(controller["ucs_start"], controller["ucs_end"]))
    atomic_write_json(manifest_path, manifest)
    script_path = None
    if manifest["sources"]:
        script_path = _write_sbatch(
            manifest_path,
            job_tag,
            _job_name(manifest["block_start"], manifest["block_end"]),
            allow_compute_download=allow_compute_download,
            max_resubmits=max_resubmits,
        )
    block = {
        "block_start": manifest["block_start"],
        "block_end": manifest["block_end"],
        "batch": manifest["batch"],
        "manifest_path": str(manifest_path),
        "script_path": str(script_path) if script_path else None,
        "job_tag": job_tag,
        "source_records": manifest["source_records"],
        "source_names": [source["source_name"] for source in manifest["sources"]],
        "estimated_staging_bytes": manifest["estimated_staging_bytes"],
        "status": "planned",
    }
    controller["next_ucs"] = manifest["block_end"] + 1
    controller["blocks"].append(block)
    return block


def _prepare_and_submit_block(
    controller: dict[str, Any],
    block: dict[str, Any],
    submit: bool,
    prepare_requested: bool,
) -> None:
    """Prepare archives on the login node and optionally submit the worker.

    English: Preparation is deliberately completed before submission; the
    compute worker normally has no CASDA network access.

    中文：准备步骤有意在提交前完成；正常情况下计算 worker 无 CASDA 网络访问。
    """

    manifest_path = Path(block["manifest_path"])
    manifest = read_json(manifest_path)
    if not manifest:
        raise FileNotFoundError(f"Block manifest is missing: {manifest_path}")

    if not manifest.get("sources"):
        if _inspect_block(block):
            block["status"] = "complete"
        return

    if prepare_requested and block["status"] in {"planned", "preparing"}:
        block["status"] = "preparing"
        try:
            manifest = prepare_manifest(manifest_path, allow_multiple=True)
        except Exception:
            block["status"] = "failed"
            raise
        block["source_records"] = manifest.get(
            "source_records", block.get("source_records", [])
        )
        block["source_names"] = [
            source["source_name"] for source in manifest.get("sources", [])
        ]
        block["status"] = "prepared"

        if not manifest.get("sources"):
            if _inspect_block(block):
                block["status"] = "complete"
            return

    if not submit:
        return
    if block["status"] in {"planned", "preparing"}:
        raise RuntimeError(
            f"{manifest_path}: submit requested before login-node archive preparation"
        )
    if block["status"] == "prepared":
        script_path = Path(str(block["script_path"]))
        job_id = _submit(script_path)
        block["job_id"] = job_id
        block["submitted_at"] = utc_now()
        block["status"] = "submitted"
        for source in manifest["sources"]:
            name = safe_source_name(str(source["source_name"]))
            current = _source_state(name)
            current["job_id"] = job_id
            current["manifest"] = str(manifest_path)
            if current.get("status") in {None, "planned", "staged"}:
                current["status"] = "queued"
            current["updated_at"] = utc_now()
            atomic_write_json(config.STATE_ROOT / "sources" / f"{name}.json", current)


def _wait_for_block(
    controller_path: Path,
    controller: dict[str, Any],
    block: dict[str, Any],
    initial_delay: float,
    poll_interval: float,
) -> None:
    """Poll filesystem state until the block has verified terminal sources.

    中文：轮询文件系统状态，直到 block 中的源都达到经过验证的终态。
    """

    if block["status"] == "submitted" and initial_delay > 0:
        logger.info(
            "Waiting %.1f seconds before filesystem polling for block %s-%s",
            initial_delay,
            block["block_start"],
        )
        time.sleep(initial_delay)
    while True:
        if _inspect_block(block):
            block["status"] = "complete"
            block["completed_at"] = utc_now()
            atomic_write_json(controller_path, controller)
            return
        atomic_write_json(controller_path, controller)
        if poll_interval > 0:
            time.sleep(poll_interval)


def run_controller(
    start: int,
    end: int,
    submit: bool,
    prepare_requested: bool,
    retry_existing: bool,
    refresh_cache: bool,
    allow_compute_download: bool,
    max_resubmits: int,
    initial_delay: float,
    poll_interval: float,
) -> dict[str, Any]:
    """Run a resumable filesystem-state controller for one inclusive range.

    English: Blocks are created and submitted sequentially. A reconnect reads
    the controller JSON rather than inferring completion from Slurm accounting.

    中文：为一个包含端点的范围运行可恢复的文件系统状态 controller。block 按顺序
    创建和提交；重连时读取 controller JSON，不根据 Slurm 记账推断完成。
    """

    config.ensure_directories()
    controller_path = _controller_path(start, end)
    controller = read_json(controller_path)
    if controller:
        if controller.get("ucs_start") != start or controller.get("ucs_end") != end:
            raise RuntimeError(f"Controller range does not match {controller_path}")
    else:
        controller = _new_controller(start, end)
    if controller.get("status") == "complete":
        logger.info("Controller already completed: %s", controller_path)
        return controller

    if retry_existing:
        _reset_incomplete_controller_for_retry(controller, controller_path)

    catalogue = _load_catalogue()
    while True:
        unfinished = [block for block in controller["blocks"] if block.get("status") != "complete"]
        if unfinished:
            block = unfinished[0]
            if submit or prepare_requested:
                try:
                    _prepare_and_submit_block(
                        controller,
                        block,
                        submit=submit,
                        prepare_requested=prepare_requested,
                    )
                except Exception:
                    atomic_write_json(controller_path, controller)
                    raise
            atomic_write_json(controller_path, controller)
            if submit and block.get("status") == "submitted":
                _wait_for_block(controller_path, controller, block, initial_delay, poll_interval)
                continue
            if not submit:
                logger.info(
                    "No submission requested; controller block %s-%s is %s. "
                    "Manifest: %s",
                    block["block_start"],
                    block["block_end"],
                    block.get("status"),
                    block.get("manifest_path"),
                )
                break
            if block.get("status") == "complete":
                continue
            break

        if int(controller["next_ucs"]) > end:
            controller["status"] = "complete"
            controller["completed_at"] = utc_now()
            atomic_write_json(controller_path, controller)
            logger.info("Completed UCS range %d-%d", start, end)
            return controller

        block = _create_block(
            controller,
            catalogue,
            retry_existing=retry_existing,
            refresh_cache=refresh_cache,
            allow_compute_download=allow_compute_download,
            max_resubmits=max_resubmits,
        )
        atomic_write_json(controller_path, controller)
        logger.info(
            "Planned block %d-%d (%s): %s",
            block["block_start"],
            block["block_end"],
            block["batch"],
            json_summary(read_json(Path(block["manifest_path"]))),
        )
        if not submit and not prepare_requested:
            continue


def json_summary(manifest: dict[str, Any]) -> str:
    """Return a concise human-readable manifest estimate.

    中文：返回简洁、可读的 manifest 估计摘要。
    """

    return (
        f"Selected {len(manifest.get('sources', []))} source(s), "
        f"{manifest.get('estimated_observations', 0)} observation(s), "
        f"estimated staging {int(manifest.get('estimated_staging_bytes', 0))} bytes, "
        f"estimated walltime {float(manifest.get('estimated_wall_hours', 0.0)):.2f} h"
    )


def main() -> None:
    """Parse CLI controls and run the login-node controller.

    中文：解析 CLI 控制项并运行登录节点 controller。
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--begin", type=int, default=UCS_START)
    parser.add_argument("--end", type=int, default=UCS_END)
    parser.add_argument("--submit", action="store_true", default=SUBMIT)
    parser.add_argument("--no-submit", action="store_true")
    parser.add_argument("--retry-existing", action="store_true", default=RETRY_EXISTING)
    parser.add_argument(
        "--prepare-download",
        action="store_true",
        help="Prepare login-node CASDA archives even when not submitting (normally --submit implies this).",
    )
    parser.add_argument(
        "--allow-multiple",
        action="store_true",
        help="Retained for compatibility; block size and staging budget now control multi-source preparation.",
    )
    parser.add_argument(
        "--allow-compute-download",
        action="store_true",
        help="Debugging only: let the generated compute job attempt CASDA downloads.",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Re-query TAP instead of reusing per-source full-row ECSV caches.",
    )
    parser.add_argument(
        "--max-resubmits",
        type=int,
        default=config.MAX_AUTO_RESUBMITS,
        help="Maximum automatic self-resubmits after a partial worker exit.",
    )
    parser.add_argument(
        "--controller-initial-delay",
        type=float,
        default=config.CONTROLLER_INITIAL_DELAY_SECONDS,
        help="Initial filesystem-poll delay after submission; completion is never time-based.",
    )
    parser.add_argument(
        "--controller-poll-interval",
        type=float,
        default=config.CONTROLLER_POLL_INTERVAL_SECONDS,
        help="Filesystem-state polling interval in seconds.",
    )
    args = parser.parse_args()

    if args.begin > args.end:
        raise SystemExit("--begin must not be larger than --end")
    if args.max_resubmits < 0:
        raise SystemExit("--max-resubmits must not be negative")
    if args.no_submit:
        args.submit = False

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run_controller(
            args.begin,
            args.end,
            submit=args.submit,
            prepare_requested=args.submit or args.prepare_download,
            retry_existing=args.retry_existing,
            refresh_cache=args.refresh_cache,
            allow_compute_download=args.allow_compute_download,
            max_resubmits=args.max_resubmits,
            initial_delay=max(0.0, args.controller_initial_delay),
            poll_interval=max(0.0, args.controller_poll_interval),
        )
    except Exception as exc:
        raise SystemExit(f"Ozstar controller failed: {exc}") from exc


if __name__ == "__main__":
    main()
