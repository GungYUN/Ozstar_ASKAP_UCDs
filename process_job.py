"""Run one prepared manifest on an Ozstar compute node.

English: Persistent data remains below `ASKAP_WORK`. Sources are handled
serially while up to four SB folders run in parallel, with eight CPU threads
per DStools process by default. CASDA downloads are disabled by default because
compute nodes cannot resolve `data.csiro.au`; login preparation leaves tar
files for this worker to extract. A walltime partial result is checkpointed so
the generated sbatch wrapper can resubmit itself.

中文：持久化数据全部位于 `ASKAP_WORK` 下。源按顺序处理，每次最多并行四个 SB 目录，
默认每个 DStools 进程使用八个 CPU 线程。由于计算节点不能解析 `data.csiro.au`，
默认禁止 CASDA 下载；登录节点准备步骤会留下 tar 供 worker 解压。墙钟导致的 partial
结果会写入 checkpoint，生成的 sbatch wrapper 可以自动重新提交。
"""

from __future__ import annotations

import argparse
import h5py
import logging
import os
import re
import shutil
import subprocess
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
from astropy.io import fits
from astropy.table import Table

try:
    from . import config
    from .crop_fits import crop_fits
    from .pipeline_utils import (
        atomic_write_json,
        completed_product_sb_names,
        completed_product_sb_numbers,
        obs_number,
        read_json,
        read_t_min_map,
        read_t_min_map_from_table,
        safe_source_name,
        sb_number_from_name,
        source_product_root,
        utc_now,
        write_source_state,
    )
    from .staging import decompress_and_move
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config
    from crop_fits import crop_fits
    from pipeline_utils import (
        atomic_write_json,
        completed_product_sb_names,
        completed_product_sb_numbers,
        obs_number,
        read_json,
        read_t_min_map,
        read_t_min_map_from_table,
        safe_source_name,
        sb_number_from_name,
        source_product_root,
        utc_now,
        write_source_state,
    )
    from staging import decompress_and_move

logger = logging.getLogger("askap.process_job")


class NotEnoughTime(RuntimeError):
    """Signal that another subprocess would cross the walltime reserve.

    中文：表示启动另一个子进程会越过预留的 walltime 安全窗口。
    """


class ProcessingError(RuntimeError):
    """Signal that one SB cannot be completed by the worker.

    中文：表示 worker 无法完成一个 SB 的处理。
    """


def _remove_path(path: Path) -> None:
    """Remove a file, directory, or symlink used by retry cleanup.

    中文：删除重试清理阶段使用的文件、目录或符号链接。
    """

    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def _find_ms(sb_dir: Path) -> Path | None:
    """Find a valid raw MeasurementSet in an SB directory.

    中文：在 SB 目录中查找有效的原始 MeasurementSet。
    """

    candidates = sorted(
        path
        for path in sb_dir.iterdir()
        if path.name.endswith(".ms")
        and not path.name.endswith(".subtracted.ms")
        and path.is_dir()
        and (path / "table.dat").exists()
    )
    return candidates[0] if candidates else None


def _find_subtracted_ms(sb_dir: Path) -> Path | None:
    """Find a DStools-subtracted MeasurementSet in an SB directory.

    中文：在 SB 目录中查找 DStools 生成的减源 MeasurementSet。
    """

    candidates = sorted(sb_dir.glob("*.subtracted.ms"))
    return candidates[0] if candidates else None


def _valid_ds(path: Path) -> bool:
    """Check that a DS file contains the required HDF5 datasets.

    中文：检查 DS 文件是否包含所需的 HDF5 dataset。
    """

    try:
        with h5py.File(path, "r") as handle:
            return {"time", "frequency", "flux"}.issubset(handle.keys())
    except (OSError, ValueError):
        return False


def _find_ds(sb_dir: Path) -> Path | None:
    """Return the first valid DS product and remove invalid candidates.

    中文：返回第一个有效 DS 产物，并删除无效候选文件。
    """

    for candidate in sorted(sb_dir.glob("*.ds")):
        if _valid_ds(candidate):
            return candidate
        logger.warning("Removing invalid/incomplete DS file %s", candidate)
        candidate.unlink(missing_ok=True)
    return None


def _valid_fits(path: Path) -> bool:
    """Check that a FITS image opens and contains non-empty data.

    中文：检查 FITS 图像可以打开且包含非空数据。
    """

    try:
        with fits.open(path, memmap=False) as hdul:
            return hdul[0].data is not None and hdul[0].data.size > 0
    except (OSError, ValueError):
        return False


def _find_model_images(model_dir: Path) -> list[Path]:
    """Return the expected WSClean Stokes-I and Stokes-V image paths.

    中文：返回预期的 WSClean Stokes-I 和 Stokes-V 图像路径。
    """

    return [
        model_dir / "wsclean-MFS-I-image.fits",
        model_dir / "wsclean-MFS-V-image.fits",
    ]


def _remaining_seconds(deadline: float | None) -> float | None:
    """Return usable seconds after enforcing the configured reserve.

    中文：扣除配置的 reserve 后返回可用秒数。
    """

    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    reserve = config.WALLTIME_RESERVE_MINUTES * 60
    if remaining <= reserve + 60:
        raise NotEnoughTime("Walltime reserve reached")
    return remaining - reserve


def _command_timeout(deadline: float | None) -> int | None:
    """Convert the remaining walltime into a subprocess timeout.

    中文：把剩余 walltime 转换为子进程 timeout。
    """

    remaining = _remaining_seconds(deadline)
    if remaining is None:
        return None
    return int(remaining)


def _run_dstools(
    command: list[str],
    cwd: Path,
    log_path: Path,
    deadline: float | None,
) -> None:
    """Run one container command and append stdout/stderr to the SB log.

    中文：运行一个容器命令，并把 stdout/stderr 追加到 SB 日志。
    """

    timeout = _command_timeout(deadline)
    env = os.environ.copy()
    thread_count = str(config.CPU_PER_SLOT)
    env.update(
        {
            "OMP_NUM_THREADS": thread_count,
            "OPENBLAS_NUM_THREADS": thread_count,
            "MKL_NUM_THREADS": thread_count,
            "NUMEXPR_NUM_THREADS": thread_count,
        }
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Running in %s: %s", cwd, " ".join(command))
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"\n[{utc_now()}] {' '.join(command)}\n")
        try:
            result = subprocess.run(
                command,
                cwd=str(cwd),
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise NotEnoughTime(f"Command exceeded available walltime: {command[0]}") from exc
    if result.returncode != 0:
        raise ProcessingError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}"
        )


def _container_command(*arguments: str) -> list[str]:
    """Build an Apptainer command for the configured DStools image.

    中文：为配置的 DStools 镜像构造 Apptainer 命令。
    """

    return [config.APPTAINER_BIN, "exec", str(config.CONTAINER), *arguments]


def _proper_motion_position(source: dict[str, Any], sb_name: str, t_min_map: dict[int, float]) -> tuple[float, float]:
    """Apply the source proper motion at the effective observation epoch.

    English: `t_min_map` already combines legacy CSV values with CASDA ECSV
    values, with CASDA taking precedence. Missing finite values use MJD
    `61041.5`.

    中文：`t_min_map` 已经合并旧 CSV 和 CASDA ECSV 值，并由 CASDA 优先覆盖。缺少
    有限值时使用 MJD `61041.5`。
    """

    sb_number = sb_number_from_name(sb_name)
    t_min = t_min_map.get(sb_number, config.DEFAULT_T_MIN) if sb_number is not None else config.DEFAULT_T_MIN
    if sb_number is not None and sb_number not in t_min_map:
        logger.warning("No t_min for %s; using %.2f", sb_name, config.DEFAULT_T_MIN)
    years = (t_min - config.MJD_J2000) / 365.25
    pmra = float(source.get("pmra", 0.0) or 0.0)
    pmdec = float(source.get("pmdec", 0.0) or 0.0)
    ra = float(source["ra"]) + pmra * years / 3600000.0
    dec = float(source["dec"]) + pmdec * years / 3600000.0
    return ra, dec


def _copy_products(
    sb_dir: Path,
    product_sb_dir: Path,
    ds_path: Path,
    model_dir: Path,
) -> None:
    """Copy a validated DS and model images into an atomic product directory.

    中文：将经过验证的 DS 和 model 图像原子地复制到产物目录。
    """

    temporary = product_sb_dir.parent / (
        f".{product_sb_dir.name}.tmp-{os.getpid()}-{threading.get_ident()}"
    )
    _remove_path(temporary)
    temporary.mkdir(parents=True, exist_ok=True)
    target_ds = temporary / ds_path.name
    shutil.copy2(ds_path, target_ds)

    target_model = temporary / "wsclean_model"
    target_model.mkdir(parents=True, exist_ok=True)
    for image in _find_model_images(model_dir):
        if image.exists():
            shutil.copy2(image, target_model / image.name)

    if not target_ds.exists():
        _remove_path(temporary)
        raise ProcessingError(f"Product copy failed for {sb_dir}")
    if not (target_model / "wsclean-MFS-I-image.fits").exists():
        _remove_path(temporary)
        raise ProcessingError(f"Missing Stokes-I image after processing {sb_dir}")
    _remove_path(product_sb_dir)
    os.replace(temporary, product_sb_dir)


def process_sb(
    source: dict[str, Any],
    sb_dir: Path,
    product_sb_dir: Path,
    t_min_map: dict[int, float],
    deadline: float | None,
) -> dict[str, Any]:
    """Process one SB folder and atomically promote its final products.

    English: DStools preprocessing, model creation, subtraction, DS extraction,
    FITS cropping, and product validation are checkpoint-safe. Staging data is
    removed only after the DS and required Stokes-I image are copied.

    中文：DStools 预处理、model 创建、减源、DS 提取、FITS 裁剪和产物验证都适合
    checkpoint 恢复。只有 DS 和必需的 Stokes-I 图像复制成功后才删除 staging 数据。
    """

    started = time.monotonic()
    sb_name = sb_dir.name
    sb_log = config.LOG_ROOT / "sb" / f"{source['source_name']}_{sb_name}.log"
    product_ds = next(product_sb_dir.glob("*.ds"), None) if product_sb_dir.exists() else None
    product_i = (
        product_sb_dir / "wsclean_model" / "wsclean-MFS-I-image.fits"
        if product_sb_dir.exists()
        else None
    )
    if product_ds is not None and product_i is not None and product_i.exists():
        return {"sb": sb_name, "status": "complete", "duration_seconds": 0}

    ra_cor, dec_cor = _proper_motion_position(source, sb_name, t_min_map)
    ds_path = _find_ds(sb_dir)
    ms_path = _find_ms(sb_dir)
    model_dir = sb_dir / "wsclean_model"
    if ds_path is not None and not _valid_fits(
        model_dir / "wsclean-MFS-I-image.fits"
    ):
        logger.warning("DS exists without a valid Stokes-I image; restarting %s", sb_name)
        ds_path.unlink(missing_ok=True)
        ds_path = None
    temp_dir = config.WSCLEAN_TEMP_ROOT / f"{source['source_name']}_{sb_name}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        if ds_path is None:
            if ms_path is None:
                raise ProcessingError(f"No MS or DS found in {sb_dir}")

            # DStools' ASKAP correction is required before imaging. FIELD_OLD
            # is the marker written by fix-ms after the correction succeeds.
            # 成像前必须完成 DStools ASKAP 校正；fix-ms 成功后写入 FIELD_OLD 标记。
            if not (ms_path / "FIELD_OLD").exists():
                _run_dstools(
                    _container_command("dstools-askap-preprocess", str(ms_path)),
                    sb_dir,
                    sb_log,
                    deadline,
                )

            model_file = model_dir / "wsclean-MFS-I-model.fits"
            model_image = model_dir / "wsclean-MFS-I-image.fits"
            if not (_valid_fits(model_file) and _valid_fits(model_image)):
                _remove_path(model_dir)
                command = _container_command(
                    "dstools-create-model",
                    "-B",
                    config.BAND,
                    "-N",
                    str(config.CREATE_MODEL_ITERATIONS),
                    "-j",
                    str(config.CREATE_MODEL_THREADS),
                    "-o",
                    "wsclean_model",
                    "--temp-dir",
                    str(temp_dir),
                )
                if config.MIN_UV_METRES > 0:
                    command.extend(["--minuvw-m", str(config.MIN_UV_METRES)])
                command.append(str(ms_path))
                _run_dstools(command, sb_dir, sb_log, deadline)

            if not (_valid_fits(model_file) and _valid_fits(model_image)):
                raise ProcessingError(f"Create-model did not produce valid products in {model_dir}")

            subtracted = _find_subtracted_ms(sb_dir)
            completion_marker = sb_dir / ".subtracted.complete"
            if subtracted is not None and not completion_marker.exists():
                _remove_path(subtracted)
                subtracted = None
            if subtracted is None:
                _run_dstools(
                    _container_command(
                        "dstools-insert-model",
                        "-p",
                        f"{ra_cor:.12f}",
                        f"{dec_cor:.12f}",
                        str(model_dir),
                        str(ms_path),
                    ),
                    sb_dir,
                    sb_log,
                    deadline,
                )
                _run_dstools(
                    _container_command("dstools-subtract-model", "-S", str(ms_path)),
                    sb_dir,
                    sb_log,
                    deadline,
                )
                subtracted = _find_subtracted_ms(sb_dir)
            if subtracted is None:
                raise ProcessingError(f"No subtracted MS found in {sb_dir}")
            completion_marker.write_text("complete\n", encoding="utf-8")

            output_ds = sb_dir / f"{sb_name}.ds"
            if not output_ds.exists():
                partial_ds = sb_dir / f".{sb_name}.ds.part"
                partial_ds.unlink(missing_ok=True)
                _run_dstools(
                    _container_command(
                        "dstools-extract-ds",
                        "-u",
                        str(int(config.EXTRACT_MIN_UV_METRES)),
                        "-p",
                        f"{ra_cor:.12f}",
                        f"{dec_cor:.12f}",
                        str(subtracted),
                        str(partial_ds),
                    ),
                    sb_dir,
                    sb_log,
                    deadline,
                )
                if not _valid_ds(partial_ds):
                    partial_ds.unlink(missing_ok=True)
                    raise ProcessingError("DStools produced an invalid DS file")
                os.replace(partial_ds, output_ds)
            ds_path = _find_ds(sb_dir)

        if ds_path is None or not ds_path.exists():
            raise ProcessingError(f"DS extraction did not produce a file in {sb_dir}")

        for image in _find_model_images(model_dir):
            if image.exists():
                crop_fits(image, ra_cor, dec_cor, config.CROP_ARCMIN)
            elif image.name.endswith("MFS-I-image.fits"):
                raise ProcessingError(f"Missing required Stokes-I image: {image}")
            else:
                logger.warning("Stokes-V image is missing in %s", sb_dir)

        _copy_products(sb_dir, product_sb_dir, ds_path, model_dir)

        # Delete staging only after DS and the required image reach the
        # persistent product tree; this makes interrupted retries safe.
        # 只有 DS 和必需图像进入持久化产物树后才删除 staging，保证中断重试安全。
        _remove_path(sb_dir)
        _remove_path(temp_dir)
        return {
            "sb": sb_name,
            "status": "complete",
            "duration_seconds": round(time.monotonic() - started, 2),
        }
    except NotEnoughTime:
        logger.warning("Stopping %s because the walltime reserve was reached", sb_name)
        return {
            "sb": sb_name,
            "status": "partial",
            "duration_seconds": round(time.monotonic() - started, 2),
        }
    except Exception as exc:
        logger.exception("Processing failed for %s", sb_name)
        if _find_ms(sb_dir) is None and _find_ds(sb_dir) is None:
            # Do not let an empty/incomplete extraction directory suppress a
            # fresh CASDA download on the next job.
            # 空或不完整的解压目录不能阻止下一个 job 重新下载 CASDA 数据。
            _remove_path(sb_dir)
        return {
            "sb": sb_name,
            "status": "failed",
            "error": str(exc),
            "duration_seconds": round(time.monotonic() - started, 2),
        }


def _discover_sb_dirs(longobs: Path) -> list[Path]:
    """List extracted SB/beam directories for one source.

    中文：列出一个源已经解压出的 SB/beam 目录。
    """

    return sorted(
        path
        for path in longobs.iterdir()
        if path.is_dir() and re.fullmatch(r"SB\d+_beam\d+", path.name)
    ) if longobs.exists() else []


def _expected_numbers(source: dict[str, Any]) -> set[int]:
    """Extract expected numeric observations from a manifest source.

    中文：从 manifest source 中提取预期的数字观测编号。
    """

    return {
        number
        for value in source.get("obs_ids", [])
        if (number := obs_number(value)) is not None
    }


def _deadline_from_args(started: float, walltime_hours: float) -> float:
    """Calculate a deadline using configured time and Slurm's end timestamp.

    中文：结合配置的 walltime 和 Slurm 结束时间戳计算 deadline。
    """

    configured = started + walltime_hours * 3600
    raw_slurm_end = os.environ.get("SLURM_JOB_END_TIME")
    if raw_slurm_end:
        try:
            return min(configured, time.monotonic() + max(0, float(raw_slurm_end) - time.time()))
        except ValueError:
            pass
    return configured


def _has_time_for_wave(deadline: float, number_of_sb: int) -> bool:
    """Return whether one more SB wave fits before the reserve.

    中文：判断再运行一个 SB wave 是否能在 reserve 前完成。
    """

    remaining = deadline - time.monotonic()
    estimated = (
        np.ceil(number_of_sb / config.PARALLEL_SLOTS)
        * config.HOURS_PER_OBS
        * 3600
    )
    reserve = config.WALLTIME_RESERVE_MINUTES * 60
    return remaining > estimated + reserve


def _cleanup_source_scratch(source_name: str) -> None:
    """Remove completed source staging and temporary WSClean scratch.

    中文：删除已完成源的 staging 和 WSClean 临时 scratch。
    """

    _remove_path(config.STAGING_ROOT / source_name)
    for path in config.WSCLEAN_TEMP_ROOT.glob(f"{source_name}_*"):
        _remove_path(path)


def process_source(
    source: dict[str, Any],
    t_min_map: dict[int, float],
    deadline: float,
    allow_compute_download: bool = False,
) -> str:
    """Process one source, resuming from products and staged checkpoints.

    English: The source is decompressed on compute immediately before DStools.
    CASDA ECSV `t_min` values override legacy CSV values, and missing epochs use
    the configured fallback. A partial source is left for automatic resubmission.

    中文：在计算节点、紧邻 DStools 处理前解压当前源。CASDA ECSV 的 `t_min` 覆盖旧
    CSV，缺失 epoch 使用配置 fallback。partial 源保留状态供自动重新提交。
    """

    source_name = safe_source_name(source["source_name"])
    staging_longobs = config.STAGING_ROOT / source_name / "LongObs"
    product_longobs = source_product_root(source["ucs_number"], source_name)
    staging_longobs.mkdir(parents=True, exist_ok=True)
    product_longobs.mkdir(parents=True, exist_ok=True)

    expected = _expected_numbers(source)
    completed = completed_product_sb_numbers(product_longobs)
    if expected and expected.issubset(completed):
        _cleanup_source_scratch(source_name)
        write_source_state(source_name, status="complete", completed_sb=sorted(completed))
        return "complete"

    write_source_state(
        source_name,
        status="running",
        ucs_number=source["ucs_number"],
        expected_sb=sorted(expected),
        completed_sb=sorted(completed),
    )

    # Login preparation leaves archives untouched. Extract only this source on
    # compute immediately before its DStools work; other archives stay unopened.
    # 登录节点准备不会动 archive；计算节点只在 DStools 前解压当前源，其他 archive 保持
    # staging 且不打开。
    decompress_and_move(staging_longobs)

    # ozstar_main caches query tables. Reusing them prevents a resumed job from
    # issuing another TAP query; login preparation normally staged the raw MS.
    # query table 由 ozstar_main 缓存，恢复运行不会再次 TAP 查询；登录准备通常已放置 raw MS。
    rows = Table.read(source["query_cache"], format="ascii.ecsv")
    csv_t_min_map = t_min_map
    casda_t_min_map = read_t_min_map_from_table(rows)
    effective_t_min_map = dict(csv_t_min_map)
    effective_t_min_map.update(casda_t_min_map)
    logger.info(
        "%s: proper-motion t_min sources: CASDA=%d, legacy CSV fallback=%d, "
        "default MJD=%.1f",
        source_name,
        len(casda_t_min_map),
        len(set(csv_t_min_map).difference(casda_t_min_map)),
        config.DEFAULT_T_MIN,
    )
    client = None
    partial_seen = False
    failed_seen = False
    failed_sb: set[str] = set()
    while True:
        completed = completed_product_sb_numbers(product_longobs)
        completed_names = completed_product_sb_names(product_longobs)
        pending_dirs = [
            path
            for path in _discover_sb_dirs(staging_longobs)
            if path.name not in failed_sb
            and path.name not in completed_names
        ]
        if not pending_dirs:
            failed_numbers = {
                number
                for name in failed_sb
                if (number := sb_number_from_name(name)) is not None
            }
            pending_numbers = expected.difference(completed, failed_numbers)
            if not pending_numbers:
                break
            if not allow_compute_download:
                missing = ", ".join(
                    f"SB{number}" for number in sorted(pending_numbers)
                )
                raise ProcessingError(
                    f"{source_name}: required staged data is absent for {missing}. "
                    "Compute nodes cannot access CASDA; run ozstar_main.py "
                    "--prepare-download on a login node before submitting."
                )
            if not _has_time_for_wave(
                deadline, min(config.PARALLEL_SLOTS, len(pending_numbers))
            ):
                partial_seen = True
                break

            pending_rows = [
                index
                for index, value in enumerate(rows["obs_id"])
                if obs_number(value) in pending_numbers
            ]
            if not pending_rows:
                raise ProcessingError(
                    f"{source_name}: expected observations have no matching cached "
                    "rows; cannot verify staged data"
                )
            if client is None:
                try:
                    from .casda_query import build_casda_client, download_source_table
                except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
                    from casda_query import build_casda_client, download_source_table

                client = build_casda_client()
            batch = rows[pending_rows[: config.PARALLEL_SLOTS]]
            success, failed = download_source_table(
                client, batch, staging_longobs, source_name
            )
            logger.info(
                "%s: CASDA wave downloaded (%d successful URLs, %d failed URLs)",
                source_name,
                success,
                failed,
            )
            decompress_and_move(staging_longobs)
            if not _discover_sb_dirs(staging_longobs):
                failed_seen = True
                logger.error("%s: download wave produced no SB directory", source_name)
                break
            continue

        wave = pending_dirs[: config.PARALLEL_SLOTS]
        if not _has_time_for_wave(deadline, len(wave)):
            partial_seen = True
            break

        futures = {}
        with ThreadPoolExecutor(max_workers=config.PARALLEL_SLOTS) as executor:
            for sb_dir in wave:
                futures[executor.submit(
                    process_sb,
                    source,
                    sb_dir,
                    product_longobs / sb_dir.name,
                    effective_t_min_map,
                    deadline,
                )] = sb_dir.name
            for future in as_completed(futures):
                result = future.result()
                logger.info("%s %s: %s", source_name, result["sb"], result["status"])
                if result["status"] == "partial":
                    partial_seen = True
                elif result["status"] == "failed":
                    failed_seen = True
                    failed_sb.add(result["sb"])
                checkpoint = config.STATE_ROOT / "sources" / f"{source_name}.checkpoint.json"
                current = read_json(checkpoint, {}) or {}
                current.setdefault("sb", {})[result["sb"]] = result
                current["updated_at"] = utc_now()
                atomic_write_json(checkpoint, current)
        if partial_seen:
            break

    # Remove stale archives/checksums, but preserve incomplete SB directories
    # for the next job after a download or DStools failure.
    # 清理残留 archive/checksum，但下载或 DStools 失败时保留不完整 SB 目录供下次运行。
    for path in (staging_longobs.iterdir() if staging_longobs.exists() else []):
        if path.is_file() and (
            path.name.endswith(".checksum")
            or path.name.endswith(".tar")
            or path.name.endswith(".tar.gz")
            or path.name.endswith(".tgz")
        ):
            path.unlink()

    completed = completed_product_sb_numbers(product_longobs)
    if expected and expected.issubset(completed):
        _cleanup_source_scratch(source_name)
        write_source_state(
            source_name,
            status="complete",
            completed_sb=sorted(completed),
            failed=failed_seen,
        )
        return "complete"

    status = "partial" if partial_seen or time.monotonic() >= deadline else "failed"
    write_source_state(
        source_name,
        status=status,
        completed_sb=sorted(completed),
        expected_sb=sorted(expected),
        failed=failed_seen,
    )
    return status


def process_manifest(
    manifest: dict[str, Any], allow_compute_download: bool = False
) -> int:
    """Process manifest sources serially and return Slurm worker status.

    English: Zero means every source completed; the dedicated partial code
    means the generated sbatch wrapper should resubmit; any other non-zero
    result represents a failure.

    中文：零表示所有源完成；专用 partial code 表示生成的 sbatch wrapper 应自动
    重提交；其他非零结果表示失败。
    """

    started = time.monotonic()
    deadline = _deadline_from_args(started, float(manifest["job_walltime_hours"]))
    t_min_map = read_t_min_map(config.TIME_CSV_PATH)
    statuses: dict[str, str] = {}

    interrupted = False
    for source in manifest["sources"]:
        if time.monotonic() + config.WALLTIME_RESERVE_MINUTES * 60 >= deadline:
            logger.warning("Walltime reserve reached before the next source")
            interrupted = True
            break
        try:
            status = process_source(
                source,
                t_min_map,
                deadline,
                allow_compute_download=allow_compute_download,
            )
        except Exception as exc:
            logger.exception("Source failed: %s", source["source_name"])
            write_source_state(
                source["source_name"],
                status="failed",
                error=str(exc),
            )
            status = "failed"
        statuses[source["source_name"]] = status
        if status == "partial":
            logger.warning("Stopping job after partial source %s", source["source_name"])
            interrupted = True
            break

    manifest["finished_at"] = utc_now()
    manifest["statuses"] = statuses
    manifest_path = Path(manifest["manifest_path"])
    atomic_write_json(manifest_path, manifest)
    unprocessed = {
        source["source_name"] for source in manifest["sources"]
    }.difference(statuses)
    if not interrupted and not unprocessed and all(
        status == "complete" for status in statuses.values()
    ):
        return 0
    if not any(status == "failed" for status in statuses.values()) and (
        interrupted or any(status == "partial" for status in statuses.values())
    ):
        return config.PARTIAL_EXIT_CODE
    return 1


def main() -> None:
    """Parse worker arguments and execute one prepared manifest.

    中文：解析 worker 参数并执行一个已准备的 manifest。
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--allow-compute-download",
        action="store_true",
        help="Debugging only: attempt CASDA downloads from the compute node.",
    )
    args = parser.parse_args()

    config.ensure_directories()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    manifest = read_json(args.manifest)
    if not manifest:
        raise SystemExit(f"Manifest does not exist or is empty: {args.manifest}")
    manifest["manifest_path"] = str(args.manifest)
    raise SystemExit(
        process_manifest(
            manifest,
            allow_compute_download=args.allow_compute_download,
        )
    )


if __name__ == "__main__":
    main()
