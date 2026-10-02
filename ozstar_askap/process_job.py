"""Run one prepared manifest on an Ozstar compute node.

English: Persistent data remains below `ASKAP_WORK`. Each worker handles one
UCS source chunk while up to four SB folders run in parallel, with eight CPU
threads per DStools process by default. CASDA downloads are disabled because
compute nodes cannot resolve `data.csiro.au`; login preparation leaves tar
files for this worker to extract. A walltime partial result is checkpointed so
the generated sbatch wrapper can resubmit itself.

中文：持久化数据全部位于 `ASKAP_WORK` 下。每个 worker 只处理一个 UCS 源的一个
chunk，每次最多并行四个 SB 目录，默认每个 DStools 进程使用八个 CPU 线程。由于计算节点不能解析 `data.csiro.au`，
默认禁止 CASDA 下载；登录节点准备步骤会留下 tar 供 worker 解压。墙钟导致的 partial
结果会写入 checkpoint，生成的 sbatch wrapper 可以自动重新提交。
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
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
    from .sb_status import (
        StateDatabaseError,
        record_source_failure,
        record_step,
        retry_sb_names,
        retry_stage,
        set_retry_stage,
    )
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
    from sb_status import (
        StateDatabaseError,
        record_source_failure,
        record_step,
        retry_sb_names,
        retry_stage,
        set_retry_stage,
    )

logger = logging.getLogger("askap.process_job")


class NotEnoughTime(RuntimeError):
    """Signal that another subprocess would cross the walltime reserve.

    中文：表示启动另一个子进程会越过预留的 walltime 安全窗口。
    """


class ProcessingError(RuntimeError):
    """Signal that one SB cannot be completed by the worker.

    中文：表示 worker 无法完成一个 SB 的处理。
    """


def _inject_gate6_failure(
    source_name: str,
    sb_id: str,
    current_retry_stage: str,
) -> None:
    """Raise one tightly scoped, state-recorded Gate 6 validation failure.

    English: The hook is inert unless an explicit environment variable points
    to a JSON file inside this run's isolated state directory.  The JSON must
    independently name the same isolated work directory, source, SB, step and
    retry stage.  Requiring ``gate6`` and ``validation`` in the work path keeps
    an accidentally exported variable from affecting a production tree.

    中文：只有显式环境变量指向本次隔离 state 目录内的 JSON 时，该钩子才可能
    生效。JSON 还必须独立匹配同一隔离工作目录、源、SB、步骤和 retry stage。
    工作路径必须同时包含 ``gate6`` 与 ``validation``，以防误导出的变量影响生产目录。
    """

    raw_path = os.environ.get("ASKAP_GATE6_FAULT_CONFIG", "").strip()
    if not raw_path:
        return

    fault_path = Path(raw_path).expanduser().resolve()
    state_root = config.STATE_ROOT.resolve()
    work_root = config.WORK.resolve()
    try:
        fault_path.relative_to(state_root)
    except ValueError as exc:
        raise ProcessingError(
            f"Refusing Gate 6 fault config outside isolated state: {fault_path}"
        ) from exc

    lowered_work = str(work_root).lower()
    if "gate6" not in lowered_work or "validation" not in lowered_work:
        raise ProcessingError(
            f"Refusing Gate 6 fault injection in non-validation work tree: {work_root}"
        )
    if not fault_path.is_file():
        raise ProcessingError(f"Gate 6 fault config is missing: {fault_path}")

    payload = read_json(fault_path, {}) or {}
    configured_work = Path(str(payload.get("work", ""))).expanduser().resolve()
    configured_stages = {
        str(value) for value in payload.get("fail_retry_stages", [])
    }
    matches = (
        payload.get("gate") == "gate6"
        and configured_work == work_root
        and payload.get("source_name") == source_name
        and payload.get("sb_id") == sb_id
        and payload.get("fail_step") == "create_model"
        and current_retry_stage in configured_stages
    )
    if not matches:
        return

    events_path = state_root / "gate6-fault-events.json"
    events = read_json(events_path, []) or []
    if not isinstance(events, list):
        raise ProcessingError(f"Invalid Gate 6 event state: {events_path}")
    events.append(
        {
            "injected_at": utc_now(),
            "source_name": source_name,
            "sb_id": sb_id,
            "step": "create_model",
            "retry_stage": current_retry_stage,
        }
    )
    atomic_write_json(events_path, events)
    raise ProcessingError(
        "Gate 6 injected create_model failure "
        f"for {source_name}_{sb_id} at retry stage {current_retry_stage}"
    )


def _remove_path(path: Path) -> None:
    """Remove a file, directory, or symlink used by retry cleanup.

    中文：删除重试清理阶段使用的文件、目录或符号链接。
    """

    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def _find_ms(sb_dir: Path) -> Path | None:
    """Find a raw MeasurementSet by its filename.

    中文：在 SB 目录中查找有效的原始 MeasurementSet。
    """

    candidates = sorted(
        path
        for path in sb_dir.iterdir()
        if path.name.endswith(".ms")
        and not path.name.endswith(".subtracted.ms")
    )
    return candidates[0] if candidates else None


def _find_subtracted_ms(sb_dir: Path) -> Path | None:
    """Find a DStools-subtracted MeasurementSet in an SB directory.

    中文：在 SB 目录中查找 DStools 生成的减源 MeasurementSet。
    """

    candidates = sorted(sb_dir.glob("*.subtracted.ms"))
    return candidates[0] if candidates else None


def _find_ds(sb_dir: Path) -> Path | None:
    """Return the first DS path by filename only.

    中文：只按文件名返回第一个 DS 路径，不读取文件内容。
    """

    return next(iter(sorted(sb_dir.glob("*.ds"))), None)


def _find_model_images(model_dir: Path) -> list[Path]:
    """Return the expected WSClean Stokes-I and Stokes-V image paths.

    中文：返回预期的 WSClean Stokes-I 和 Stokes-V 图像路径。
    """

    return [
        model_dir / "wsclean-MFS-I-image.fits",
        model_dir / "wsclean-MFS-V-image.fits",
    ]


def _clear_retry_outputs(
    sb_dir: Path,
    product_sb_dir: Path,
    model_dir: Path,
    temp_dir: Path,
) -> None:
    """Remove every product derived after preprocess, but preserve the raw MS."""

    _remove_path(model_dir)
    _remove_path(temp_dir)
    _remove_path(product_sb_dir)
    for path in sorted(sb_dir.glob("*.subtracted.ms")):
        _remove_path(path)
    for path in sorted(sb_dir.glob("*.ds")):
        _remove_path(path)


def _discard_sb_for_redownload(
    source_name: str,
    sb_dir: Path,
    product_sb_dir: Path,
    temp_dir: Path,
) -> None:
    """Delete one failed SB and its download evidence to force one fresh fetch."""

    sb_number = sb_number_from_name(sb_dir.name)
    if sb_number is None:
        raise ProcessingError(f"Cannot identify SB number for retry cleanup: {sb_dir}")
    longobs = sb_dir.parent
    _remove_path(sb_dir)
    _remove_path(product_sb_dir)
    _remove_path(temp_dir)
    sb_pattern = re.compile(rf"SB{sb_number}(?:\D|$)", flags=re.IGNORECASE)
    for path in list(longobs.iterdir()):
        if not path.is_file() or not sb_pattern.search(path.name):
            continue
        lower = path.name.lower()
        if lower.endswith((".checksum", ".tar", ".tar.gz", ".tgz", ".part")):
            path.unlink()
    # This marker covers the entire source and would otherwise make a deleted
    # SB look prepared during the next explicit login-node retry.
    (longobs / ".download_complete.json").unlink(missing_ok=True)
    logger.warning(
        "%s %s: removed MS/checksum after failed processing retry; "
        "one fresh CASDA download is required",
        source_name,
        sb_dir.name,
    )


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
            # WSClean deliberately refuses to run when linked OpenBLAS is
            # allowed to create multiple threads.  This is independent of
            # WSClean's own ``-j`` worker count, which remains CPU_PER_SLOT.
            # WSClean 在 OpenBLAS 多线程开启时会直接退出；这不影响它自身
            # 由 ``-j`` 控制的并行线程数。
            "OPENBLAS_NUM_THREADS": "1",
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
    """Build an isolated Apptainer command for the configured DStools image.

    English: Host compiler/Python modules must not leak their library paths
    into the self-contained WSClean/casacore image.  Only the explicitly
    selected thread controls and a writable Matplotlib cache are passed in.

    中文：为配置的 DStools 镜像构造隔离的 Apptainer 命令。宿主机编译器/
    Python module 的库路径不得泄漏进自包含的 WSClean/casacore 镜像。
    """

    thread_count = str(config.CPU_PER_SLOT)
    scratch_root = os.environ.get("JOBFS") or os.environ.get("SLURM_TMPDIR")
    if scratch_root:
        matplotlib_dir = Path(scratch_root) / "matplotlib"
    else:
        job_tag = os.environ.get("SLURM_JOB_ID", str(os.getpid()))
        matplotlib_dir = config.WSCLEAN_TEMP_ROOT / f".matplotlib-{job_tag}"
    matplotlib_dir.mkdir(parents=True, exist_ok=True)

    command = [config.APPTAINER_BIN, "exec", "--cleanenv", "--no-home"]
    if config.APPTAINER_BIND.strip():
        command.extend(["--bind", config.APPTAINER_BIND])
    for name, value in (
        ("MPLCONFIGDIR", str(matplotlib_dir)),
        ("OMP_NUM_THREADS", thread_count),
        # Override the image's environment explicitly: Apptainer ``--env``
        # otherwise allowed the pipeline's eight-thread value to replace the
        # safe OPENBLAS_NUM_THREADS=1 baked into the candidate SIF.
        ("OPENBLAS_NUM_THREADS", "1"),
        ("MKL_NUM_THREADS", thread_count),
        ("NUMEXPR_NUM_THREADS", thread_count),
    ):
        command.extend(["--env", f"{name}={value}"])
    for name in ("SLURM_CPUS_PER_TASK", "SLURM_MEM_PER_NODE"):
        value = os.environ.get(name)
        if value:
            command.extend(["--env", f"{name}={value}"])
    command.extend([str(config.CONTAINER), *arguments])
    return command


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
    """Copy named DS and Stokes-I/V products into the product directory.

    中文：将按文件名验证的 DS 与 Stokes-I/V 图像复制到产物目录。
    """

    if not ds_path.is_file():
        raise ProcessingError(f"Missing DS before product copy: {ds_path}")
    required_images = _find_model_images(model_dir)
    missing_images = [image for image in required_images if not image.is_file()]
    if missing_images:
        raise ProcessingError(
            "Missing required image(s) before product copy: "
            + ", ".join(str(image) for image in missing_images)
        )

    # Do not touch an existing persistent product directory until every named
    # source product needed for a safe replacement is present.
    _remove_path(product_sb_dir)
    product_sb_dir.mkdir(parents=True, exist_ok=True)
    target_ds = product_sb_dir / ds_path.name
    shutil.copy2(ds_path, target_ds)

    target_model = product_sb_dir / "wsclean_model"
    target_model.mkdir(parents=True, exist_ok=True)
    for image in required_images:
        shutil.copy2(image, target_model / image.name)

    if not target_ds.exists():
        raise ProcessingError(f"Product copy failed for {sb_dir}")
    for stokes in ("I", "V"):
        image = target_model / f"wsclean-MFS-{stokes}-image.fits"
        if not image.is_file():
            raise ProcessingError(
                f"Missing Stokes-{stokes} image after processing {sb_dir}"
            )


def process_sb(
    source: dict[str, Any],
    sb_dir: Path,
    product_sb_dir: Path,
    t_min_map: dict[int, float],
    deadline: float | None,
) -> dict[str, Any]:
    """Process one SB folder using filename-based product checks.

    English: DStools preprocessing, model creation, subtraction, DS extraction,
    FITS cropping, and named-product checks are checkpoint-safe. Staging data is
    removed only after the DS and required Stokes-I/V images are copied.

    中文：DStools 预处理、model 创建、减源、DS 提取、FITS 裁剪和产物验证都适合
    checkpoint 恢复。只有 DS 和必需的 Stokes-I/V 文件名复制成功后才删除 staging 数据。
    """

    started = time.monotonic()
    sb_name = sb_dir.name
    sb_id = f"SB{sb_number_from_name(sb_name)}"
    beam_match = re.search(r"_(beam\d+)$", sb_name)
    beam = beam_match.group(1) if beam_match else None
    sb_log = config.LOG_ROOT / ".sb_tmp" / f"{source['source_name']}_{sb_name}.log"
    active_step = "worker_setup"

    def record(step: str, status: str, *, seconds: float | None = None, error: str | None = None,
               log: Path | None = None) -> None:
        record_step(source["source_name"], sb_id, step, status, beam=beam,
                    seconds=seconds, error=error, failure_log_path=log)

    def run_step(step: str, command: list[str]) -> None:
        nonlocal active_step
        active_step = step
        step_started = time.monotonic()
        try:
            _run_dstools(command, sb_dir, sb_log, deadline)
        except Exception as exc:
            record(step, "N", error=str(exc))
            raise
        record(step, "Y", seconds=time.monotonic() - step_started)

    def retain_failure_log() -> Path | None:
        if not sb_log.exists():
            return None
        stage = retry_stage(source["source_name"], sb_id, beam=beam)
        stamp = utc_now().replace(":", "").replace("+", "_")
        failure_log = (
            config.LOG_ROOT
            / "failures"
            / f"{source['source_name']}_{sb_name}.{stage}.{stamp}.log"
        )
        failure_log.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(sb_log), str(failure_log))
        return failure_log
    product_ds = next(product_sb_dir.glob("*.ds"), None) if product_sb_dir.exists() else None
    product_i = (
        product_sb_dir / "wsclean_model" / "wsclean-MFS-I-image.fits"
        if product_sb_dir.exists()
        else None
    )
    product_v = (
        product_sb_dir / "wsclean_model" / "wsclean-MFS-V-image.fits"
        if product_sb_dir.exists()
        else None
    )
    record("worker_setup", "Y")
    if (
        product_ds is not None
        and product_i is not None
        and product_i.is_file()
        and product_v is not None
        and product_v.is_file()
    ):
        record("product_copy", "Y", seconds=0)
        set_retry_stage(
            source["source_name"], sb_id, "complete", beam=beam
        )
        return {"sb": sb_name, "status": "complete", "duration_seconds": 0}

    ra_cor, dec_cor = _proper_motion_position(source, sb_name, t_min_map)
    model_dir = sb_dir / "wsclean_model"
    temp_dir = config.WSCLEAN_TEMP_ROOT / f"{source['source_name']}_{sb_name}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    current_retry_stage = retry_stage(source["source_name"], sb_id, beam=beam)

    ms_path = _find_ms(sb_dir)
    try:
        if current_retry_stage in {
            "processing_retry_ready",
            "redownload_retry_ready",
        }:
            _clear_retry_outputs(sb_dir, product_sb_dir, model_dir, temp_dir)
            temp_dir.mkdir(parents=True, exist_ok=True)

        if current_retry_stage == "processing_retry_ready" and ms_path is not None:
            active_step = "preprocess"
            _run_dstools(
                _container_command(
                    "/opt/askap/bin/python",
                    str(config.PROGRAM_DIR / "reset_askap_ms.py"),
                    str(ms_path),
                ),
                sb_dir,
                sb_log,
                deadline,
            )

        ds_path = _find_ds(sb_dir)
        if ds_path is None:
            if ms_path is None:
                raise ProcessingError(f"No MS or DS found in {sb_dir}")

            # DStools exits non-zero when FIELD_OLD already exists, so use the
            # marker written by successful preprocessing to make retries safe.
            # DStools 发现 FIELD_OLD 时会以非零状态退出，因此用成功预处理写入的
            # 标记跳过重复调用，保证重试安全。
            if not (ms_path / "FIELD_OLD").exists():
                run_step("preprocess",
                    _container_command("dstools-askap-preprocess", str(ms_path)),
                )
            else:
                record("preprocess", "Y", seconds=0)

            model_file = model_dir / "wsclean-MFS-I-model.fits"
            model_image = model_dir / "wsclean-MFS-I-image.fits"
            model_v_image = model_dir / "wsclean-MFS-V-image.fits"
            if not (
                model_file.is_file()
                and model_image.is_file()
                and model_v_image.is_file()
            ):
                _remove_path(model_dir)
                active_step = "create_model"
                _inject_gate6_failure(
                    source["source_name"], sb_id, current_retry_stage
                )
                command = _container_command(
                    "dstools-create-model",
                    "-v",
                    "-B",
                    config.BAND,
                    "-N",
                    str(config.CREATE_MODEL_ITERATIONS),
                    "-j",
                    str(config.CREATE_MODEL_THREADS),
                )
                if config.MIN_UV_METRES > 0:
                    command.extend(["--minuvw-m", str(config.MIN_UV_METRES)])
                if config.CREATE_MODEL_MULTISCALE:
                    command.append("-S")
                command.extend(
                    [
                        "--temp-dir",
                        str(temp_dir),
                    ]
                )
                command.append(str(ms_path))
                run_step("create_model", command)
            else:
                record("create_model", "Y", seconds=0)

            if not (
                model_file.is_file()
                and model_image.is_file()
                and model_v_image.is_file()
            ):
                raise ProcessingError(
                    "Create-model did not produce the required Stokes-I model "
                    f"and Stokes-I/V images in {model_dir}"
                )

            subtracted = _find_subtracted_ms(sb_dir)
            if subtracted is None:
                run_step("insert_model",
                    _container_command(
                        "dstools-insert-model",
                        "-p",
                        f"{ra_cor:.12f}",
                        f"{dec_cor:.12f}",
                        str(model_dir),
                        str(ms_path),
                    ),
                )
                run_step("subtract_model",
                    _container_command("dstools-subtract-model", "-S", str(ms_path)),
                )
                subtracted = _find_subtracted_ms(sb_dir)
            else:
                record("insert_model", "Y", seconds=0)
                record("subtract_model", "Y", seconds=0)
            if subtracted is None:
                raise ProcessingError(f"No subtracted MS found in {sb_dir}")

            output_ds = sb_dir / f"{sb_name}.ds"
            if not output_ds.exists():
                run_step("extract_ds",
                    _container_command(
                        "dstools-extract-ds",
                        "-u",
                        str(int(config.EXTRACT_MIN_UV_METRES)),
                        "-p",
                        f"{ra_cor:.12f}",
                        f"{dec_cor:.12f}",
                        str(subtracted),
                        str(output_ds),
                    ),
                )
                if not output_ds.exists():
                    raise ProcessingError("DStools did not produce a DS filename")
            else:
                record("extract_ds", "Y", seconds=0)
            ds_path = _find_ds(sb_dir)

        if ds_path is None or not ds_path.exists():
            raise ProcessingError(f"DS extraction did not produce a file in {sb_dir}")

        for image in _find_model_images(model_dir):
            stokes = "I" if "MFS-I" in image.name else "V"
            if not image.is_file():
                raise ProcessingError(
                    f"Missing required Stokes-{stokes} image: {image}"
                )
            crop_started = time.monotonic()
            crop_fits(image, ra_cor, dec_cor, config.CROP_ARCMIN)
            record(
                "crop_i" if stokes == "I" else "crop_v",
                "Y",
                seconds=time.monotonic() - crop_started,
            )

        active_step = "product_copy"
        copy_started = time.monotonic()
        _copy_products(sb_dir, product_sb_dir, ds_path, model_dir)
        record("product_copy", "Y", seconds=time.monotonic() - copy_started)

        # Delete this SB's staging only after DS plus both Stokes-I and
        # Stokes-V images reach the persistent product tree.
        # 只有 DS、Stokes-I 和 Stokes-V 都进入持久化产物树后，才删除该
        # SB 的 staging 数据。
        active_step = "cleanup"
        cleanup_started = time.monotonic()
        _remove_path(sb_dir)
        _remove_path(temp_dir)
        record("cleanup", "Y", seconds=time.monotonic() - cleanup_started)
        if sb_log.exists():
            sb_log.unlink()
        set_retry_stage(
            source["source_name"],
            sb_id,
            "complete",
            beam=beam,
        )
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
        if isinstance(exc, StateDatabaseError):
            raise
        logger.exception("Processing failed for %s", sb_name)
        retained = retain_failure_log()
        record(active_step, "N", error=str(exc), log=retained)
        current_retry_stage = retry_stage(
            source["source_name"], sb_id, beam=beam
        )
        if current_retry_stage == "processing_retry_ready":
            _discard_sb_for_redownload(
                source["source_name"],
                sb_dir,
                product_sb_dir,
                temp_dir,
            )
            set_retry_stage(
                source["source_name"],
                sb_id,
                "redownload_required",
                beam=beam,
            )
        elif current_retry_stage == "redownload_retry_ready":
            set_retry_stage(
                source["source_name"],
                sb_id,
                "exhausted",
                beam=beam,
            )
        else:
            set_retry_stage(
                source["source_name"],
                sb_id,
                "processing_failed",
                beam=beam,
            )
        # Preserve every failed SB directory and all of its diagnostic/input
        # files.  A retry or a human investigation must not require recovery
        # from an eager source-level cleanup.
        # 保留失败 SB 目录中的所有输入与诊断文件；未生成完整产物时
        # 不执行数据清理。
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

    # Login preparation leaves archives untouched.  Extract at most one
    # walltime-sized source chunk; remaining archives wait for the next
    # self-resubmitted sbatch.
    # 登录节点不会动 archive；每次只解压一个墙钟安全的 source chunk，其余 archive
    # 留给下一次自动续投的 sbatch。
    decompress_and_move(staging_longobs, config.MAX_MS_PER_JOB)

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
    exhausted_sb = retry_sb_names(source_name, {"exhausted"})
    failed_seen = bool(exhausted_sb)
    failed_sb: set[str] = set(exhausted_sb)
    processed_this_job = 0
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
            archives_remain = any(
                path.is_file() and path.name.endswith((".tar", ".tar.gz", ".tgz"))
                for path in staging_longobs.iterdir()
            )
            if archives_remain:
                partial_seen = True
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

        remaining_chunk_slots = config.MAX_MS_PER_JOB - processed_this_job
        if remaining_chunk_slots <= 0:
            partial_seen = True
            break
        wave = pending_dirs[: min(config.PARALLEL_SLOTS, remaining_chunk_slots)]
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
                processed_this_job += 1
        if partial_seen:
            break

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

    if not partial_seen and time.monotonic() < deadline:
        # Individual failures are already durable in sb_processing.sqlite and
        # their command logs are retained under logs/failures.  Do not let one
        # bad SB pin a high-volume sequential controller on this source, but
        # preserve failed staging for diagnosis and an explicit retry.
        write_source_state(
            source_name,
            status="complete_with_failures",
            completed_sb=sorted(completed),
            expected_sb=sorted(expected),
            failed=True,
        )
        return "complete_with_failures"

    status = "partial"
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
    """Process exactly one source manifest and return Slurm worker status.

    English: Zero means every source completed; the dedicated partial code
    means the generated sbatch wrapper should resubmit; any other non-zero
    result represents a failure.

    中文：零表示所有源完成；专用 partial code 表示生成的 sbatch wrapper 应自动
    重提交；其他非零结果表示失败。
    """

    if len(manifest.get("sources", [])) > 1:
        raise ValueError("A compute worker manifest must contain at most one UCS source")

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
        except StateDatabaseError:
            # Continuing without the central record would make the batch
            # scientifically untraceable, so this is the sole hard failure.
            raise
        except Exception as exc:
            logger.exception("Source failed: %s", source["source_name"])
            record_source_failure(source, str(exc))
            # A source-level failure is not proof of complete science products;
            # retain all staging inputs for diagnosis and explicit retry.
            write_source_state(
                source["source_name"],
                status="complete_with_failures",
                error=str(exc),
            )
            status = "complete_with_failures"
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
        status in {"complete", "complete_with_failures"} for status in statuses.values()
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
    required_fields = {"job_walltime_hours", "sources", "manifest_path"}
    missing_fields = sorted(required_fields.difference(manifest))
    if missing_fields:
        raise SystemExit(
            f"Manifest is incompatible with this worker; missing fields: "
            f"{', '.join(missing_fields)}. Rebuild the manifest on the login node."
        )
    manifest["manifest_path"] = str(args.manifest)
    raise SystemExit(
        process_manifest(
            manifest,
            allow_compute_download=args.allow_compute_download,
        )
    )


if __name__ == "__main__":
    main()
