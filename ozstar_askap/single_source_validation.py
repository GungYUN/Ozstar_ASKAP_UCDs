#!/usr/bin/env python3
"""Prepare and submit a one-source, short walltime validation job.

English: This diagnostic is intentionally separate from the production range
controller. It prepares exactly one UCS source on the login node, then creates
a two-hour Slurm worker using 16 CPUs as two 8-thread DStools slots. It writes a
dedicated validation log and never modifies the original ``ozstar_askap``
directory. The default worker uses one 8-thread DStools slot.

中文：本诊断程序独立于生产范围 controller。它只准备一个 UCS 源，然后生成一个
2 小时、8 CPU（一个 8 线程 DStools slot）的 Slurm worker。它写入独立的验证日志，
不会修改原始 `ozstar_askap` 目录。
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ucs", type=int, required=True, help="One UCS number to validate.")
    parser.add_argument("--walltime-hours", type=float, default=2.0)
    parser.add_argument(
        "--hours-per-obs",
        type=float,
        default=0.5,
        help="Test-only time estimate used to allow a short validation wave.",
    )
    parser.add_argument("--cpus", type=int, default=8)
    parser.add_argument("--slots", type=int, default=1)
    parser.add_argument("--threads-per-slot", type=int, default=8)
    parser.add_argument("--memory", default="32G")
    parser.add_argument("--max-resubmits", type=int, default=0)
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--submit", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.cpus != args.slots * args.threads_per_slot:
        raise SystemExit("--cpus must equal --slots * --threads-per-slot")
    if args.walltime_hours <= 0 or args.hours_per_obs <= 0 or args.max_resubmits < 0:
        raise SystemExit("walltime/hours-per-obs must be positive; max-resubmits non-negative")

    # Set these before importing the pipeline modules because config.py reads
    # environment variables at import time.
    # 在导入流水线模块前设置变量，因为 config.py 在 import 时读取环境变量。
    os.environ["ASKAP_JOB_WALLTIME_HOURS"] = str(args.walltime_hours)
    os.environ["ASKAP_HOURS_PER_OBS"] = str(args.hours_per_obs)
    os.environ["ASKAP_PARALLEL_SLOTS"] = str(args.slots)
    os.environ["ASKAP_CPU_PER_SLOT"] = str(args.threads_per_slot)
    os.environ["ASKAP_CREATE_MODEL_THREADS"] = str(args.threads_per_slot)
    os.environ["ASKAP_SBATCH_MEM"] = str(args.memory)
    os.environ["ASKAP_MAX_SOURCES_PER_JOB"] = "1"

    try:
        from . import config
        from .ozstar_main import (
            _job_tag,
            _submit,
            _write_sbatch,
            build_manifest,
        )
        from .pipeline_utils import atomic_write_json, utc_now
        from .prepare_download import prepare_manifest
    except ImportError:  # Script execution from the deployed backup directory.
        import config
        from ozstar_main import _job_tag, _submit, _write_sbatch, build_manifest
        from pipeline_utils import atomic_write_json, utc_now
        from prepare_download import prepare_manifest

    config.ensure_directories()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = config.LOG_ROOT / f"validation-UCS{args.ucs}-{timestamp}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger("askap.single_source_validation")

    logger.info("Validation source: UCS%s", args.ucs)
    logger.info(
        "Walltime: %.2f h; estimate: %.2f h/obs; CPUs: %d; slots: %d x %d",
        args.walltime_hours,
        args.hours_per_obs,
        args.cpus,
        args.slots,
        args.threads_per_slot,
    )
    logger.info("Memory: %s; max automatic resubmits: %d", args.memory, args.max_resubmits)
    logger.info("Checks: login-node archive preparation, manifest fields, compute startup, DStools and product/checkpoint creation")

    manifest = build_manifest(
        args.ucs,
        args.ucs,
        retry_existing=True,
        refresh_cache=args.refresh_cache,
    )
    if len(manifest.get("sources", [])) != 1:
        raise SystemExit(
            f"Expected exactly one eligible source, found {len(manifest.get('sources', []))}"
        )

    # The worker deadline must match the actual validation SBATCH limit.  The
    # production estimator also includes per-source overhead, which is useful
    # for normal submissions but must not make a two-hour validation worker
    # believe that it owns more walltime than Slurm granted it.
    # worker 的内部 deadline 必须与验证作业实际申请的 SBATCH 时间一致。生产估时还
    # 包含 source overhead，但不能因此让两小时验证 worker 误以为自己拥有更长墙钟。
    manifest["job_walltime_hours"] = float(args.walltime_hours)

    job_tag = f"validation-{_job_tag(args.ucs, args.ucs)}"
    manifest_path = config.STATE_ROOT / "jobs" / f"{job_tag}.json"
    manifest["manifest_path"] = str(manifest_path)
    manifest["validation"] = {
        "walltime_hours": args.walltime_hours,
        "cpus": args.cpus,
        "slots": args.slots,
        "threads_per_slot": args.threads_per_slot,
        "created_at": utc_now(),
    }
    atomic_write_json(manifest_path, manifest)
    logger.info("Manifest written: %s", manifest_path)

    # Download/extract preparation happens on the login node. The backup
    # production code leaves archives for the compute worker to extract.
    # 下载/准备发生在登录节点；backup 生产代码把 archive 留给计算节点解压。
    prepared = prepare_manifest(manifest_path, allow_multiple=False)
    if len(prepared.get("sources", [])) != 1:
        raise SystemExit("The validation source could not be prepared successfully")
    logger.info("Login-node archive preparation completed")

    script_path = _write_sbatch(
        manifest_path,
        job_tag,
        f"T{args.ucs}",
        allow_compute_download=False,
        max_resubmits=args.max_resubmits,
    )
    logger.info("SBATCH written: %s", script_path)
    print(f"VALIDATION_LOG={log_path}")
    print(f"VALIDATION_MANIFEST={manifest_path}")
    print(f"VALIDATION_SBATCH={script_path}")

    if args.submit:
        job_id = _submit(script_path)
        logger.info("Submitted validation Slurm job: %s", job_id)
        print(f"VALIDATION_JOB_ID={job_id}")
    else:
        logger.info("Preparation only; use sbatch %s to submit", script_path)


if __name__ == "__main__":
    main()
