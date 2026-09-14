"""Prepare one existing manifest on an Ozstar login node.

English: CASDA is reachable from login nodes but not from compute nodes. This
module therefore authenticates and downloads visibility tar archives before a
Slurm worker is submitted. Archives remain in staging until the compute worker
starts the corresponding source; extraction is not performed here.

中文：CASDA 可从登录节点访问，但计算节点不能访问。因此本模块在提交 Slurm worker
前完成登录和 visibility tar 下载。archive 会留在 staging，直到计算 worker 开始
对应源；本模块不执行解压。
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import numpy as np
from astropy.table import Table

try:
    from . import config
    from .casda_query import (
        build_casda_client,
        download_source_table,
        estimated_access_bytes,
    )
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
    from casda_query import build_casda_client, download_source_table, estimated_access_bytes
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


logger = logging.getLogger("askap.prepare_download")


class SourceDownloadFailed(RuntimeError):
    """Raised after all login-node download retries for one source fail."""


def _download_retry_message(source_name: str, attempt: int, error: Exception) -> None:
    logger.warning(
        "%s: download attempt %d/%d failed: %s",
        source_name,
        attempt,
        config.CASDA_SOURCE_DOWNLOAD_RETRIES,
        error,
    )


def _expected_numbers(source: dict[str, Any]) -> set[int]:
    """Return the valid SB numbers promised by one manifest source.

    中文：返回一个 manifest source 声明的有效 SB 编号集合。
    """

    values = source.get("obs_ids", [])
    if not values:
        raise ValueError(f"{source.get('source_name', '<unknown>')}: manifest has no obs_ids")
    numbers = {number for value in values if (number := obs_number(value)) is not None}
    if len(numbers) != len(values):
        raise ValueError(
            f"{source.get('source_name', '<unknown>')}: manifest contains an invalid obs_id"
        )
    return numbers


def _staged_numbers(longobs: Path) -> set[int]:
    """Find already extracted SB directories in the source staging tree.

    中文：查找源 staging 树中已经解压出的 SB 目录。
    """

    if not longobs.exists():
        return set()
    return {
        int(match.group(1))
        for path in longobs.iterdir()
        if path.is_dir()
        and (match := re.fullmatch(r"SB(\d+)_beam\d+", path.name)) is not None
    }


def _archive_numbers(longobs: Path) -> set[int]:
    """Find downloaded tar archives by the SB number in their names.

    中文：根据文件名中的 SB 编号查找已下载的 tar archive。
    """

    if not longobs.exists():
        return set()
    numbers: set[int] = set()
    for path in longobs.iterdir():
        if not path.is_file() or not path.name.lower().endswith(
            (".tar", ".tar.gz", ".tgz")
        ):
            continue
        match = re.search(r"SB(\d+)", path.name)
        if match:
            numbers.add(int(match.group(1)))
    return numbers


def _read_rows(source: dict[str, Any], expected: set[int]) -> Table:
    """Read the cached full-row ECSV and verify expected observations exist.

    English: Preparation reuses the login-node TAP cache rather than issuing a
    second query.

    中文：准备阶段复用登录节点的 TAP cache，不重复发起查询，并验证所有预期观测存在。
    """

    cache_value = source.get("query_cache")
    cache = Path(str(cache_value)).expanduser() if cache_value else Path()
    if not cache.is_file():
        raise FileNotFoundError(
            f"{source['source_name']}: query cache is missing: {cache}. "
            "Rebuild the manifest on a login node."
        )
    rows = Table.read(cache, format="ascii.ecsv")
    if "obs_id" not in rows.colnames:
        raise ValueError(f"{source['source_name']}: query cache has no obs_id column")
    cached_numbers = {
        number for value in rows["obs_id"] if (number := obs_number(value)) is not None
    }
    missing = sorted(expected.difference(cached_numbers))
    if missing:
        raise ValueError(
            f"{source['source_name']}: query cache is missing observations {missing}"
        )
    return rows


def _prepare_source_once(
    source: dict[str, Any], manifest_path: Path | None = None
) -> dict[str, Any]:
    """Download all required visibility archives for one manifest source.

    English: Only archive and checksum files are downloaded on the login node;
    this function records a marker only after all expected observations exist.

    中文：登录节点只下载 archive 和 checksum；只有所有预期观测存在后才写入 marker。
    """

    source_name = safe_source_name(str(source["source_name"]))
    expected = _expected_numbers(source)
    staging_longobs = config.STAGING_ROOT / source_name / "LongObs"
    product_longobs = source_product_root(int(source["ucs_number"]), source_name)
    staging_longobs.mkdir(parents=True, exist_ok=True)

    completed = completed_product_sb_numbers(product_longobs)
    staged = _staged_numbers(staging_longobs)
    archives = _archive_numbers(staging_longobs)
    marker = staging_longobs / ".download_complete.json"
    marker_numbers: set[int] = set()
    if marker.exists():
        try:
            marker_payload = json.loads(marker.read_text(encoding="utf-8"))
            marker_numbers = {int(value) for value in marker_payload.get("expected_sb", [])}
        except (OSError, ValueError, TypeError):
            marker_numbers = set()
    estimated_bytes = int(source.get("estimated_staging_bytes", 0) or 0)
    if estimated_bytes > config.STAGING_BUDGET_BYTES:
        raise RuntimeError(
            f"{source_name}: estimated staging size {estimated_bytes} bytes exceeds "
            f"the {config.STAGING_BUDGET_BYTES}-byte budget"
        )
    missing = expected.difference(completed | staged | archives | marker_numbers)

    if missing:
        rows = _read_rows(source, expected)
        calculated_bytes = estimated_access_bytes(rows)
        if estimated_bytes and calculated_bytes != estimated_bytes:
            raise RuntimeError(
                f"{source_name}: manifest estimate {estimated_bytes} does not match "
                f"the cached filtered-row estimate {calculated_bytes}"
            )
        pending_mask = np.asarray(
            [
                obs_number(value) not in completed | staged | archives
                for value in rows["obs_id"]
            ],
            dtype=bool,
        )
        if np.any(pending_mask):
            client = build_casda_client()
            success, failed = download_source_table(
                client,
                rows[pending_mask],
                staging_longobs,
                source_name,
            )
            logger.info(
                "%s: login-node CASDA preparation downloaded %d URLs (%d failed)",
                source_name,
                success,
                failed,
            )
            if failed == 0:
                atomic_write_json(
                    marker,
                    {
                        "expected_sb": sorted(expected),
                        "prepared_at": utc_now(),
                    },
                )
        staged = _staged_numbers(staging_longobs)
        archives = _archive_numbers(staging_longobs)
        marker_numbers = set(expected) if marker.exists() else set()
        missing = expected.difference(completed | staged | archives | marker_numbers)

    if missing:
        raise RuntimeError(
            f"{source_name}: login-node preparation is incomplete; required staged "
            f"observations are absent: {', '.join(f'SB{number}' for number in sorted(missing))}. "
            "Retry preparation after inspecting the CASDA/download log."
        )

    staged_at = utc_now()
    write_source_state(
        source_name,
        status="staged",
        ucs_number=source["ucs_number"],
        obs_count=len(source.get("obs_ids", [])),
        expected_sb=sorted(expected),
        completed_sb=sorted(completed),
        staged_sb=sorted(staged),
        archive_sb=sorted(archives),
        download_marker=marker.exists(),
        estimated_staging_bytes=estimated_bytes,
        staged_at=staged_at,
        manifest=str(manifest_path) if manifest_path else None,
    )
    return {
        "source_name": source_name,
        "staged_at": staged_at,
        "expected_sb": sorted(expected),
        "staged_sb": sorted(staged),
        "archive_sb": sorted(archives),
        "estimated_staging_bytes": estimated_bytes,
    }


def prepare_source(
    source: dict[str, Any], manifest_path: Path | None = None
) -> dict[str, Any]:
    """Retry one source download before allowing the block to continue.

    English: A fresh CASDA stage/download attempt is made up to the configured
    number of times. Partial archive files are handled by the normal staging
    checks on the next attempt.

    中文：对同一个源最多重新执行配置次数的 CASDA stage/download。每次失败后
    重新获取下载 URL；之后才允许 controller 进入下一个源。
    """

    source_name = safe_source_name(str(source["source_name"]))
    retries = max(1, config.CASDA_SOURCE_DOWNLOAD_RETRIES)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _prepare_source_once(source, manifest_path)
        except Exception as exc:
            last_error = exc
            _download_retry_message(source_name, attempt, exc)
            if attempt < retries and config.CASDA_SOURCE_RETRY_DELAY_SECONDS > 0:
                time.sleep(config.CASDA_SOURCE_RETRY_DELAY_SECONDS)
    raise SourceDownloadFailed(
        f"{source_name}: download failed after {retries} attempts: {last_error}"
    ) from last_error


def prepare_manifest(manifest_path: Path, allow_multiple: bool = False) -> dict[str, Any]:
    """Read a block manifest and download its archives on the login node.

    English: The estimated filtered/deduplicated CASDA size is checked against
    both the 2 TB staging budget and actual persistent free space before work.

    中文：开始前将过滤/去重后的 CASDA 大小估计同时与 2 TB staging 预算和持久化
    实际剩余空间比较。
    """

    manifest = read_json(manifest_path)
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError(f"Manifest does not exist or is empty: {manifest_path}")
    sources = manifest.get("sources")
    if not isinstance(sources, list):
        raise ValueError(f"Manifest has no valid sources list: {manifest_path}")

    estimated_bytes = int(manifest.get("estimated_staging_bytes", 0) or 0)
    if estimated_bytes > config.STAGING_BUDGET_BYTES:
        raise ValueError(
            f"Manifest estimated staging size {estimated_bytes} bytes exceeds the "
            f"{config.STAGING_BUDGET_BYTES}-byte budget"
        )
    staging_bytes = directory_size(config.STAGING_ROOT)
    existing_block_bytes = sum(
        directory_size(config.STAGING_ROOT / safe_source_name(str(source.get("source_name", ""))))
        for source in sources
        if isinstance(source, dict) and source.get("source_name")
    )
    additional_estimate = max(0, estimated_bytes - existing_block_bytes)
    _total, work_used, work_free = persistent_usage(config.WORK)
    if staging_bytes + additional_estimate > config.STAGING_BUDGET_BYTES:
        raise RuntimeError(
            "Refusing to add block: current staging usage "
            f"{staging_bytes} + estimated new bytes {additional_estimate} exceeds "
            f"the {config.STAGING_BUDGET_BYTES}-byte budget"
        )
    if estimated_bytes > work_free:
        raise RuntimeError(
            "Refusing to add block: estimated block size "
            f"{estimated_bytes} exceeds persistent WORK free space {work_free} "
            f"(WORK used={work_used})"
        )

    prepared_sources: list[dict[str, Any]] = []
    failed_source_names: set[str] = set()
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("Manifest contains a non-object source")
        source_name = safe_source_name(str(source.get("source_name", "")))
        try:
            result = prepare_source(source, manifest_path)
        except SourceDownloadFailed as exc:
            failed_source_names.add(source_name)
            write_source_state(source_name, status="download_failed", error=str(exc))
            logger.error("%s: skipping after download retries: %s", source_name, exc)
            continue
        except Exception as exc:
            write_source_state(source_name, status="stage_failed", error=str(exc))
            raise RuntimeError(f"{source_name}: login-node preparation failed: {exc}") from exc
        source["stage_status"] = "staged"
        source["staged_at"] = result["staged_at"]
        prepared_sources.append(source)
        atomic_write_json(manifest_path, manifest)

        current_staging = directory_size(config.STAGING_ROOT)
        if current_staging > config.STAGING_BUDGET_BYTES:
            raise RuntimeError(
                "CASDA downloads exceeded the staging budget: "
                f"{current_staging} > {config.STAGING_BUDGET_BYTES} bytes"
            )

    manifest["sources"] = prepared_sources
    for record in manifest.get("source_records", []):
        if record.get("source_name") in failed_source_names:
            record["status"] = "download_failed"
            record["reason"] = "download retries exhausted"
    manifest["download_failed_sources"] = sorted(failed_source_names)
    manifest["download_prepared_at"] = utc_now()
    manifest["download_prepared_on"] = "login"
    atomic_write_json(manifest_path, manifest)
    return manifest


def main() -> None:
    """Run explicit login-node preparation from the command line.

    中文：从命令行执行显式的登录节点准备步骤。
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--allow-multiple",
        action="store_true",
        help="Retained for compatibility; block planning now enforces the staging budget.",
    )
    args = parser.parse_args()

    config.ensure_directories()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        manifest = prepare_manifest(args.manifest, allow_multiple=args.allow_multiple)
    except Exception as exc:
        raise SystemExit(f"Download preparation failed: {exc}") from exc
    print(f"Prepared {len(manifest['sources'])} source(s) on the login node")


if __name__ == "__main__":
    main()
