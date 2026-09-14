"""CASDA TAP selection and per-source visibility download helpers.

English: This module performs login-node TAP filtering/deduplication, preserves
full returned rows in ECSV, obtains credentials only at runtime, and downloads
only CASDA visibility archives and checksum files. It does not extract data.

中文：本模块负责登录节点的 TAP 筛选/去重、将返回的完整行保存为 ECSV、只在运行时
读取凭据，并且只下载 CASDA visibility archive 和 checksum 文件。本模块不解压数据。
"""

from __future__ import annotations

import logging
import os
import re
import stat
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table

try:
    from . import config
    from .pipeline_utils import obs_number, safe_source_name
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config
    from pipeline_utils import obs_number, safe_source_name

logger = logging.getLogger(__name__)

TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"


def _text(value: Any) -> str:
    """Convert a CASDA table value to text without exposing credentials.

    中文：把 CASDA 表值转换为文本，不接触或暴露凭据。
    """

    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def _url_basename(url: str) -> str:
    """Return only the filename component of a CASDA URL.

    中文：只返回 CASDA URL 的文件名部分。
    """

    return Path(urlparse(url).path).name


def _read_password(path: Path, env_name: str) -> str | None:
    """Read a runtime password from the environment or a mode-600 file.

    English: The environment variable is a one-process override; an insecure
    or symlinked file is rejected.

    中文：环境变量是当前进程的覆盖值；不安全或符号链接密码文件会被拒绝。
    """

    value = os.environ.get(env_name)
    if value:
        return value.rstrip("\r\n")
    if path.exists():
        if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) & 0o077:
            raise RuntimeError(
                f"Refusing insecure CASDA password file {path}; use mode 600"
            )
        return path.read_text(encoding="utf-8").strip()
    return None


def _safe_float(value: Any) -> float | None:
    """Convert a possibly masked table value to a finite candidate float.

    中文：把可能被 mask 的表值转换成候选浮点数。
    """

    try:
        if np.ma.is_masked(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def estimated_access_bytes(rows: Table) -> int:
    """Sum filtered, deduplicated CASDA `access_estsize` values in bytes.

    English: CASDA reports the source values in KB; conversion uses 1024 bytes
    per KB after all filtering and nearest-row deduplication.

    中文：CASDA 源值单位是 KB；在完成全部筛选和最近行去重后，按每 KB 1024 bytes
    转换并求和。
    """

    if "access_estsize" not in rows.colnames:
        raise ValueError("CASDA query rows have no access_estsize column")
    total_kb = 0.0
    for value in rows["access_estsize"]:
        size_kb = _safe_float(value)
        if size_kb is None or not np.isfinite(size_kb) or size_kb < 0:
            raise ValueError(f"Invalid CASDA access_estsize value: {value!r}")
        total_kb += size_kb
    return int(np.ceil(total_kb * 1024.0))


def _redact_url(url: str) -> str:
    """Remove query/fragment credentials or tokens from a logged URL.

    中文：从日志 URL 中移除 query/fragment 里的凭据或 token。
    """

    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _filter_and_deduplicate(rows: Table, ra_deg: float, dec_deg: float) -> Table:
    """Apply the source/SB filters used by the ada downloader.

    English: Keep every column returned by TAP, select the nearest row for each
    observation, and fail closed if CASDA release filtering is unavailable.

    中文：保留 TAP 返回的每一列，为每个观测选择最近的一行；CASDA release 筛选不可用
    时安全失败，不静默放行未发布观测。
    """

    if len(rows) == 0:
        return rows
    if "access_estsize" not in rows.colnames:
        raise ValueError("CASDA TAP response has no access_estsize column")

    keep: list[bool] = []
    for row in rows:
        quality = _text(row["quality_level"]).strip()
        sb = obs_number(row["obs_id"])
        size_kb = _safe_float(row["access_estsize"])
        keep.append(
            quality in {"GOOD", "UNCERTAIN"}
            and sb is not None
            and sb >= 10000
            and sb != 65478
            and size_kb is not None
            and size_kb <= config.CASDA_MAX_FILE_SIZE_KB
        )

    filtered = rows[np.asarray(keep, dtype=bool)]
    if len(filtered) == 0:
        return filtered

    valid_coordinates = np.isfinite(np.asarray(filtered["s_ra"], dtype=float)) & np.isfinite(
        np.asarray(filtered["s_dec"], dtype=float)
    )
    filtered = filtered[valid_coordinates]
    if len(filtered) == 0:
        return filtered

    target = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame="icrs")
    coordinates = SkyCoord(
        ra=np.asarray(filtered["s_ra"], dtype=float) * u.deg,
        dec=np.asarray(filtered["s_dec"], dtype=float) * u.deg,
        frame="icrs",
    )
    separations = target.separation(coordinates).arcsec

    best_index: dict[str, int] = {}
    for index, row in enumerate(filtered):
        obs_id = _text(row["obs_id"])
        if obs_id not in best_index or separations[index] < separations[best_index[obs_id]]:
            best_index[obs_id] = index

    # Keep every TAP column, including access_estsize and optional t_min/t_max.
    # 保留 TAP 的所有列，包括 access_estsize 以及可选的 t_min/t_max。
    result = Table(filtered[sorted(best_index.values())], copy=True)
    try:
        # Fail closed so unavailable release checks cannot pass unreleased data.
        # 必须安全失败，避免 release 检查不可用时静默包含未发布观测。
        result = __import__("astroquery.casda", fromlist=["Casda"]).Casda.filter_out_unreleased(result)
        result = Table(result, copy=True)
    except Exception as exc:
        raise RuntimeError("CASDA release filtering failed") from exc
    return result


def query_source_table(ra_deg: float, dec_deg: float) -> Table:
    """Query and filter CASDA visibility rows for one target.

    中文：为一个目标查询并筛选 CASDA visibility 行。
    """

    if not np.isfinite(ra_deg) or not np.isfinite(dec_deg):
        raise ValueError("Source coordinates must be finite")
    if not 0 <= ra_deg < 360 or not -90 <= dec_deg <= 90:
        raise ValueError(f"Invalid source coordinates: RA={ra_deg}, Dec={dec_deg}")

    from astroquery.utils.tap.core import TapPlus

    tap = TapPlus(url=TAP_URL)
    query = (
        f"SELECT TOP {config.CASDA_QUERY_LIMIT} * FROM ivoa.obscore "
        "WHERE dataproduct_type = 'visibility' "
        "AND t_exptime > 500 "
        f"AND 1 = CONTAINS(POINT('ICRS',{ra_deg},{dec_deg}), "
        "circle('ICRS', s_ra, s_dec, 0.5))"
    )
    job = tap.launch_job_async(query)
    rows = job.get_results()
    return _filter_and_deduplicate(rows, ra_deg, dec_deg)


def cache_path(source_name: str) -> Path:
    """Return the safe persistent ECSV cache path for one source.

    中文：返回一个源对应的安全持久化 ECSV cache 路径。
    """

    return config.STATE_ROOT / "tap_cache" / f"{safe_source_name(source_name)}.ecsv"


def read_or_query_source(
    source_name: str,
    ra_deg: float,
    dec_deg: float,
    refresh: bool = False,
) -> tuple[Table, Path]:
    """Read a valid full-row cache or query TAP and atomically write one.

    中文：读取有效的完整行 cache；若需要则查询 TAP 并原子写入 cache。
    """

    path = cache_path(source_name)
    if path.exists() and not refresh:
        cached = Table.read(path, format="ascii.ecsv")
        if "access_estsize" in cached.colnames:
            return cached, path
        logger.warning("Ignoring stale TAP cache without access_estsize: %s", path)
    rows = query_source_table(ra_deg, dec_deg)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    rows.write(temporary, format="ascii.ecsv", overwrite=True)
    os.replace(temporary, path)
    return rows, path


def build_casda_client():
    """Create a non-interactive CASDA client for batch downloads.

    English: A protected runtime password is exposed only through an in-memory
    keyring backend; it is not written to persistent keyring storage.

    中文：受保护的运行时密码只通过内存 keyring backend 暴露，不写入持久化 keyring。
    """

    from astroquery.casda import Casda
    from keyring.backend import KeyringBackend
    import keyring

    class _RuntimeKeyring(KeyringBackend):
        """Expose the protected batch password only for this Python process.

        中文：只在当前 Python 进程中提供受保护的批处理密码。
        """

        priority = 1

        def __init__(self, username: str, password: str):
            self.username = username
            self.password = password

        def get_password(self, service: str, username: str) -> str | None:
            if service == "astroquery:casda.csiro.au" and username == self.username:
                return self.password
            return None

        def set_password(self, service: str, username: str, password: str) -> None:
            return None

        def delete_password(self, service: str, username: str) -> None:
            return None

    password = _read_password(config.CASDA_PASSWORD_FILE, config.CASDA_PASSWORD_ENV)
    if not password:
        raise RuntimeError(
            "CASDA credentials are missing. Set CASDA_PASSWORD or create "
            f"a protected password file at {config.CASDA_PASSWORD_FILE}."
        )
    # Current astroquery.casda reads keyring credentials and rejects the older
    # password= keyword. Keep the file value in an in-memory backend only.
    # 当前 astroquery.casda 从 keyring 读取凭据并拒绝旧的 password= 参数；只使用内存
    # backend，避免把密码文件复制到持久化 keyring。
    keyring.set_keyring(_RuntimeKeyring(config.CASDA_USERNAME, password))
    client = Casda()
    authenticated = client.login(
        username=config.CASDA_USERNAME,
        store_password=False,
    )
    if authenticated is False:
        raise RuntimeError("CASDA authentication failed")
    return client


def _existing_sb_numbers(longobs: Path) -> set[int]:
    """Find observations already represented by valid MS trees or archives.

    中文：查找已经由有效 MS 树或 archive 表示的观测编号。
    """

    numbers: set[int] = set()
    if not longobs.exists():
        return numbers
    for path in longobs.iterdir():
        if path.is_dir():
            match = re.search(r"SB(\d+)_beam", path.name)
            has_valid_ms = any(
                ms.is_dir()
                and not ms.name.endswith(".subtracted.ms")
                and (ms / "table.dat").exists()
                for ms in path.glob("*.ms")
            )
            if match and has_valid_ms:
                numbers.add(int(match.group(1)))
        elif path.is_file() and _is_archive_name(path.name):
            match = re.search(r"SB(\d+)", path.name)
            if match:
                numbers.add(int(match.group(1)))
    return numbers


def _is_archive_name(filename: str) -> bool:
    """Return whether a filename is an accepted CASDA tar archive.

    中文：判断文件名是否是允许的 CASDA tar archive。
    """

    lower = filename.lower()
    return lower.endswith((".tar", ".tar.gz", ".tgz"))


def _pair_urls(urls: list[str]) -> dict[str, dict[str, str]]:
    """Pair archive URLs with their optional checksum URLs by filename.

    中文：按文件名把 archive URL 与可选 checksum URL 配对。
    """

    pairs: dict[str, dict[str, str]] = {}
    for url in urls:
        filename = _url_basename(url)
        if not filename:
            continue
        if filename.endswith(".checksum"):
            base = filename[: -len(".checksum")]
            if _is_archive_name(base):
                pairs.setdefault(base, {})["checksum"] = url
        elif _is_archive_name(filename):
            pairs.setdefault(filename, {})["data"] = url
    return pairs


def download_urls_safely(
    client,
    urls: list[str],
    savedir: Path,
    source_name: str,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Download only CASDA archives and their checksums, one at a time.

    English: Non-archive URLs are ignored; failures are logged with redacted
    URLs and do not discard successful files from the same source.

    中文：忽略非 archive URL；失败使用去敏 URL 记录，并不丢弃同一源已经成功下载的文件。
    """

    savedir.mkdir(parents=True, exist_ok=True)
    successful: list[str] = []
    failed: list[tuple[str, str]] = []
    failure_log = config.STATE_ROOT / "failed_downloads.log"

    for base_name, pair in _pair_urls(urls).items():
        data_url = pair.get("data")
        checksum_url = pair.get("checksum")
        data_ok = False
        if data_url:
            try:
                downloaded = client.download_files([data_url], savedir=str(savedir))
                if not downloaded or not any(
                    Path(path).exists() and _is_archive_name(Path(path).name)
                    for path in downloaded
                ):
                    raise RuntimeError("CASDA returned no local data file")
                successful.append(data_url)
                data_ok = True
                logger.info("Downloaded %s for %s", base_name, source_name)
            except Exception as exc:  # One file must not abort a source / 单个文件失败不能中止整个源。
                message = str(exc)
                failed.append((data_url, message))
                failure_log.parent.mkdir(parents=True, exist_ok=True)
                with failure_log.open("a", encoding="utf-8") as handle:
                    handle.write(f"{source_name} | {_redact_url(data_url)} | {message}\n")
                partial = savedir / base_name
                if partial.exists() and partial.is_file():
                    partial.unlink()
                logger.warning("Download failed for %s: %s", _redact_url(data_url), message)

        if checksum_url and data_ok:
            try:
                client.download_files([checksum_url], savedir=str(savedir))
                successful.append(checksum_url)
            except Exception as exc:
                failed.append((checksum_url, str(exc)))
                logger.warning(
                    "Checksum download failed for %s: %s",
                    _redact_url(checksum_url),
                    exc,
                )

    return successful, failed


def download_source_table(
    client,
    rows: Table,
    longobs: Path,
    source_name: str,
) -> tuple[int, int]:
    """Stage and download all not-yet-present observations for one source.

    中文：为一个源 stage 并下载所有尚未存在的观测。
    """

    longobs.mkdir(parents=True, exist_ok=True)
    existing = _existing_sb_numbers(longobs)
    pending_mask = [obs_number(value) not in existing for value in rows["obs_id"]]
    pending = rows[np.asarray(pending_mask, dtype=bool)]
    success_count = 0
    failure_count = 0

    for start in range(0, len(pending), config.CASDA_STAGE_BATCH_SIZE):
        batch = pending[start : start + config.CASDA_STAGE_BATCH_SIZE]
        try:
            urls = client.stage_data(batch)
            success, failed = download_urls_safely(client, list(urls), longobs, source_name)
            success_count += len(success)
            failure_count += len(failed)
        except Exception as exc:
            failure_count += len(batch)
            logger.exception("CASDA stage_data failed for %s: %s", source_name, exc)

    return success_count, failure_count
