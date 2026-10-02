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
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table

try:
    from . import config
    from .pipeline_utils import (
        atomic_write_json,
        download_state_path,
        obs_number,
        read_json,
        safe_source_name,
        utc_now,
    )
    from .sb_status import record_step
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config
    from pipeline_utils import (
        atomic_write_json,
        download_state_path,
        obs_number,
        read_json,
        safe_source_name,
        utc_now,
    )
    from sb_status import record_step

logger = logging.getLogger(__name__)

TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"
TAP_CACHE_SELECTION_VERSION = "askap-frequency-half-power-v1"
SPEED_OF_LIGHT_METRES_PER_SECOND = 299_792_458.0


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


def _encode_casda_staged_url(url: str) -> str:
    """Encode literal characters that invalidate or ambiguate staged URLs.

    English: CASDA can return a mixed pair in which the archive URL already
    contains ``%2B/%20/%22`` while its checksum URL still contains a literal
    ``+``. These replacements are intentionally narrow and idempotent for
    already percent-encoded URLs; Requests can then transmit the strings via
    the existing ``Casda.download_files`` implementation.

    中文：CASDA 有时返回混合编码的一对 URL：archive 已包含
    ``%2B/%20/%22``，checksum 路径却仍包含字面 ``+``。这里仅编码这三类
    字面字符；已经百分号编码的 URL 不会被二次编码，后续仍使用现有的
    ``Casda.download_files``。
    """

    return str(url).replace("+", "%2B").replace(" ", "%20").replace('"', "%22")


_URL_WITH_QUERY_RE = re.compile(r"https?://[^\s<>]+", flags=re.IGNORECASE)
_AWS_QUERY_VALUE_RE = re.compile(
    r"(?i)(X-Amz-[A-Za-z-]+)=([^&\s'\"]+)"
)


def _redact_download_error(error: BaseException, urls: list[str]) -> str:
    """Return useful download diagnostics without retaining signed queries.

    English: Requests may reproduce the full prepared S3 URL in an exception.
    Replace both the staged form and any URL-shaped query text, then mask any
    detached ``X-Amz-*`` values left by a malformed URL containing spaces.

    中文：Requests 的异常可能包含完整的 S3 预签名 URL。这里同时清理原始 staged
    URL、异常中的 query URL，并遮蔽因含空格 URL 而残留的 ``X-Amz-*`` 参数。
    """

    message = str(error)
    for url in sorted((str(value) for value in urls), key=len, reverse=True):
        message = message.replace(url, _redact_url(url))

    def redact_match(match: re.Match[str]) -> str:
        value = match.group(0)
        if "?" not in value:
            return value
        trailing = ""
        while value and value[-1] in ".,;:)]}":
            trailing = value[-1] + trailing
            value = value[:-1]
        return f"{value.split('?', 1)[0]}?<redacted>{trailing}"

    message = _URL_WITH_QUERY_RE.sub(redact_match, message)
    return _AWS_QUERY_VALUE_RE.sub(r"\1=<redacted>", message)


def _download_sb_key(url: str) -> str:
    """Extract an SB key from a CASDA data/checksum URL.

    中文：从 CASDA data/checksum URL 中提取 SB 编号，作为下载状态键。
    """

    filename = _url_basename(url)
    match = re.search(r"SB(\d+)", filename, flags=re.IGNORECASE)
    return f"SB{match.group(1)}" if match else f"UNKNOWN:{filename}"


def _record_download_event(
    source_name: str,
    url: str,
    status: str,
    attempt: int,
    error: str | None = None,
) -> None:
    """Record the latest and historical status for one source+SB download.

    English: Signed URL query strings are redacted before they are stored.

    中文：保存源+SB 的最新状态和历史重试记录；写入前会去除签名 URL 的查询参数。
    """

    path = download_state_path(source_name)
    state = read_json(path, {}) or {}
    sb_key = _download_sb_key(url)
    entry = state.setdefault("sbs", {}).setdefault(sb_key, {})
    history = entry.setdefault("history", [])
    event = {
        "attempt": attempt,
        "status": status,
        "url": _redact_url(url),
        "time": utc_now(),
    }
    if error:
        event["error"] = error
    history.append(event)
    entry.update(
        {
            "source_name": safe_source_name(source_name),
            "sb": sb_key,
            "status": status,
            "attempts": max(int(entry.get("attempts", 0)), attempt),
            "last_url": _redact_url(url),
            "updated_at": event["time"],
        }
    )
    if error:
        entry["last_error"] = error
    state["source_name"] = safe_source_name(source_name)
    state["updated_at"] = event["time"]
    atomic_write_json(path, state)


def failed_download_sbs(source_name: str) -> list[str]:
    """Return source+SB keys whose latest status is download_failed.

    中文：返回最新状态为 `download_failed` 的源+SB 键列表。
    """

    state = read_json(download_state_path(source_name), {}) or {}
    return sorted(
        key
        for key, entry in state.get("sbs", {}).items()
        if entry.get("status") == "download_failed"
    )


def _wavelength_values_metres(rows: Table, column: str) -> np.ndarray:
    """Return one ObsCore wavelength column as metres, preserving invalids.

    English: ObsCore defines ``em_min`` and ``em_max`` as vacuum wavelengths
    in metres.  Honour an explicit compatible table unit when present; an
    absent unit therefore uses the ObsCore-mandated metre unit.

    中文：ObsCore 将 ``em_min`` 和 ``em_max`` 定义为以米表示的真空波长。若表格
    明确提供兼容单位则进行换算；没有单位时按 ObsCore 标准使用米。
    """

    unit = getattr(rows[column], "unit", None)
    try:
        scale_to_metres = (1.0 * (u.Unit(unit) if unit else u.m)).to_value(u.m)
    except (TypeError, ValueError, u.UnitConversionError) as exc:
        raise ValueError(
            f"CASDA TAP column {column} has a non-wavelength unit: {unit!r}"
        ) from exc

    values = np.full(len(rows), np.nan, dtype=float)
    for index, value in enumerate(rows[column]):
        numeric = _safe_float(value)
        if numeric is not None:
            values[index] = numeric * scale_to_metres
    return values


def _half_power_filter(
    rows: Table,
    separations_deg: np.ndarray,
) -> tuple[Table, np.ndarray]:
    """Apply the frequency-dependent ASKAP half-power radial cutoff.

    English: The narrowest beam occurs at the highest frequency in a row.
    Since ObsCore stores wavelength bounds, that frequency is ``c/em_min``.
    FWHM is a full width, so the radial half-power cutoff is ``FWHM/2``.

    中文：每行最高频率对应最窄波束；由于 ObsCore 保存波长边界，该频率为
    ``c/em_min``。FWHM 是全宽，因此径向半功率截止值为 ``FWHM/2``。
    """

    missing = [name for name in ("em_min", "em_max") if name not in rows.colnames]
    if missing:
        raise ValueError(
            "CASDA TAP response lacks required spectral column(s): "
            + ", ".join(missing)
        )

    reference_fwhm_deg = config.CASDA_REFERENCE_FWHM_DEG
    reference_frequency_hz = config.CASDA_REFERENCE_FREQUENCY_GHZ * 1.0e9
    if reference_fwhm_deg <= 0 or reference_frequency_hz <= 0:
        raise ValueError("CASDA reference FWHM and frequency must be positive")

    em_min_metres = _wavelength_values_metres(rows, "em_min")
    em_max_metres = _wavelength_values_metres(rows, "em_max")
    valid_spectral = (
        np.isfinite(em_min_metres)
        & np.isfinite(em_max_metres)
        & (em_min_metres > 0)
        & (em_max_metres >= em_min_metres)
    )
    invalid_count = int(np.count_nonzero(~valid_spectral))
    if invalid_count:
        logger.warning(
            "Discarding %d CASDA row(s) with invalid em_min/em_max wavelength bounds",
            invalid_count,
        )

    max_frequency_hz = np.full(len(rows), np.nan, dtype=float)
    max_frequency_hz[valid_spectral] = (
        SPEED_OF_LIGHT_METRES_PER_SECOND / em_min_metres[valid_spectral]
    )
    half_power_radius_deg = np.full(len(rows), np.nan, dtype=float)
    half_power_radius_deg[valid_spectral] = (
        0.5
        * reference_fwhm_deg
        * reference_frequency_hz
        / max_frequency_hz[valid_spectral]
    )
    within_half_power = valid_spectral & (
        separations_deg <= half_power_radius_deg
    )
    outside_count = int(np.count_nonzero(valid_spectral & ~within_half_power))
    if outside_count:
        logger.info(
            "Discarding %d CASDA row(s) outside the frequency-dependent "
            "ASKAP half-power radius",
            outside_count,
        )
    return rows[within_half_power], separations_deg[within_half_power]


def _filter_and_deduplicate(rows: Table, ra_deg: float, dec_deg: float) -> Table:
    """Apply the source/SB and frequency-dependent beam filters.

    English: Keep every column returned by TAP, select the nearest row for each
    observation after the half-power-radius filter, and fail closed if CASDA
    release filtering is unavailable.

    中文：保留 TAP 返回的每一列，在半功率半径筛选后为每个观测选择最近的一行；
    CASDA release 筛选不可用时安全失败，不静默放行未发布观测。
    """

    if len(rows) == 0:
        return rows
    required_columns = {
        "access_estsize",
        "em_min",
        "em_max",
        "obs_id",
        "quality_level",
        "s_ra",
        "s_dec",
    }
    missing_columns = sorted(required_columns.difference(rows.colnames))
    if missing_columns:
        raise ValueError(
            "CASDA TAP response lacks required column(s): "
            + ", ".join(missing_columns)
        )

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
    separations_deg = target.separation(coordinates).deg
    filtered, separations_deg = _half_power_filter(filtered, separations_deg)
    if len(filtered) == 0:
        return filtered

    best_index: dict[str, int] = {}
    for index, row in enumerate(filtered):
        obs_id = _text(row["obs_id"])
        if (
            obs_id not in best_index
            or separations_deg[index] < separations_deg[best_index[obs_id]]
        ):
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
    if config.CASDA_QUERY_LIMIT <= 0:
        raise ValueError("ASKAP_CASDA_QUERY_LIMIT must be positive")
    if config.CASDA_INITIAL_QUERY_RADIUS_DEG <= 0:
        raise ValueError("ASKAP_CASDA_INITIAL_QUERY_RADIUS_DEG must be positive")

    from astroquery.utils.tap.core import TapPlus

    tap = TapPlus(url=TAP_URL)
    query = (
        f"SELECT TOP {config.CASDA_QUERY_LIMIT} * FROM ivoa.obscore "
        "WHERE dataproduct_type = 'visibility' "
        "AND t_exptime > 500 "
        f"AND 1 = CONTAINS(POINT('ICRS',{ra_deg},{dec_deg}), "
        "circle('ICRS', s_ra, s_dec, "
        f"{config.CASDA_INITIAL_QUERY_RADIUS_DEG}))"
    )
    job = tap.launch_job_async(query)
    rows = job.get_results()
    if len(rows) >= config.CASDA_QUERY_LIMIT:
        raise RuntimeError(
            "CASDA TAP query returned the configured TOP limit "
            f"({config.CASDA_QUERY_LIMIT}); refusing a potentially truncated result"
        )
    result = _filter_and_deduplicate(rows, ra_deg, dec_deg)
    result.meta.update(_cache_selection_metadata(ra_deg, dec_deg))
    return result


def _cache_selection_metadata(ra_deg: float, dec_deg: float) -> dict[str, Any]:
    """Return metadata that makes the TAP spatial selection reproducible.

    中文：返回可重现 TAP 空间筛选、并用于自动识别旧 cache 的元数据。
    """

    return {
        "askap_selection_version": TAP_CACHE_SELECTION_VERSION,
        "askap_target_ra_deg": float(ra_deg),
        "askap_target_dec_deg": float(dec_deg),
        "askap_initial_query_radius_deg": float(
            config.CASDA_INITIAL_QUERY_RADIUS_DEG
        ),
        "askap_reference_fwhm_deg": float(config.CASDA_REFERENCE_FWHM_DEG),
        "askap_reference_frequency_ghz": float(
            config.CASDA_REFERENCE_FREQUENCY_GHZ
        ),
    }


def _cache_matches_current_selection(
    cached: Table,
    ra_deg: float,
    dec_deg: float,
) -> tuple[bool, str]:
    """Check cache columns and selection metadata against current settings.

    中文：检查 cache 列及其筛选元数据是否与当前设置一致。
    """

    required_columns = {"access_estsize", "em_min", "em_max"}
    missing = sorted(required_columns.difference(cached.colnames))
    if missing:
        return False, "missing column(s): " + ", ".join(missing)

    expected = _cache_selection_metadata(ra_deg, dec_deg)
    if cached.meta.get("askap_selection_version") != TAP_CACHE_SELECTION_VERSION:
        return False, "selection version is absent or obsolete"
    for key, value in expected.items():
        if key == "askap_selection_version":
            continue
        cached_value = _safe_float(cached.meta.get(key))
        if cached_value is None or not np.isclose(
            cached_value,
            float(value),
            rtol=0.0,
            atol=1.0e-12,
        ):
            return False, f"selection metadata mismatch: {key}"
    return True, "current"


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
        cache_is_current, reason = _cache_matches_current_selection(
            cached,
            ra_deg,
            dec_deg,
        )
        if cache_is_current:
            return cached, path
        logger.warning("Ignoring stale TAP cache %s: %s", path, reason)
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

    import astroquery
    from astroquery.casda import CasdaClass
    from keyring.backend import KeyringBackend
    import keyring

    class _PercentEncodedUrlCasda(CasdaClass):
        """Backport the Astroquery 0.4.12 CASDA signed-URL fix.

        Astroquery 0.4.11 calls ``unquote()`` on each staged URL in
        ``_complete_job``. That changes signed path/query bytes such as ``%2B``
        and invalidates AWS S3 signatures. Start with the upstream 0.4.12
        behaviour, then encode only any literal ``+``, spaces, or quotes that
        CASDA still returns (observed on checksum URLs). The later
        ``download_files()`` call remains unchanged and still downloads one
        archive/checksum pair at a time.

        中文：Astroquery 0.4.11 会在 ``_complete_job`` 中对 staged URL 调用
        ``unquote()``，从而改变 ``%2B`` 等已签名字符并破坏 AWS S3 签名。这里先按
        0.4.12 上游实现保留编码，再仅编码 CASDA 仍返回的字面 ``+``、空格或引号；
        后续 ``download_files()`` 逻辑不变。
        """

        def _complete_job(self, job_url, verbose):
            final_status = self._run_job(
                job_url,
                verbose,
                poll_interval=self.POLL_INTERVAL,
            )
            if final_status != "COMPLETED":
                if verbose:
                    logger.info("CASDA staging job ended with status %s", final_status)
                raise ValueError(
                    "Data staging job did not complete successfully. "
                    f"Status was {final_status}"
                )

            job_details = self._get_job_details_xml(job_url)
            fileurls: list[str] = []
            results = job_details.find("uws:results", self._uws_ns)
            if results is None:
                raise ValueError("CASDA staging job returned no result URL collection")
            href_key = "{http://www.w3.org/1999/xlink}href"
            for result in results.findall("uws:result", self._uws_ns):
                file_location = result.get(href_key)
                if file_location:
                    fileurls.append(_encode_casda_staged_url(file_location))
            return fileurls

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
    client = _PercentEncodedUrlCasda()
    authenticated = client.login(
        username=config.CASDA_USERNAME,
        store_password=False,
    )
    if authenticated is False:
        raise RuntimeError("CASDA authentication failed")
    logger.info(
        "CASDA client ready (astroquery %s; staged URL percent-encoding preserved)",
        astroquery.__version__,
    )
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
    attempt: int = 1,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Download each SB archive and checksum together in one CASDA call.

    English: This mirrors the verified single-SB test workflow: pass the
    archive URL and its ``.checksum`` companion together to
    ``Casda.download_files``. Non-archive URLs are ignored.

    中文：此处复现已验证的单个 SB 测试流程：将 archive URL 与对应的
    ``.checksum`` URL 一起交给 ``Casda.download_files`` 下载；忽略非 archive URL。
    """

    savedir.mkdir(parents=True, exist_ok=True)
    successful: list[str] = []
    failed: list[tuple[str, str]] = []
    failure_log = config.STATE_ROOT / "failed_downloads.log"

    for base_name, pair in _pair_urls(urls).items():
        sb_urls = [pair[key] for key in ("data", "checksum") if key in pair]
        if not sb_urls:
            continue
        try:
            started = time.monotonic()
            client.download_files(sb_urls, savedir=str(savedir))
            elapsed = time.monotonic() - started
            successful.extend(sb_urls)
            for url in sb_urls:
                status = "checksum_downloaded" if _url_basename(url).endswith(".checksum") else "downloaded"
                _record_download_event(source_name, url, status, attempt)
            sb_match = re.search(r"(SB\d+)", base_name)
            if sb_match:
                sb_id = sb_match.group(1)
                record_step(source_name, sb_id, "archive_download", "Y", seconds=elapsed)
                if "checksum" in pair:
                    record_step(source_name, sb_id, "checksum_download", "Y", seconds=elapsed)
            logger.info("Downloaded archive and checksum for %s (%s)", source_name, base_name)
        except Exception as exc:  # One SB must not abort a source / 单个 SB 失败不能中止整个源。
            message = _redact_download_error(exc, sb_urls)
            sb_match = re.search(r"(SB\d+)", base_name)
            if sb_match:
                sb_id = sb_match.group(1)
                record_step(source_name, sb_id, "archive_download", "N", error=message)
                if "checksum" in pair:
                    record_step(source_name, sb_id, "checksum_download", "N", error=message)
            for url in sb_urls:
                failed.append((url, message))
                _record_download_event(source_name, url, "download_failed", attempt, message)
            failure_log.parent.mkdir(parents=True, exist_ok=True)
            with failure_log.open("a", encoding="utf-8") as handle:
                handle.write(f"{source_name} | {_redact_url(sb_urls[0])} | {message}\n")
            logger.warning("Download failed for %s: %s", _redact_url(sb_urls[0]), message)

    return successful, failed


def download_source_table(
    client,
    rows: Table,
    longobs: Path,
    source_name: str,
    attempt: int = 1,
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
            success, failed = download_urls_safely(
                client,
                list(urls),
                longobs,
                source_name,
                attempt=attempt,
            )
            success_count += len(success)
            failure_count += len(failed)
        except Exception as exc:
            failure_count += len(batch)
            logger.exception("CASDA stage_data failed for %s: %s", source_name, exc)

    return success_count, failure_count
