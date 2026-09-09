"""CASDA TAP selection and per-source visibility download helpers."""

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
except ImportError:  # Script execution from the deployed program directory.
    import config
    from pipeline_utils import obs_number, safe_source_name

logger = logging.getLogger(__name__)

TAP_URL = "https://casda.csiro.au/casda_vo_tools/tap"


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def _url_basename(url: str) -> str:
    return Path(urlparse(url).path).name


def _read_password(path: Path, env_name: str) -> str | None:
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
    try:
        if np.ma.is_masked(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _redact_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _filter_and_deduplicate(rows: Table, ra_deg: float, dec_deg: float) -> Table:
    """Apply the exact source/SB filters used by the ada downloader."""

    if len(rows) == 0:
        return rows

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

    result = filtered[sorted(best_index.values())]
    result = Table(result, copy=True)
    try:
        # Failing closed is important: the table must not silently contain
        # unreleased observations if the CASDA release check is unavailable.
        result = __import__("astroquery.casda", fromlist=["Casda"]).Casda.filter_out_unreleased(result)
    except Exception as exc:
        raise RuntimeError("CASDA release filtering failed") from exc
    return result


def query_source_table(ra_deg: float, dec_deg: float) -> Table:
    """Query and filter CASDA visibility rows for one target."""

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
    return config.STATE_ROOT / "tap_cache" / f"{safe_source_name(source_name)}.ecsv"


def read_or_query_source(
    source_name: str,
    ra_deg: float,
    dec_deg: float,
    refresh: bool = False,
) -> tuple[Table, Path]:
    path = cache_path(source_name)
    if path.exists() and not refresh:
        return Table.read(path, format="ascii.ecsv"), path
    rows = query_source_table(ra_deg, dec_deg)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    rows.write(temporary, format="ascii.ecsv", overwrite=True)
    os.replace(temporary, path)
    return rows, path


def build_casda_client():
    """Create a non-interactive CASDA client for batch downloads."""

    from astroquery.casda import Casda

    password = _read_password(config.CASDA_PASSWORD_FILE, config.CASDA_PASSWORD_ENV)
    if not password:
        raise RuntimeError(
            "CASDA credentials are missing. Set CASDA_PASSWORD or create "
            f"a protected password file at {config.CASDA_PASSWORD_FILE}."
        )
    client = Casda()
    client.login(
        username=config.CASDA_USERNAME,
        password=password,
        store_password=False,
    )
    return client


def _existing_sb_numbers(longobs: Path) -> set[int]:
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
    return numbers


def _pair_urls(urls: list[str]) -> dict[str, dict[str, str]]:
    pairs: dict[str, dict[str, str]] = {}
    for url in urls:
        filename = _url_basename(url)
        if not filename:
            continue
        if filename.endswith(".checksum"):
            base = filename[: -len(".checksum")]
            pairs.setdefault(base, {})["checksum"] = url
        else:
            pairs.setdefault(filename, {})["data"] = url
    return pairs


def download_urls_safely(
    client,
    urls: list[str],
    savedir: Path,
    source_name: str,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Download data files one at a time and never leave partial tar files."""

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
                if not downloaded or not any(Path(path).exists() for path in downloaded):
                    raise RuntimeError("CASDA returned no local data file")
                successful.append(data_url)
                data_ok = True
                logger.info("Downloaded %s for %s", base_name, source_name)
            except Exception as exc:  # one file must not abort the source
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
    """Stage and download all not-yet-present observations for one source."""

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
