"""Small shared helpers for manifests, checkpoints and ASKAP paths."""

from __future__ import annotations

import csv
import json
import math
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import h5py

try:
    from .config import PRODUCT_ROOT, STATE_ROOT, batch_name
except ImportError:  # Script execution from the deployed program directory.
    from config import PRODUCT_ROOT, STATE_ROOT, batch_name


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: Any) -> None:
    """Write JSON atomically so a killed job cannot leave a half JSON file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def source_state_path(source_name: str) -> Path:
    return STATE_ROOT / "sources" / f"{safe_source_name(source_name)}.json"


def write_source_state(source_name: str, **values: Any) -> None:
    path = source_state_path(source_name)
    current = read_json(path, {}) or {}
    current.update(values)
    current["updated_at"] = utc_now()
    atomic_write_json(path, current)


def source_product_root(ucs_number: int, source_name: str) -> Path:
    return (
        PRODUCT_ROOT
        / batch_name(ucs_number)
        / safe_source_name(source_name)
        / "LongObs"
    )


def safe_source_name(source_name: str) -> str:
    """Allow only the UCS names used by the catalogue as path components."""

    if not re.fullmatch(r"UCS[0-9]+", source_name):
        raise ValueError(f"Unsafe source name: {source_name!r}")
    return source_name


def obs_number(value: Any) -> int | None:
    """Extract the numeric SB/observation identifier from an obs_id-like value."""

    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    match = re.search(r"(\d+)$", str(value))
    return int(match.group(1)) if match else None


def sb_number_from_name(name: str) -> int | None:
    match = re.search(r"SB(\d+)", name)
    return int(match.group(1)) if match else None


def read_t_min_map(path: Path) -> dict[int, float]:
    """Read the ada t_min CSV using only the standard library."""

    result: dict[int, float] = {}
    if not path.exists():
        return result
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            number = obs_number(row.get("obs_id", ""))
            try:
                value = float(row.get("t_min", ""))
            except (TypeError, ValueError):
                continue
            if number is not None and math.isfinite(value):
                result[number] = value
    return result


def product_sb_dirs(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and path.name.startswith("SB")
    )


def completed_product_sb_numbers(root: Path) -> set[int]:
    return {
        number
        for name in completed_product_sb_names(root)
        if (number := sb_number_from_name(name)) is not None
    }


def completed_product_sb_names(root: Path) -> set[str]:
    names: set[str] = set()
    for sb_dir in product_sb_dirs(root):
        has_ds = False
        for ds_path in sb_dir.glob("*.ds"):
            try:
                with h5py.File(ds_path, "r") as handle:
                    has_ds = {"time", "frequency", "flux"}.issubset(handle.keys())
            except (OSError, ValueError):
                has_ds = False
            if has_ds:
                break
        has_stokes_i = (sb_dir / "wsclean_model" / "wsclean-MFS-I-image.fits").exists()
        if has_ds and has_stokes_i:
            names.add(sb_dir.name)
    return names
