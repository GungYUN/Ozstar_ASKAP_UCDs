"""Small shared helpers for manifests, checkpoints and ASKAP paths.

English: These helpers provide atomic JSON state, persistent-size checks,
source-safe paths, observation parsing, proper-motion epoch lookup, and final
DS/FITS product validation.

中文：这些 helper 提供原子 JSON 状态写入、持久化大小检查、安全源路径、观测编号解析、
自行 epoch 查找，以及最终 DS/FITS 产物验证。
"""

from __future__ import annotations

import csv
import json
import math
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import h5py
from astropy.io import fits

try:
    from .config import PRODUCT_ROOT, STATE_ROOT, batch_name
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    from config import PRODUCT_ROOT, STATE_ROOT, batch_name


def utc_now() -> str:
    """Return the current time as an ISO-8601 UTC string.

    中文：返回 ISO-8601 格式的当前 UTC 时间字符串。
    """

    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: Any) -> None:
    """Write JSON atomically so a killed job cannot leave half a state file.

    中文：原子写入 JSON，避免 job 被终止后留下半个状态文件。
    """

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
    """Read JSON or return the caller's default when the path is absent.

    中文：读取 JSON；路径不存在时返回调用者提供的默认值。
    """

    if not path.exists():
        return default
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def directory_size(path: Path) -> int:
    """Return regular-file bytes below `path` without following links.

    English: A concurrent worker may remove a file during measurement; that
    race is ignored and the next controller poll measures again.

    中文：返回 `path` 下普通文件的 bytes 且不跟随链接。worker 并发删除文件造成的
    race 会忽略，下一次 controller 轮询再测量。
    """

    if not path.exists():
        return 0
    total = 0
    for root, directories, filenames in os.walk(path, followlinks=False):
        directories[:] = [name for name in directories if not (Path(root) / name).is_symlink()]
        for name in filenames:
            file_path = Path(root) / name
            try:
                if not file_path.is_symlink():
                    total += file_path.stat().st_size
            except FileNotFoundError:
                # A worker may finish a file while the login controller measures
                # the tree; the next poll measures it again.
                # 登录 controller 测量时 worker 可能刚好完成文件；下一次轮询会重新测量。
                continue
    return total


def persistent_usage(path: Path) -> tuple[int, int, int]:
    """Return filesystem total, used, and free bytes for a persistent path.

    中文：返回持久化路径所在文件系统的总量、已用和剩余 bytes。
    """

    usage = shutil.disk_usage(path)
    return usage.total, usage.used, usage.free


def source_state_path(source_name: str) -> Path:
    """Return the safe JSON state path for one source.

    中文：返回一个源对应的安全 JSON 状态路径。
    """

    return STATE_ROOT / "sources" / f"{safe_source_name(source_name)}.json"


def download_state_path(source_name: str) -> Path:
    """Return the per-source, per-SB download-state JSON path.

    English: This is separate from the source controller state so one source
    can retain individual SB download history and retry outcomes.

    中文：该文件独立于源级 controller 状态，用于保留每个 SB 的下载历史和重试结果。
    """

    return STATE_ROOT / "sources" / f"{safe_source_name(source_name)}.downloads.json"


def write_source_state(source_name: str, **values: Any) -> None:
    """Merge values into one source state and write it atomically.

    中文：把值合并进一个源状态，并原子写入文件。
    """

    path = source_state_path(source_name)
    current = read_json(path, {}) or {}
    current.update(values)
    current["updated_at"] = utc_now()
    atomic_write_json(path, current)


def source_product_root(ucs_number: int, source_name: str) -> Path:
    """Return the ada-compatible persistent product root for one source.

    中文：返回一个源的 ada-compatible 持久化产物根目录。
    """

    return (
        PRODUCT_ROOT
        / batch_name(ucs_number)
        / safe_source_name(source_name)
        / "LongObs"
    )


def safe_source_name(source_name: str) -> str:
    """Allow only catalogue UCS names as path components.

    中文：只允许 catalogue 使用的 UCS 名称作为路径组件。
    """

    if not re.fullmatch(r"UCS[0-9]+", source_name):
        raise ValueError(f"Unsafe source name: {source_name!r}")
    return source_name


def obs_number(value: Any) -> int | None:
    """Extract a numeric SB/observation identifier from an obs_id-like value.

    中文：从类似 obs_id 的值中提取数字 SB/观测编号。
    """

    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    match = re.search(r"(\d+)$", str(value))
    return int(match.group(1)) if match else None


def sb_number_from_name(name: str) -> int | None:
    """Extract an SB number from a staged or product directory name.

    中文：从 staging 或产物目录名中提取 SB 编号。
    """

    match = re.search(r"SB(\d+)", name)
    return int(match.group(1)) if match else None


def read_t_min_map(path: Path) -> dict[int, float]:
    """Read the legacy ada `obs_id -> t_min` CSV using the standard library.

    English: This map is the fallback below CASDA ECSV values.

    中文：读取旧 ada `obs_id -> t_min` CSV；该 map 的优先级低于 CASDA ECSV。
    """

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


def read_t_min_map_from_table(table: Any) -> dict[int, float]:
    """Read finite CASDA `obs_id`/`t_min` pairs from a TAP/ECSV table.

    English: The worker overlays this map on the legacy map, giving CASDA
    values precedence, and uses MJD `61041.5` only when both are absent.

    中文：worker 将这个 map 覆盖到旧 map 上，因此 CASDA 优先；两者都缺失时才使用
    MJD `61041.5`。
    """

    if not hasattr(table, "colnames"):
        return {}
    if "obs_id" not in table.colnames or "t_min" not in table.colnames:
        return {}

    result: dict[int, float] = {}
    for obs_id_value, t_min_value in zip(table["obs_id"], table["t_min"]):
        number = obs_number(obs_id_value)
        if number is None:
            continue
        try:
            if hasattr(t_min_value, "value"):
                t_min_value = t_min_value.value
            value = float(t_min_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            result[number] = value
    return result


def product_sb_dirs(root: Path) -> Iterable[Path]:
    """Return SB-like product directories below one source root.

    中文：返回一个源根目录下的 SB 类产物目录。
    """

    if not root.exists():
        return []
    return sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and path.name.startswith("SB")
    )


def completed_product_sb_numbers(root: Path) -> set[int]:
    """Return SB numbers whose DS and Stokes-I products validate.

    中文：返回 DS 和 Stokes-I 产物均通过验证的 SB 编号。
    """

    return {
        number
        for name in completed_product_sb_names(root)
        if (number := sb_number_from_name(name)) is not None
    }


def completed_product_sb_names(root: Path) -> set[str]:
    """Return names of complete SB product directories.

    中文：返回完整 SB 产物目录的名称。
    """

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
        stokes_i = sb_dir / "wsclean_model" / "wsclean-MFS-I-image.fits"
        try:
            with fits.open(stokes_i, memmap=False) as handle:
                has_stokes_i = bool(handle and handle[0].data is not None)
        except (OSError, ValueError):
            has_stokes_i = False
        if has_ds and has_stokes_i:
            names.add(sb_dir.name)
    return names
