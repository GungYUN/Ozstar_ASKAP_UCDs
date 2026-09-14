"""Safe archive extraction helpers used by compute workers.

English: Login nodes only download CASDA archives. These helpers run on a
compute node, reject links/devices and path traversal, extract one archive into
a temporary directory, and then arrange raw MS data under SB/beam folders.

中文：登录节点只下载 CASDA archive。这些 helper 在计算节点运行，拒绝链接/设备和
路径穿越，先把每个 archive 解压到临时目录，再把原始 MS 放入 SB/beam 目录。
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tarfile
from pathlib import Path


logger = logging.getLogger("askap.staging")


def _remove_path(path: Path) -> None:
    """Remove a file, directory, or stale temporary path safely.

    中文：安全删除文件、目录或残留的临时路径。
    """

    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def _safe_extract(archive: Path, destination: Path) -> None:
    """Extract regular files/directories only after path containment checks.

    English: Symlinks, hard links, devices, and members escaping the target
    directory are rejected before extraction.

    中文：只允许普通文件/目录，并在解压前检查目标路径未逃出目标目录；符号链接、
    硬链接和设备文件都会被拒绝。
    """

    destination_resolved = destination.resolve()
    with tarfile.open(archive, mode="r:*") as tar:
        for member in tar.getmembers():
            if member.issym() or member.islnk() or not (member.isdir() or member.isfile()):
                raise RuntimeError(
                    f"Unsupported link/device in archive {archive}: {member.name}"
                )
            target = (destination / member.name).resolve()
            if os.path.commonpath((str(destination_resolved), str(target))) != str(
                destination_resolved
            ):
                raise RuntimeError(f"Unsafe archive member in {archive}: {member.name}")
        tar.extractall(destination)


def decompress_and_move(longobs: Path) -> None:
    """Extract CASDA archives and move each raw MS into SB*_beam* folders.

    English: This is intentionally called for the current source only on the
    compute node. Successful archive extraction removes the archive and its
    checksum after the raw MS has been placed.

    中文：该函数只在计算节点针对当前源调用。archive 成功解压并放置 raw MS 后，
    会删除 archive 和 checksum。
    """

    longobs.mkdir(parents=True, exist_ok=True)
    for stale in longobs.glob(".extract-*"):
        _remove_path(stale)
    archives = sorted(
        path
        for path in longobs.iterdir()
        if path.is_file()
        and (
            path.name.endswith(".tar")
            or path.name.endswith(".tar.gz")
            or path.name.endswith(".tgz")
        )
    )
    for archive in archives:
        logger.info("Extracting %s", archive)
        temporary = longobs / f".extract-{archive.name}-{os.getpid()}"
        _remove_path(temporary)
        temporary.mkdir(parents=True, exist_ok=True)
        try:
            _safe_extract(archive, temporary)
            for extracted in temporary.iterdir():
                target = longobs / extracted.name
                if target.exists():
                    # Replacing an interrupted extraction is safer than merging
                    # a possibly incomplete MeasurementSet.
                    # 替换中断的解压结果比合并可能不完整的 MeasurementSet 更安全。
                    _remove_path(target)
                shutil.move(str(extracted), str(target))
        except Exception:
            _remove_path(temporary)
            raise
        _remove_path(temporary)
        archive.unlink()

    # Checksums help download verification but are not needed by DStools.
    # checksum 用于下载校验，但 DStools 不需要它们。
    for checksum in longobs.glob("*.checksum"):
        checksum.unlink()

    for ms_path in sorted(longobs.glob("*.ms")):
        match = re.search(r"(SB\d+).*?(beam\d+)", ms_path.name)
        if not match:
            logger.warning("Cannot derive SB folder from %s", ms_path.name)
            continue
        sb_folder = longobs / f"{match.group(1)}_{match.group(2)}"
        sb_folder.mkdir(parents=True, exist_ok=True)
        target = sb_folder / ms_path.name
        if target.exists():
            _remove_path(ms_path)
        else:
            shutil.move(str(ms_path), str(target))
