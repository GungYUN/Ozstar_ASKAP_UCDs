"""Basic archive extraction helpers used by compute workers.

English: Login nodes only download CASDA archives. These helpers run on a
compute node, use the same basic tar extraction and filename-based MS move as
the original ASKAP_Combined_Image shell script.

中文：登录节点只下载 CASDA archive。这些 helper 在计算节点运行，使用与原始
ASKAP_Combined_Image shell 脚本相同的基础 tar 解压和按文件名移动 MS 逻辑。
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import time
from pathlib import Path

try:
    from .sb_status import record_step
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    from sb_status import record_step


logger = logging.getLogger("askap.staging")


def _remove_path(path: Path) -> None:
    """Remove a file, directory, or stale temporary path safely.

    中文：安全删除文件、目录或残留的临时路径。
    """

    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def decompress_and_move(longobs: Path, maximum_archives: int | None = None) -> None:
    """Extract CASDA archives and move each raw MS into SB*_beam* folders.

    English: This is intentionally called for the current source only on the
    compute node. It extracts directly into LongObs with ``tar -xf``,
    then removes the archive and moves top-level *.ms entries by filename.

    中文：该函数只在计算节点针对当前源调用。它直接把 archive 解压到 LongObs，
    然后删除 archive，并根据文件名移动顶层 *.ms 文件。
    """

    longobs.mkdir(parents=True, exist_ok=True)
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
    if maximum_archives is not None:
        archives = archives[:maximum_archives]
    for archive in archives:
        logger.info("Extracting %s", archive)
        match = re.search(r"(SB\d+).*?(beam\d+)", archive.name)
        started = time.monotonic()
        try:
            subprocess.run(
                ["tar", "-xf", str(archive), "-C", str(longobs)],
                check=True,
            )
        except Exception as exc:
            if match:
                record_step(longobs.parent.name, match.group(1), "extract_archive", "N",
                            beam=match.group(2), error=str(exc))
            raise
        if match:
            record_step(longobs.parent.name, match.group(1), "extract_archive", "Y",
                        beam=match.group(2), seconds=time.monotonic() - started)
        archive.unlink()

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
