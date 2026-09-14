#!/usr/bin/env python3
"""Compare one CASDA download using the production Ozstar code path.

English: This diagnostic reads the production TAP cache, creates the client via
``build_casda_client()``, stages exactly one cached row, and downloads the data
URL with ``client.download_files([data_url], ...)``. It does not use the
legacy local keyring and never writes into production staging/products.

中文：本诊断程序读取生产环境的 TAP cache，使用生产代码的
`build_casda_client()`，对一行缓存数据执行 `stage_data()`，再使用
`client.download_files([data_url], ...)` 下载。它不使用旧的本地 keyring，
也不会写入生产 staging/products。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

from astropy.table import Table

try:
    from . import config
    from .casda_query import build_casda_client
    from .pipeline_utils import obs_number
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config
    from casda_query import build_casda_client
    from pipeline_utils import obs_number


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def _is_checksum(url: str) -> bool:
    return urlparse(url).path.endswith(".checksum")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="UCS1501")
    parser.add_argument("--obs-id", type=int, default=63406)
    parser.add_argument("--cache", type=Path, help="Override the TAP ECSV cache path.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Diagnostic output directory; never use production staging/products.",
    )
    args = parser.parse_args()

    cache = args.cache or config.STATE_ROOT / "tap_cache" / f"{args.source}.ecsv"
    if not cache.is_file():
        raise SystemExit(f"TAP cache not found: {cache}")

    rows = Table.read(cache, format="ascii.ecsv")
    mask = [obs_number(value) == args.obs_id for value in rows["obs_id"]]
    selected = rows[mask]
    if len(selected) != 1:
        raise SystemExit(
            f"Expected exactly one cached row for {args.obs_id}, found {len(selected)}"
        )

    row = selected[0]
    print(f"Cache: {cache}")
    print(f"obs_id: {_text(row['obs_id'])}")
    print(f"filename: {_text(row['filename'])}")
    for column in ("access_estsize", "t_exptime", "t_min", "t_max"):
        if column in selected.colnames:
            print(f"{column}: {_text(row[column])}")

    output_dir = args.output_dir or (
        config.WORK / "diagnostics" / f"casda_{args.source}_SB{args.obs_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Creating the production CASDA client...")
    client = build_casda_client()
    print("Calling stage_data() for exactly one cached row...")
    urls = list(client.stage_data(selected))
    print(f"Returned {len(urls)} URL(s)")

    data_urls = [url for url in urls if not _is_checksum(str(url))]
    checksum_urls = [url for url in urls if _is_checksum(str(url))]
    if len(data_urls) != 1:
        raise SystemExit(f"Expected one data URL, found {len(data_urls)}")

    try:
        print("Calling client.download_files([data_url], savedir=...) ...")
        data_files = client.download_files([data_urls[0]], savedir=str(output_dir))
        print(f"Data download succeeded: {data_files}")
        if checksum_urls:
            checksum_files = client.download_files(
                [checksum_urls[0]], savedir=str(output_dir)
            )
            print(f"Checksum download succeeded: {checksum_files}")
    except Exception as exc:
        # Avoid printing the complete signed URL from the exception.
        message = str(exc).split(" for url:", 1)[0]
        raise SystemExit(
            f"Production-path download failed: {type(exc).__name__}: {message}"
        ) from exc


if __name__ == "__main__":
    main()
