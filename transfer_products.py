"""Password-assisted SCP/rsync transfer for ASKAP products.

English: Password values are deliberately absent from source code and command
arguments. Use a mode-600 password file (recommended) or set
`TRANSFER_PASSWORD` for one process. `sshpass` supplies it to ssh/scp
non-interactively. Transfers are explicit, product-limited, and SHA-256
verified; deletion is guarded and never automatic.

中文：密码值不会出现在源码或命令参数中。推荐使用 mode-600 密码文件，也可以为当前
进程设置 `TRANSFER_PASSWORD`。`sshpass` 非交互地把密码提供给 ssh/scp。传输必须
显式执行，只传产物并进行 SHA-256 校验；删除受保护且永远不会自动发生。

Examples, when this directory is installed at
``/fred/oz299/qhuang/ASKAP-UCDs/ozstar_askap`` on Ozstar::

    python3 transfer_products.py --direction to-ada --batch UCS1-50
    python3 transfer_products.py --direction to-ada --dry-run

The reverse direction is useful for moving a batch from ada back to the
Ozstar product tree::

    python3 transfer_products.py --direction to-ozstar --batch UCS1-50
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
from pathlib import Path

try:
    from . import config
except ImportError:  # Deployed-directory script mode / 部署目录直接脚本模式。
    import config


PRODUCT_NAMES = {
    "wsclean-MFS-I-image.fits",
    "wsclean-MFS-V-image.fits",
}


def _password_source(path: Path, allow_missing: bool = False) -> tuple[str, str]:
    """Select a runtime password source without returning the password in logs.

    English: The return value is either an environment marker/value or a file
    path for `sshpass`; a protected file is required unless this is a dry run.

    中文：返回环境变量标记/值或供 `sshpass` 使用的文件路径，不把密码写入日志；除
    dry-run 外必须使用受保护的密码文件。
    """

    env_password = os.environ.get("TRANSFER_PASSWORD")
    if env_password:
        return "env", env_password
    if not path.exists():
        if allow_missing:
            return "file", str(path)
        raise RuntimeError(
            "Transfer password is missing. Create a mode-600 password file at "
            f"{path} or set TRANSFER_PASSWORD in the environment."
        )
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise RuntimeError(
            f"Refusing insecure password file {path}; use chmod 600 {path}"
        )
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"Transfer password file is empty: {path}")
    # sshpass -f receives only the filename; never put the password itself in
    # the child command or printed command line.
    # sshpass -f 只接收文件名；密码本身不能进入子命令或打印的命令行。
    return "file", str(path)


def _sshpass_prefix(
    password_kind: str,
    password_value: str,
    require_tool: bool = True,
) -> tuple[list[str], dict[str, str]]:
    """Build the sshpass command prefix and child environment.

    中文：构造 sshpass 命令前缀和子进程环境。
    """

    if require_tool and shutil.which(config.SSHPASS_BIN) is None:
        raise RuntimeError(
            f"{config.SSHPASS_BIN!r} is not installed. Install sshpass on the "
            "machine that performs the transfer, or use an SSH key."
        )
    environment = os.environ.copy()
    if password_kind == "env":
        environment["SSHPASS"] = password_value
        return [config.SSHPASS_BIN, "-e"], environment
    return [config.SSHPASS_BIN, "-f", password_value], environment


def _ssh_options() -> list[str]:
    """Return common SSH connection options.

    中文：返回共用的 SSH 连接选项。
    """

    return ["-o", f"ConnectTimeout={config.SSH_CONNECT_TIMEOUT}"]


def _run(command: list[str], environment: dict[str, str], dry_run: bool) -> str:
    """Print a password-safe command and optionally execute it.

    English: Passwords are held in the environment or file descriptor path,
    never interpolated into the printed command.

    中文：打印并可选执行密码安全的命令；密码只在环境或文件路径中传递，绝不插入
    打印的命令。
    """

    printable = " ".join(shlex.quote(part) for part in command)
    print(f"$ {printable}")
    if dry_run:
        return ""
    result = subprocess.run(
        command,
        env=environment,
        check=True,
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout.rstrip())
    return result.stdout.strip()


def _remote_mkdir(
    prefix: list[str],
    environment: dict[str, str],
    remote: str,
    path: str,
    dry_run: bool,
) -> None:
    """Create a remote directory through the selected SSH prefix.

    中文：通过选定的 SSH 前缀在远端创建目录。
    """

    remote_command = f"mkdir -p -- {shlex.quote(path)}"
    command = prefix + ["ssh", *_ssh_options(), remote, remote_command]
    _run(command, environment, dry_run)


def _local_product_files(batch_path: Path) -> list[Path]:
    """List only DS and allowed MFS FITS files in a local batch.

    中文：只列出本地 batch 中允许传输的 DS 和 MFS FITS 文件。
    """

    return sorted(
        path
        for path in batch_path.rglob("*")
        if path.is_file()
        and (path.name.endswith(".ds") or path.name in PRODUCT_NAMES)
    )


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest of one local product file.

    中文：返回一个本地产物文件的 SHA-256 摘要。
    """

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _local_product_inventory(batch_path: Path) -> dict[str, str]:
    """Build relative-path to SHA-256 inventory for a local batch.

    中文：为本地 batch 构造相对路径到 SHA-256 的清单。
    """

    return {
        path.relative_to(batch_path).as_posix(): _sha256(path)
        for path in _local_product_files(batch_path)
    }


def _unexpected_local_files(batch_path: Path) -> list[Path]:
    """Find hidden or non-product files that should block transfer.

    中文：查找会阻止传输的隐藏文件或非产物文件。
    """

    return sorted(
        path
        for path in batch_path.rglob("*")
        if path.is_file()
        and (
            any(part.startswith(".") for part in path.relative_to(batch_path).parts)
            or not (path.name.endswith(".ds") or path.name in PRODUCT_NAMES)
        )
    )


def _validate_product_tree(batch_path: Path) -> None:
    """Require every discovered SB directory to contain DS and Stokes-I.

    中文：要求发现的每个 SB 目录都包含 DS 和 Stokes-I 产物。
    """

    sb_dirs = sorted(
        path
        for path in batch_path.rglob("*")
        if path.is_dir() and re.fullmatch(r"SB\d+_beam\d+", path.name)
    )
    if not sb_dirs:
        raise RuntimeError(f"No SB product directories found in {batch_path}")
    incomplete = []
    for sb_dir in sb_dirs:
        has_ds = any(sb_dir.glob("*.ds"))
        has_i = (sb_dir / "wsclean_model" / "wsclean-MFS-I-image.fits").exists()
        if not (has_ds and has_i):
            incomplete.append(sb_dir.relative_to(batch_path).as_posix())
    if incomplete:
        raise RuntimeError(
            "Incomplete SB product directories: " + ", ".join(incomplete[:20])
        )


def _validate_source_states(batch_path: Path) -> None:
    """Require local Ozstar source states to be complete before deletion.

    中文：删除前要求本地 Ozstar 源状态全部为 complete。
    """

    source_dirs = sorted(
        path
        for path in batch_path.iterdir()
        if path.is_dir() and re.fullmatch(r"UCS\d+", path.name)
    )
    for source_dir in source_dirs:
        state_path = config.STATE_ROOT / "sources" / f"{source_dir.name}.json"
        if not state_path.exists():
            raise RuntimeError(
                f"No completion state for {source_dir.name}; refusing deletion"
            )
        with state_path.open(encoding="utf-8") as handle:
            state = json.load(handle)
        if state.get("status") != "complete":
            raise RuntimeError(
                f"{source_dir.name} is not complete ({state.get('status')!r}); "
                "refusing deletion"
            )


def _remote_product_inventory(
    prefix: list[str],
    environment: dict[str, str],
    remote: str,
    path: str,
    dry_run: bool,
) -> dict[str, str]:
    """Read and hash the remote product inventory for one batch.

    中文：读取并计算一个远端 batch 的产物清单摘要。
    """

    expression = (
        f"find {shlex.quote(path)} -type f "
        r"\( -name '*.ds' -o -name 'wsclean-MFS-I-image.fits' "
        r"-o -name 'wsclean-MFS-V-image.fits' \) -exec sha256sum {} + | sort"
    )
    command = prefix + ["ssh", *_ssh_options(), remote, expression]
    output = _run(command, environment, dry_run)
    if dry_run:
        return {}
    inventory: dict[str, str] = {}
    for line in output.splitlines():
        digest, separator, absolute = line.partition("  ")
        if not separator:
            raise RuntimeError(f"Could not parse remote product listing: {line!r}")
        prefix_path = path.rstrip("/") + "/"
        if not absolute.startswith(prefix_path):
            raise RuntimeError(f"Remote product path is outside batch: {absolute!r}")
        inventory[absolute[len(prefix_path) :]] = digest
    return inventory


def _direction_defaults(direction: str) -> tuple[Path, str, str, str, Path]:
    """Return local root, remote endpoint, root, and password defaults.

    中文：根据方向返回本地根、远端端点、远端根和密码文件默认值。
    """

    if direction == "to-ada":
        return (
            config.PRODUCT_ROOT,
            config.ADA_SSH_USER,
            config.ADA_SSH_HOST,
            config.ADA_VISIBILITY_ROOT,
            config.ADA_PASSWORD_FILE,
        )
    return (
        Path(config.ADA_VISIBILITY_ROOT),
        config.OZSTAR_SSH_USER,
        config.OZSTAR_SSH_HOST,
        config.OZSTAR_PRODUCT_ROOT,
        config.OZSTAR_PASSWORD_FILE,
    )


def _batch_paths(source_root: Path, requested: list[str]) -> list[Path]:
    """Resolve requested or all safe batch directories below a root.

    中文：在根目录下解析指定的或全部安全 batch 目录。
    """

    if requested:
        for name in requested:
            if not re.fullmatch(r"UCS\d+-\d+", name):
                raise ValueError(f"Unsafe batch name: {name!r}")
        paths = [source_root / name for name in requested]
    else:
        paths = sorted(
            path
            for path in source_root.glob("UCS*")
            if path.is_dir() and re.fullmatch(r"UCS\d+-\d+", path.name)
        )
    source_root = source_root.resolve()
    paths = [path.resolve() for path in paths]
    if any(source_root not in path.parents for path in paths):
        raise ValueError("A batch path escapes the configured source root")
    missing = [path for path in paths if not path.is_dir()]
    if missing:
        raise FileNotFoundError(
            "Batch directory does not exist: " + ", ".join(str(path) for path in missing)
        )
    return paths


def transfer(args: argparse.Namespace) -> None:
    """Validate, transfer, verify, and optionally guarded-delete batches.

    English: The same product inventory must match before deletion is allowed.

    中文：验证、传输、校验 batch，并在满足保护条件时可选删除；删除前后产物清单必须
    保持一致。
    """

    default_source, default_user, default_host, default_remote_root, password_file = (
        _direction_defaults(args.direction)
    )
    source_root = Path(args.source_root).expanduser() if args.source_root else default_source
    remote_user = args.remote_user or default_user
    remote_host = args.remote_host or default_host
    remote_root = args.remote_root or default_remote_root
    batches = _batch_paths(source_root, args.batch)

    password_kind, password_value = _password_source(
        Path(args.password_file).expanduser() if args.password_file else password_file,
        allow_missing=args.dry_run,
    )
    prefix, environment = _sshpass_prefix(
        password_kind,
        password_value,
        require_tool=not args.dry_run,
    )
    remote = f"{remote_user}@{remote_host}"

    for batch_path in batches:
        _validate_product_tree(batch_path)
        local_inventory = _local_product_inventory(batch_path)
        if not local_inventory:
            print(f"Skipping empty batch: {batch_path}")
            continue
        unexpected = _unexpected_local_files(batch_path)
        if unexpected:
            raise RuntimeError(
                f"Unexpected non-product files in {batch_path}: "
                + ", ".join(str(path.relative_to(batch_path)) for path in unexpected[:10])
            )
        remote_batch = f"{remote_root.rstrip('/')}/{batch_path.name}"
        _remote_mkdir(prefix, environment, remote, remote_root, args.dry_run)

        if args.method == "scp":
            command = prefix + [
                "scp",
                "-r",
                "-p",
                *_ssh_options(),
                str(batch_path),
                f"{remote}:{remote_root.rstrip('/')}/",
            ]
        else:
            # rsync uses the same sshpass prefix and helps interrupted large
            # transfers; preserve the source batch name explicitly.
            # rsync 使用同一 sshpass 前缀，适合中断的大型传输，并显式保留 batch 名称。
            command = prefix + [
                "rsync",
                "-a",
                "--partial",
                "--protect-args",
                "-e",
                "ssh " + " ".join(shlex.quote(item) for item in _ssh_options()),
                f"{str(batch_path).rstrip('/')}/",
                f"{remote}:{remote_batch}/",
            ]

        _run(command, environment, args.dry_run)
        remote_inventory = _remote_product_inventory(
            prefix, environment, remote, remote_batch, args.dry_run
        )
        if not args.dry_run and remote_inventory != local_inventory:
            raise RuntimeError(
                f"Verification failed for {batch_path.name}: local and remote "
                "product paths/digests differ"
            )
        print(
            f"Verified {batch_path.name}: local {len(local_inventory)} product files, "
            f"remote {len(remote_inventory) if not args.dry_run else 'planned'}"
        )

        if args.delete_source and not args.dry_run:
            if args.direction == "to-ada":
                _validate_source_states(batch_path)
            elif not args.allow_delete_without_state:
                raise RuntimeError(
                    "Reverse-transfer deletion has no local Ozstar state. "
                    "Use --allow-delete-without-state only after manual verification."
                )
            current_inventory = _local_product_inventory(batch_path)
            if current_inventory != local_inventory:
                raise RuntimeError(
                    f"Local batch changed during transfer: {batch_path.name}"
                )
            final_remote_inventory = _remote_product_inventory(
                prefix, environment, remote, remote_batch, args.dry_run
            )
            if final_remote_inventory != current_inventory:
                raise RuntimeError(
                    f"Remote batch changed during transfer: {batch_path.name}"
                )
            shutil.rmtree(batch_path)
            print(f"Deleted transferred source tree: {batch_path}")


def main() -> None:
    """Parse explicit transfer direction and safety options.

    中文：解析显式传输方向和安全控制选项。
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--direction",
        choices=("to-ada", "to-ozstar"),
        required=True,
        help="Transfer products from the current host to ada or Ozstar.",
    )
    parser.add_argument(
        "--batch",
        action="append",
        default=[],
        help="Batch directory name such as UCS1-50; repeat for multiple batches.",
    )
    parser.add_argument("--source-root", help="Override the local batch root.")
    parser.add_argument("--remote-root", help="Override the remote batch root.")
    parser.add_argument("--remote-user", help="Override the remote SSH user.")
    parser.add_argument("--remote-host", help="Override the remote SSH host.")
    parser.add_argument("--password-file", help="Override the local mode-600 password file.")
    parser.add_argument("--method", choices=("scp", "rsync"), default="scp")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete the local batch only after remote file-count verification.",
    )
    parser.add_argument(
        "--allow-delete-without-state",
        action="store_true",
        help="Allow reverse-transfer deletion without Ozstar completion state.",
    )
    transfer(parser.parse_args())


if __name__ == "__main__":
    main()
