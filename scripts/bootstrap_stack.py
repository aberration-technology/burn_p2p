#!/usr/bin/env python3
"""Materialize or verify burn_p2p's sibling path-dependency stack."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Repository:
    name: str
    path: Path
    url: str
    revision: str


def command(*args: str, cwd: Path | None = None, capture: bool = False) -> str:
    result = subprocess.run(
        args,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    return result.stdout.strip() if capture else ""


def load_lock(root: Path) -> list[Repository]:
    lock_path = root / "stack.lock.toml"
    with lock_path.open("rb") as handle:
        document = tomllib.load(handle)
    if document.get("schema_version") != 1:
        raise ValueError(f"unsupported stack lock schema in {lock_path}")

    repositories = []
    for raw in document.get("repositories", []):
        revision = raw["revision"]
        if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
            raise ValueError(f"{raw['name']} revision must be a full lowercase Git SHA")
        repositories.append(
            Repository(
                name=raw["name"],
                path=(root / raw["path"]).resolve(),
                url=raw["url"],
                revision=revision,
            )
        )
    if not repositories:
        raise ValueError(f"{lock_path} contains no repositories")
    return repositories


def remote_identity(url: str) -> str:
    value = url.removesuffix("/").removesuffix(".git")
    if value.startswith("git@"):
        value = value.split("@", 1)[1].replace(":", "/", 1)
    elif "://" in value:
        value = value.split("://", 1)[1]
        authority, separator, path = value.partition("/")
        authority = authority.rsplit("@", 1)[-1]
        value = f"{authority}{separator}{path}"
    return value.lower()


def verify_remote(repository: Repository) -> None:
    actual = command(
        "git", "remote", "get-url", "origin", cwd=repository.path, capture=True
    )
    if remote_identity(actual) != remote_identity(repository.url):
        raise RuntimeError(
            f"{repository.name}: origin is {actual!r}, expected {repository.url!r}"
        )


def is_dirty(path: Path) -> bool:
    return bool(command("git", "status", "--porcelain", cwd=path, capture=True))


def materialize(repository: Repository, repair_existing: bool) -> None:
    if not (repository.path / ".git").exists():
        repository.path.parent.mkdir(parents=True, exist_ok=True)
        command("git", "clone", "--no-checkout", repository.url, str(repository.path))
        command("git", "checkout", "--detach", repository.revision, cwd=repository.path)
        return

    verify_remote(repository)
    actual = command("git", "rev-parse", "HEAD", cwd=repository.path, capture=True)
    if actual == repository.revision:
        return
    if not repair_existing:
        raise RuntimeError(
            f"{repository.name}: HEAD is {actual}, expected {repository.revision}; "
            "rerun with --repair-existing to select the lock revision"
        )
    if is_dirty(repository.path):
        raise RuntimeError(
            f"{repository.name}: refusing to replace {actual} because the worktree is dirty"
        )
    command("git", "fetch", "origin", repository.revision, cwd=repository.path)
    command("git", "checkout", "--detach", repository.revision, cwd=repository.path)


def verify(repository: Repository) -> None:
    if not (repository.path / ".git").exists():
        raise RuntimeError(f"{repository.name}: missing repository at {repository.path}")
    verify_remote(repository)
    actual = command("git", "rev-parse", "HEAD", cwd=repository.path, capture=True)
    if actual != repository.revision:
        raise RuntimeError(
            f"{repository.name}: HEAD is {actual}, expected {repository.revision}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="burn_p2p repository root",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="only verify the already-materialized stack",
    )
    parser.add_argument(
        "--repair-existing",
        action="store_true",
        help="move clean existing sibling repositories to locked detached revisions",
    )
    args = parser.parse_args()

    try:
        repositories = load_lock(args.root.resolve())
        for repository in repositories:
            if args.verify:
                verify(repository)
            else:
                materialize(repository, args.repair_existing)
            print(f"stack-ok {repository.name} {repository.revision}")
    except (KeyError, OSError, subprocess.CalledProcessError, RuntimeError, ValueError) as error:
        print(f"stack-error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
