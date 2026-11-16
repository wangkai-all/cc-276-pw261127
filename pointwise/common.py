"""通用工具：原子读写、校验和、文件锁、JSON 输出、HTTP 小工具。

只依赖标准库，保证两端可以在无第三方依赖的机器上直接运行。
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

CHUNK = 1024 * 1024


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def sha256_file(path: os.PathLike[str] | str, limit: int | None = None) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        remaining = limit
        while True:
            want = CHUNK if remaining is None else min(CHUNK, remaining)
            if want <= 0:
                break
            chunk = fh.read(want)
            if not chunk:
                break
            h.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_digest(obj: Any) -> str:
    return sha256_bytes(canonical_json(obj).encode("utf-8"))


def read_json(path: os.PathLike[str] | str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError as exc:
        raise ValueError(f"corrupt json at {path}: {exc}") from exc


def write_json_atomic(path: os.PathLike[str] | str, obj: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_text_atomic(path: os.PathLike[str] | str, text: str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def fsync_dir(path: os.PathLike[str] | str) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def safe_name(name: str) -> str:
    bad = set('/\\:*?"<>|\x00')
    cleaned = "".join("_" if (ch in bad or ch.isspace()) else ch for ch in name)
    if not cleaned or cleaned in (".", ".."):
        raise ValueError(f"unsafe name: {name!r}")
    return cleaned


def copy_stream(src: os.PathLike[str] | str, dst: os.PathLike[str] | str) -> None:
    tmp = f"{dst}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        with open(src, "rb") as fin, open(tmp, "wb") as fout:
            shutil.copyfileobj(fin, fout, CHUNK)
            fout.flush()
            os.fsync(fout.fileno())
        os.replace(tmp, dst)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class FileLock:
    """跨进程文件锁。Linux 用 ``fcntl.flock``，Windows 用 ``msvcrt.locking``。

    锁的是 ``<dir>/.fence.lock``；所有“提交进度”的动作都要先拿到它，
    从而让两个复制任务、或复制任务与校验任务之间串行化。
    """

    def __init__(self, path: os.PathLike[str] | str, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._fh = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+b")
        deadline = time.time() + self.timeout
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fh = fh
                return
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK, 13, 11):
                    fh.close()
                    raise
                if time.time() >= deadline:
                    fh.close()
                    raise TimeoutError(f"could not acquire lock {self.path}")
                time.sleep(0.02)

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


@contextmanager
def exclusive(path: os.PathLike[str] | str):
    lock = FileLock(path)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()


def rows_digest(rows: Iterable[Iterable[Any]]) -> str:
    """对行集合求稳定摘要：每行 JSON 序列化后排序再 hash。"""
    lines = sorted(canonical_json(list(r)) for r in rows)
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()
