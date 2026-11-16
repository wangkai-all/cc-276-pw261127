"""基于 ``http.server`` 的极简 HTTP 层：JSON 路由、字节 Range、SSE、断点下载。

刻意不用第三方框架：两端服务可以在任何有 Python 3.9+ 的节点上直接跑，
Range / ETag / If-Range 的行为也可以完全按复制的需要来写。
"""

from __future__ import annotations

import http.client
import json
import mimetypes
import os
import re
import socket
import threading
import time
import traceback
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

from .common import CHUNK, sha256_file

JSON = dict[str, Any]


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra

    def payload(self) -> JSON:
        body = {"ok": False, "error": self.code, "message": self.message}
        body.update(self.extra)
        return body


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: bytes
    client: tuple[str, int] | None = None
    server: "ApiServer | None" = None

    def q(self, key: str, default: str | None = None) -> str | None:
        vals = self.query.get(key)
        return vals[0] if vals else default

    def qb(self, key: str, default: bool = False) -> bool:
        val = self.q(key)
        if val is None:
            return default
        return val.lower() in ("1", "true", "yes", "on")

    def qlist(self, key: str, default: list[str] | None = None) -> list[str]:
        vals = self.query.get(key)
        if not vals:
            return list(default or [])
        out: list[str] = []
        for v in vals:
            out.extend([p for p in v.split(",") if p])
        return out

    def json(self) -> JSON:
        if not self.body:
            return {}
        try:
            obj = json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, "bad_json", f"invalid JSON body: {exc}")
        if not isinstance(obj, dict):
            raise ApiError(400, "bad_json", "JSON body must be an object")
        return obj

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)


@dataclass
class Route:
    method: str
    pattern: re.Pattern[str]
    handler: Callable[..., Any]
    kind: str = "json"  # json | file | sse | html


class ApiServer:
    def __init__(self, name: str, host: str = "127.0.0.1", port: int = 0) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.routes: list[Route] = []
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._shutdown = threading.Event()
        self.log_path: Path | None = None
        self._log_lock = threading.Lock()

    # -- route registration -------------------------------------------------
    def route(self, method: str, pattern: str, kind: str = "json"):
        def deco(fn: Callable[..., Any]):
            self.routes.append(
                Route(method.upper(), re.compile("^" + pattern + "$"), fn, kind)
            )
            return fn

        return deco

    def get(self, pattern: str, kind: str = "json"):
        return self.route("GET", pattern, kind)

    def post(self, pattern: str, kind: str = "json"):
        return self.route("POST", pattern, kind)

    # -- logging ------------------------------------------------------------
    def log(self, line: str) -> None:
        stamp = time.strftime("%H:%M:%S")
        text = f"[{stamp}] {line}\n"
        with self._log_lock:
            if self.log_path is not None:
                try:
                    with open(self.log_path, "a", encoding="utf-8") as fh:
                        fh.write(text)
                except OSError:
                    pass

    # -- lifecycle ----------------------------------------------------------
    def start(self, block: bool = False) -> "ApiServer":
        handler = _make_handler(self)
        httpd = ThreadingHTTPServer((self.host, self.port), handler)
        httpd.daemon_threads = True
        self._httpd = httpd
        self.port = httpd.server_address[1]
        if block:
            try:
                httpd.serve_forever(poll_interval=0.05)
            finally:
                httpd.server_close()
        else:
            self._thread = threading.Thread(
                target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
            )
            self._thread.start()
        return self

    def stop(self) -> None:
        self._shutdown.set()
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def stopped(self) -> bool:
        return self._shutdown.is_set()

    # -- dispatch -----------------------------------------------------------
    def dispatch(self, req: Request) -> tuple[int, dict[str, str], bytes | None, Any]:
        path = req.path
        for route in self.routes:
            if route.method != req.method:
                continue
            m = route.pattern.match(path)
            if not m:
                continue
            try:
                result = route.handler(req, **m.groupdict())
            except ApiError:
                raise
            if route.kind == "file":
                return result
            if route.kind == "sse":
                return 200, {"Content-Type": "text/event-stream"}, None, result
            if route.kind == "html":
                return 200, {"Content-Type": "text/html; charset=utf-8"}, result.encode("utf-8"), None
            status, body = (result if isinstance(result, tuple) else (200, result))
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            return status, {"Content-Type": "application/json; charset=utf-8"}, payload, None
        allowed = sorted({r.method for r in self.routes if r.pattern.match(path)})
        if allowed:
            raise ApiError(405, "method_not_allowed", f"allowed: {', '.join(allowed)}")
        raise ApiError(404, "not_found", f"no route for {req.method} {path}")


def _make_handler(api: ApiServer):
    class Handler(BaseHTTPRequestHandler):
        server_version = "pointwise/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            api.log("http " + (fmt % args))

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return b""
            if length > 64 * 1024 * 1024:
                raise ApiError(413, "too_large", "request body too large")
            return self.rfile.read(length)

        def _request(self) -> Request:
            parsed = urllib.parse.urlsplit(self.path)
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            return Request(
                method=self.command,
                path=urllib.parse.unquote(parsed.path),
                query=query,
                headers={k.lower(): v for k, v in self.headers.items()},
                body=self._read_body(),
                client=self.client_address,
                server=api,
            )

        def _send_simple(self, status: int, headers: dict[str, str], body: bytes | None) -> None:
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body or b"")))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _send_file(self, status: int, headers: dict[str, str], body: tuple) -> None:
            path, start, length = body
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            write_file_range(self, path, start, length)

        def _handle(self) -> None:
            try:
                req = self._request()
                status, headers, body, extra = api.dispatch(req)
                if extra is not None and req.method == "GET" and headers.get("Content-Type", "").startswith("text/event-stream"):
                    self._stream_sse(extra)
                elif isinstance(body, tuple) and len(body) == 3 and isinstance(body[0], Path):
                    self._send_file(status, headers, body)
                else:
                    self._send_simple(status, headers, body)
            except ApiError as exc:
                payload = json.dumps(exc.payload(), ensure_ascii=False).encode("utf-8")
                self._send_simple(
                    exc.status,
                    {"Content-Type": "application/json; charset=utf-8"},
                    payload,
                )
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as exc:  # pragma: no cover - defensive
                api.log("ERROR " + traceback.format_exc().replace("\n", " | "))
                payload = json.dumps(
                    {"ok": False, "error": "internal", "message": str(exc)},
                    ensure_ascii=False,
                ).encode("utf-8")
                try:
                    self._send_simple(
                        500, {"Content-Type": "application/json; charset=utf-8"}, payload
                    )
                except OSError:
                    pass

        def _stream_sse(self, gen: Any) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                for event in gen:
                    if api.stopped:
                        break
                    data = json.dumps(event, ensure_ascii=False)
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_GET = _handle
        do_POST = _handle
        do_DELETE = _handle

    return Handler


# ---------------------------------------------------------------------------
# Response helper for file routes
# ---------------------------------------------------------------------------


def file_response(req: Request, path: Path, extra_headers: dict[str, str] | None = None):
    """返回 (status, headers, body, None)，支持单段 Range 与 If-Range(ETag)。

    复制端用 Range 做断点续传；ETag 取文件长度+mtime，避免每次请求全量 hash。
    """
    if not path.is_file():
        raise ApiError(404, "not_found", f"file not found: {path.name}")
    size = path.stat().st_size
    etag = f'"{size:x}-{int(path.stat().st_mtime_ns):x}"'
    ctype = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    base = {
        "Content-Type": ctype,
        "Accept-Ranges": "bytes",
        "ETag": etag,
        "Cache-Control": "no-store",
    }
    if extra_headers:
        base.update(extra_headers)

    rng = req.header("range")
    if_range = req.header("if-range")
    start, end = 0, size - 1
    partial = False
    if rng and (if_range is None or if_range == etag):
        m = re.match(r"^bytes=(\d*)-(\d*)$", rng.strip())
        if not m:
            raise ApiError(416, "bad_range", "only single 'bytes=a-b' ranges supported")
        g1, g2 = m.groups()
        if g1:
            start = int(g1)
            end = int(g2) if g2 else size - 1
        elif g2:
            start = max(0, size - int(g2))
            end = size - 1
        else:
            raise ApiError(416, "bad_range", "empty range")
        if start >= size or start > end:
            raise ApiError(416, "range_not_satisfiable", "range outside file", size=size)
        end = min(end, size - 1)
        partial = True

    length = end - start + 1
    headers = dict(base)
    headers["Content-Length"] = str(length)
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return (206 if partial else 200), headers, (path, start, length), None


def write_file_range(handler: Any, path: Path, start: int, length: int) -> None:
    with open(path, "rb") as fh:
        fh.seek(start)
        remaining = length
        while remaining > 0:
            chunk = fh.read(min(CHUNK, remaining))
            if not chunk:
                break
            handler.wfile.write(chunk)
            remaining -= len(chunk)


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------


class HttpClient:
    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        self.scheme = parsed.scheme or "http"
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.base_path = parsed.path.rstrip("/")
        self.timeout = timeout

    def _conn(self, timeout: float | None = None) -> http.client.HTTPConnection:
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        return cls(self.host, self.port, timeout=timeout or self.timeout)

    def request(
        self,
        method: str,
        path: str,
        body: JSON | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, dict[str, Any]]:
        payload = None
        hdrs = dict(headers or {})
        if body is not None:
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            hdrs["Content-Type"] = "application/json"
        conn = self._conn(timeout)
        try:
            conn.request(method, self.base_path + path, body=payload, headers=hdrs)
            resp = conn.getresponse()
            raw = resp.read()
            try:
                data = json.loads(raw.decode("utf-8")) if raw else {}
            except (UnicodeDecodeError, json.JSONDecodeError):
                data = {"raw": raw.decode("utf-8", "replace")}
            return resp.status, data
        finally:
            conn.close()

    def get_json(self, path: str, **kw: Any) -> tuple[int, dict[str, Any]]:
        return self.request("GET", path, **kw)

    def post_json(self, path: str, body: JSON | None = None, **kw: Any) -> tuple[int, dict[str, Any]]:
        return self.request("POST", path, body=body or {}, **kw)


class TransferAborted(Exception):
    """传输被本端主动中断（保留 .part 以便续传）。"""


def download_with_resume(
    client: HttpClient,
    remote_path: str,
    dest: Path,
    expected_sha: str | None = None,
    expected_bytes: int | None = None,
    stop_event: threading.Event | None = None,
    progress: Callable[[int, int], None] | None = None,
    max_chunk: int = CHUNK,
    timeout: float = 30.0,
    abort_after: int | None = None,
    pace: float = 0.0,
) -> dict[str, Any]:
    """把 ``remote_path`` 下载到 ``dest``，使用 Range 从 ``.part`` 续传。

    返回 ``{bytes, resumed_from, sha256}``。调用方负责在成功后把文件放进最终位置。
    ``stop_event`` 与 ``abort_after`` 都产生**真实的中断**：已经写入的字节留在
    ``.part`` 里，后续调用从断点继续。``abort_after`` 是本端“模拟链路中断”的
    确定性方式（字节阈值），``stop_event`` 则供 HTTP interrupt 接口使用。
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    meta_path = dest.with_name(dest.name + ".part.meta")
    resumed_from = 0
    if part.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text("utf-8"))
            if meta.get("remote_path") == remote_path and (
                expected_sha is None or meta.get("sha256") in (None, expected_sha)
            ):
                resumed_from = part.stat().st_size
        except (OSError, json.JSONDecodeError):
            resumed_from = 0
    if abort_after is not None and resumed_from >= abort_after:
        # 断点已经在阈值之外：保留现状，等下一次 resume 继续。
        raise TransferAborted(f"already {resumed_from} bytes, threshold {abort_after}")
    if resumed_from == 0:
        part.write_bytes(b"")
        meta_path.write_text(
            json.dumps(
                {
                    "remote_path": remote_path,
                    "sha256": expected_sha,
                    "bytes": expected_bytes,
                    "started_at": time.time(),
                }
            ),
            "utf-8",
        )
    headers = {"Range": f"bytes={resumed_from}-"} if resumed_from else {}
    conn = client._conn(timeout)
    try:
        conn.request("GET", client.base_path + remote_path, headers=headers)
        resp = conn.getresponse()
        if resp.status not in (200, 206):
            body = resp.read(4096).decode("utf-8", "replace")
            raise ApiError(resp.status, "source_error", f"GET {remote_path} -> {resp.status}: {body}")
        if resp.status == 200 and resumed_from:
            # 来源忽略了 Range：只能从头写
            resumed_from = 0
            part.write_bytes(b"")
        total = None
        cr = resp.getheader("Content-Range")
        if cr and "/" in cr:
            total = int(cr.rsplit("/", 1)[1])
        if total is None:
            clen = resp.getheader("Content-Length")
            total = (int(clen) if clen else None)
        written = resumed_from
        with open(part, "ab") as fh:
            while True:
                if stop_event is not None and stop_event.is_set():
                    fh.flush()
                    os.fsync(fh.fileno())
                    raise TransferAborted(f"interrupted after {written} bytes")
                want = max_chunk
                if abort_after is not None:
                    # 把块切到阈值处，保证中断发生在精确的字节位置（而不是块边界），
                    # 这样 .part 的长度就是“传输到一半”的真实证据。
                    want = max(1, min(want, abort_after - written))
                chunk = resp.read(want)
                if not chunk:
                    break
                fh.write(chunk)
                written += len(chunk)
                if progress is not None:
                    progress(written, total)
                if pace:
                    time.sleep(pace)
                if stop_event is not None and stop_event.is_set():
                    fh.flush()
                    os.fsync(fh.fileno())
                    raise TransferAborted(f"interrupted after {written} bytes")
                if abort_after is not None and written >= abort_after:
                    fh.flush()
                    os.fsync(fh.fileno())
                    raise TransferAborted(
                        f"aborted at {written} bytes (threshold {abort_after})"
                    )
            fh.flush()
            os.fsync(fh.fileno())
    except (socket.timeout, ConnectionError, http.client.HTTPException) as exc:
        raise ApiError(502, "upstream_unavailable", f"{type(exc).__name__}: {exc}")
    finally:
        conn.close()

    if total is not None and written < total:
        raise ApiError(502, "short_read", f"got {written}/{total} bytes for {remote_path}")
    digest = sha256_file(part)
    if expected_sha and digest != expected_sha:
        part.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
        raise ApiError(
            502,
            "checksum_mismatch",
            f"sha256 mismatch for {remote_path}",
            expected=expected_sha,
            actual=digest,
        )
    if expected_bytes is not None and written != expected_bytes:
        part.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
        raise ApiError(
            502,
            "size_mismatch",
            f"size mismatch for {remote_path}",
            expected=expected_bytes,
            actual=written,
        )
    meta_path.unlink(missing_ok=True)
    return {"bytes": written, "resumed_from": resumed_from, "sha256": digest, "part": str(part)}
