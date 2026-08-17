"""Model gateway — the one process on the backend network that holds a vendor key (`mas gateway`).

    compose worker (no egress) ──POST /v1/chat/completions (OpenAI wire)──▶ gateway (backend + egress networks)
                                                                                 │ allow-listed model names, bearer
                                                                                 │ token, size/time bounds
                                                                                 ▼
                                                                          upstream ModelProvider (from_spec)
                                                                          e.g. anthropic:<model>, openai:<model>, fake:builder

Workers keep `--network` internal-only and use the ordinary `openai:` provider with `MAS_OPENAI_BASE_URL` pointing here;
their own meter records usage and prices from the response's real upstream model id. The gateway does not persist
anything (no key, no prompts): it translates one wire shape, applies bounds, forwards, and counts. The upstream's native
assistant blocks (`Completion.native`, e.g. signed thinking blocks) ride along as `mas_native` on the wire in both
directions, so a worker behind the gateway continues a tool round exactly like a direct client (openai_compat.py).
Stdlib only (`http.server`), no streaming (`stream: true` → 400). This is a narrow egress control, not an auth system:
the bearer token stops accidental use, the network layout stops the rest.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from mas.providers.base import (
    ModelProvider,
    ProviderRateLimited,
    ProviderRequestError,
    ProviderUnavailable,
)
from mas.providers.openai_compat import from_openai_messages, from_openai_tools, to_openai_response

log = logging.getLogger(__name__)


class ModelGateway:
    def __init__(
        self,
        upstream: ModelProvider,
        *,
        allowed_models: list[str] | None = None,  # names clients may ask for; empty/None = only the upstream's own name
        token: str | None = None,
        max_body_bytes: int = 2_000_000,
        max_tokens_cap: int = 64_000,
        timeout_s: float = 600.0,
        listen: tuple[str, int] = ("127.0.0.1", 0),
    ):
        self.upstream = upstream
        names = [m.strip() for m in (allowed_models or []) if m and m.strip()]
        self.allowed_models = set(names) or {upstream.model}
        self.token = token or None
        self.max_body_bytes = int(max_body_bytes)
        self.max_tokens_cap = int(max_tokens_cap)
        self.timeout_s = float(timeout_s)
        self.stats = {"requests": 0, "ok": 0, "rejected": 0, "upstream_errors": 0}
        gw = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "mas-gateway/1"

            def log_message(self, fmt: str, *args: Any) -> None:  # no request lines with content in the logs
                log.debug("gateway: " + fmt, *args)

            def _send(self, status: int, payload: dict[str, Any], headers: dict[str, str] | None = None) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def _err(self, status: int, message: str, headers: dict[str, str] | None = None) -> None:
                gw.stats["rejected"] += 1
                self._send(status, {"error": {"message": message, "type": "gateway_error"}}, headers)

            def do_GET(self) -> None:  # noqa: N802 - http.server API
                if self.path in ("/healthz", "/health"):
                    self._send(200, {"ok": True, "upstream": gw.upstream.name, "models": sorted(gw.allowed_models)})
                else:
                    self._err(404, "not found")

            def do_POST(self) -> None:  # noqa: N802 - http.server API
                gw.stats["requests"] += 1
                if self.path.rstrip("/") not in ("/v1/chat/completions", "/chat/completions"):
                    return self._err(404, "not found")
                if gw.token:
                    auth = self.headers.get("Authorization", "")
                    if auth != f"Bearer {gw.token}":
                        return self._err(401, "unauthorized")
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    return self._err(400, "bad content-length")
                if length <= 0 or length > gw.max_body_bytes:
                    return self._err(413, f"request body must be 1..{gw.max_body_bytes} bytes")
                try:
                    body = json.loads(self.rfile.read(length).decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    return self._err(400, "invalid JSON")
                if not isinstance(body, dict):
                    return self._err(400, "body must be an object")
                if body.get("stream"):
                    return self._err(400, "streaming is not supported by the gateway")
                model = str(body.get("model") or "")
                if model not in gw.allowed_models:
                    return self._err(400, f"model {model!r} is not allowed here (allowed: {sorted(gw.allowed_models)})")
                try:
                    messages = from_openai_messages(list(body.get("messages") or []))
                    tools = from_openai_tools(body.get("tools")) or None
                except (ValueError, TypeError, KeyError) as e:
                    return self._err(400, f"bad messages/tools: {e}")
                if not messages:
                    return self._err(400, "messages must not be empty")
                raw_max = body.get("max_completion_tokens", body.get("max_tokens", 4096))
                try:
                    max_tokens = max(1, min(int(raw_max), gw.max_tokens_cap))
                except (TypeError, ValueError):
                    return self._err(400, "bad max_tokens")
                temperature = body.get("temperature")
                t0 = time.monotonic()
                try:
                    comp = gw.upstream.complete(
                        messages,
                        max_tokens=max_tokens,
                        tools=tools,
                        temperature=float(temperature) if isinstance(temperature, int | float) else None,
                        timeout_s=gw.timeout_s,
                    )
                except ProviderRateLimited as e:
                    gw.stats["upstream_errors"] += 1
                    hdrs = {"Retry-After": str(int(e.retry_after_s))} if e.retry_after_s else None
                    return self._err(429, f"upstream rate limited: {e}", hdrs)
                except ProviderRequestError as e:
                    gw.stats["upstream_errors"] += 1
                    return self._err(400, f"upstream rejected the request: {e}")
                except ProviderUnavailable as e:
                    gw.stats["upstream_errors"] += 1
                    return self._err(503, f"upstream unavailable: {e}")
                except Exception as e:  # noqa: BLE001 - the gateway must never die on one request
                    gw.stats["upstream_errors"] += 1
                    log.exception("gateway: upstream call failed")
                    return self._err(502, f"upstream error: {type(e).__name__}")
                gw.stats["ok"] += 1
                resp = to_openai_response(comp, model=model, response_id=f"gw-{uuid.uuid4().hex[:12]}")
                self._send(200, resp, {"x-request-id": resp["id"], "x-gateway-seconds": f"{time.monotonic() - t0:.3f}"})

        self.server = ThreadingHTTPServer(listen, Handler)
        self.server.daemon_threads = True

    # ------------------------------------------------------------------ lifecycle

    @property
    def address(self) -> tuple[str, int]:
        host, port = self.server.server_address[:2]
        return str(host), int(port)

    @property
    def base_url(self) -> str:
        host, port = self.address
        return f"http://{host}:{port}/v1"

    def serve_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        t = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.2}, name="gateway", daemon=True)
        t.start()
        try:
            while not stop.wait(0.5):
                pass
        finally:
            self.server.shutdown()
            t.join(5)

    def start(self) -> threading.Thread:
        t = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.2}, name="gateway", daemon=True)
        t.start()
        return t

    def close(self) -> None:
        try:
            self.server.shutdown()
        except Exception:
            pass
        self.server.server_close()
