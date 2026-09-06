#!/usr/bin/env python3
"""A fake OpenAI-compatible backend with *finite* capacity.

The point of this is measurement, not realism. A real model's per-request
variance is hundreds of milliseconds, which swamps the 5-20ms effects the
scheduler experiments are trying to resolve. This serves deterministic
latency instead, so the curves show the gateway rather than the model.

Crucially it has a hard concurrency limit. A backend that answers
everything at once never makes anything queue, so the gateway's queue
depth stays at zero and every curve comes out flat.

Config (env):
  MOCK_PORT          listen port                       (9000)
  MOCK_CONCURRENCY   requests served at once           (4)
  MOCK_PREFILL_MS    delay before the first token      (200)
  MOCK_TOKEN_MS      delay between tokens              (20)
  MOCK_TOKENS        tokens per completion             (20)

Behaviour:
  model "boom"  -> 500, for error-path tests
  model "slow"  -> 10x prefill, for timeout tests
  GET /stats    -> {"in_flight":N,"max_in_flight":N,"queued":N,"served":N}
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("MOCK_PORT", 9000))
CONCURRENCY = int(os.environ.get("MOCK_CONCURRENCY", 4))
PREFILL_MS = int(os.environ.get("MOCK_PREFILL_MS", 200))
TOKEN_MS = int(os.environ.get("MOCK_TOKEN_MS", 20))
N_TOKENS = int(os.environ.get("MOCK_TOKENS", 20))

# The capacity limit. Requests beyond CONCURRENCY block here, which is
# exactly the backpressure the gateway is supposed to be managing.
SLOTS = threading.Semaphore(CONCURRENCY)

_lock = threading.Lock()
_stats = {"in_flight": 0, "max_in_flight": 0, "queued": 0, "served": 0}


def _enter():
    with _lock:
        _stats["queued"] += 1
    SLOTS.acquire()
    with _lock:
        _stats["queued"] -= 1
        _stats["in_flight"] += 1
        _stats["max_in_flight"] = max(_stats["max_in_flight"], _stats["in_flight"])


def _leave():
    with _lock:
        _stats["in_flight"] -= 1
        _stats["served"] += 1
    SLOTS.release()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path != "/stats":
            self._json(404, {"error": "not found"})
            return
        with _lock:
            self._json(200, dict(_stats))

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        model = body.get("model", "mock")

        if model == "boom":
            self._json(500, {"error": {"message": "model exploded"}})
            return

        prefill = PREFILL_MS * (10 if model == "slow" else 1)

        _enter()
        try:
            if body.get("stream"):
                self._stream(model, prefill)
            else:
                self._buffered(model, prefill)
        finally:
            _leave()

    def _buffered(self, model, prefill):
        time.sleep(prefill / 1000.0)
        time.sleep(N_TOKENS * TOKEN_MS / 1000.0)
        text = " ".join(f"tok{i}" for i in range(N_TOKENS))
        self._json(200, {
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10,
                      "completion_tokens": N_TOKENS,
                      "total_tokens": 10 + N_TOKENS},
        })

    def _stream(self, model, prefill):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        time.sleep(prefill / 1000.0)
        for i in range(N_TOKENS):
            frame = {"id": "chatcmpl-mock",
                     "object": "chat.completion.chunk",
                     "model": model,
                     "choices": [{"index": 0, "delta": {"content": f"tok{i} "}}]}
            self._chunk(f"data: {json.dumps(frame)}\n\n")
            time.sleep(TOKEN_MS / 1000.0)
        self._chunk("data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def _chunk(self, text):
        data = text.encode()
        self.wfile.write(f"{len(data):X}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()

    def _json(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"mock backend on :{PORT} "
          f"concurrency={CONCURRENCY} prefill={PREFILL_MS}ms "
          f"token={TOKEN_MS}ms tokens={N_TOKENS}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
