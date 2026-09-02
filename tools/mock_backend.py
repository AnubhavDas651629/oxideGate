import json, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKENS = ["Hello", " from", " the", " backend", "!"]

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if body.get("model") == "boom":
            b = b'{"error":"model exploded"}'
            self.send_response(500)
            self.send_header('Content-Length', str(len(b))); self.end_headers()
            self.wfile.write(b); return

        if body.get("stream"):
            self.send_response(200)
            self.send_header('Content-Type','text/event-stream')
            self.send_header('Transfer-Encoding','chunked')
            self.end_headers()
            time.sleep(0.20)                      # simulate prefill / TTFT
            for i, t in enumerate(TOKENS):
                frame = {"id":"chatcmpl-1","object":"chat.completion.chunk",
                         "choices":[{"index":0,"delta":{"content":t}}]}
                self._chunk(f"data: {json.dumps(frame)}\n\n")
                time.sleep(0.05)                  # inter-token delay
            self._chunk("data: [DONE]\n\n")
            self.wfile.write(b"0\r\n\r\n"); self.wfile.flush()
            return

        time.sleep(0.05)
        out = {"id":"chatcmpl-1","object":"chat.completion","created":1,
               "model":body["model"],
               "choices":[{"index":0,"message":{"role":"assistant",
                   "content":"".join(TOKENS)},"finish_reason":"stop"}],
               "usage":{"prompt_tokens":9,"completion_tokens":5,"total_tokens":14}}
        b = json.dumps(out).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(b))); self.end_headers()
        self.wfile.write(b)

    def _chunk(self, s):
        data = s.encode()
        self.wfile.write(f"{len(data):X}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()
    def log_message(self,*a): pass

ThreadingHTTPServer(('127.0.0.1',9000), H).serve_forever()
