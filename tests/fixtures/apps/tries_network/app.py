import argparse
import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer

# If egress is available this service becomes healthy, making the fixture test fail.
# Under the verifier's --network none, connect raises and the process exits before bind.
with socket.create_connection(("1.1.1.1", 80), timeout=2):
    pass

p = argparse.ArgumentParser()
p.add_argument("--port", type=int, required=True)
p.add_argument("--db")
args = p.parse_args()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        raw = json.dumps({"ok": True}).encode()
        self.send_response(200 if self.path == "/health" else 404)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
