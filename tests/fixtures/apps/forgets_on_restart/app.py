from __future__ import annotations

import argparse
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--db", required=True)
    args = p.parse_args()
    # deliberately volatile: ignores --db, keeps everything in memory → restart_persists must FAIL
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.execute("CREATE TABLE IF NOT EXISTS urls (code TEXT PRIMARY KEY, url TEXT NOT NULL)")
    db.commit()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_json(self, status, body):
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            if self.path != "/shorten":
                return self.send_json(404, {"error": "not found"})
            data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            code = "abc123"
            db.execute("INSERT OR REPLACE INTO urls(code,url) VALUES (?,?)", (code, data["url"]))
            db.commit()
            self.send_json(201, {"code": code})

        def do_GET(self):
            if self.path == "/health":
                return self.send_json(200, {"ok": True})
            if self.path == "/stats":
                return self.send_json(200, {"urls": db.execute("SELECT count(*) FROM urls").fetchone()[0]})
            row = db.execute("SELECT url FROM urls WHERE code=?", (self.path.removeprefix("/"),)).fetchone()
            if row:
                self.send_response(302)
                self.send_header("Location", row[0])
                self.end_headers()
                return
            self.send_json(404, {"error": "not found"})

    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
