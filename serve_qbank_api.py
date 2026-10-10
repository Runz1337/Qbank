#!/usr/bin/env python3
"""
Lightweight REST API server for the QBank database.
Can be started immediately on any port to provide JSON endpoints and image streaming to your app or agent.
"""

import os, json, argparse, mimetypes
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, unquote

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "qbank_download", "qbank")
CATALOG_PATH = os.path.join(HERE, "mega_qbank_catalog.json")

class QBankAPIHandler(BaseHTTPRequestHandler):
    def send_json(self, data, status=200):
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = unquote(parsed.path).strip("/")
        parts = path.split("/")

        # Health
        if path == "" or path == "api":
            return self.send_json({"status": "ok", "service": "QBank API Server", "version": "1.0"})

        # Master Catalog
        if path == "api/mega/catalog":
            if os.path.exists(CATALOG_PATH):
                return self.send_json(json.load(open(CATALOG_PATH)))
            return self.send_json({"error": "Catalog not yet generated"}, 404)

        # GET /api/sets
        if path == "api/sets":
            sets = [
                d for d in os.listdir(ROOT)
                if os.path.isdir(os.path.join(ROOT, d)) and not d.endswith('-media')
            ]
            return self.send_json({"sets": sorted(sets)})

        # GET /api/sets/:set/subjects
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "sets" and parts[3] == "subjects":
            set_name = parts[2]
            idx_file = os.path.join(ROOT, set_name, "index.json")
            if os.path.exists(idx_file):
                idx = json.load(open(idx_file))
                return self.send_json(idx.get("subjects", {}))
            return self.send_json({"error": "Set index not found"}, 404)

        # GET /api/sets/:set/subjects/:subject
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "sets":
            set_name = parts[2]
            sub_slug = parts[3]
            sub_file = os.path.join(ROOT, set_name, f"{sub_slug}.json")
            if os.path.exists(sub_file):
                return self.send_json(json.load(open(sub_file)))
            return self.send_json({"error": "Subject file not found"}, 404)

        # GET /api/sets/:set/lessons/:subject/:lessonFile
        if len(parts) == 6 and parts[0] == "api" and parts[1] == "sets" and parts[3] == "lessons":
            set_name = parts[2]
            subject = parts[4]
            lesson_file = parts[5]
            l_path = os.path.join(ROOT, set_name, "lessons", subject, lesson_file)
            if os.path.exists(l_path):
                return self.send_json(json.load(open(l_path)))
            return self.send_json({"error": "Lesson file not found"}, 404)

        # Stream Image: GET /api/sets/:set/images/:subject/:filename
        if len(parts) == 6 and parts[0] == "api" and parts[1] == "sets" and parts[3] == "images":
            set_name = parts[2]
            subject = parts[4]
            img_file = parts[5]
            # Try subject images or lessons subject images
            img_paths = [
                os.path.join(ROOT, set_name, "lessons", subject, "images", img_file),
                os.path.join(ROOT, set_name, subject, "images", img_file)
            ]
            for ip in img_paths:
                if os.path.exists(ip):
                    mime, _ = mimetypes.guess_type(ip)
                    mime = mime or "application/octet-stream"
                    with open(ip, "rb") as f:
                        data = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", mime)
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
            return self.send_json({"error": "Image file not found"}, 404)

        return self.send_json({"error": "Endpoint not found"}, 404)

def run(port=8080):
    server = HTTPServer(("0.0.0.0", port), QBankAPIHandler)
    print(f"[*] QBank API Server listening on http://0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    run(args.port)
