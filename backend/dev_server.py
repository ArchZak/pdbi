"""Local development host for the query API and fixture-backed workers."""
import argparse
import importlib
import json
import logging
import os
import re
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import boto3
from moto import mock_aws

import cloud_exports
import query_api
from cloud_ingest import Ingestion
from pipeline import normalize

BACKEND = Path(__file__).resolve().parent
ASSETS = {"/": ("index.html", "text/html"), "/app.css": ("app.css", "text/css"),
          "/app.js": ("app.js", "text/javascript")}
RUN_PATH = re.compile(r"/api/query/([0-9a-f-]{36})(/resume|/download|/file)?")


class FixtureIngestion(Ingestion):
    def fetch_page(self, run, source, progress, query):
        pages = json.loads((BACKEND / "fixtures" / f"{source}.json").read_text())
        index = int(progress["cursor"] or 0)
        rows = []
        for record in pages[index]:
            seq = normalize(record["sequence"])
            if record["organism"] == query["organism"] and query["motif"] in seq:
                rows.append(dict(record, source=source, sequence=seq))
        cursor = str(index + 1) if index + 1 < len(pages) else None
        return rows, cursor, "fixture"


class LocalServer(ThreadingHTTPServer):
    def setup_workers(self, provider):
        self.provider = provider
        self.lock = threading.Lock()
        sqs = boto3.client("sqs")
        bucket = "pdbi-local-results"
        queue = sqs.create_queue(QueueName="ingest")["QueueUrl"]
        csv = sqs.create_queue(QueueName="exports")["QueueUrl"]
        os.environ.update(RESULT_BUCKET=bucket, INGEST_QUEUE_URL=queue, CSV_QUEUE_URL=csv,
                          CSV_DLQ_ARN="local-csv-dlq")
        self.worker = FixtureIngestion(bucket, queue)
        self.worker.s3.create_bucket(Bucket=bucket)
        self.queues = [(queue, "local-ingest-dlq"), (csv, "local-csv-dlq")]

    def service_actions(self):
        with self.lock:
            for queue, dead in self.queues:
                messages = self.worker.sqs.receive_message(QueueUrl=queue, VisibilityTimeout=1,
                    AttributeNames=["ApproximateReceiveCount"]).get("Messages", [])
                for msg in messages:
                    payload = json.loads(msg["Body"])
                    try:
                        if queue == self.worker.queue:
                            self.worker.step(payload)
                        else:
                            cloud_exports.materialize_csv(self.worker.s3, self.worker.bucket, payload["run_id"])
                    except Exception:
                        logging.exception("Local worker failed")
                        if int(msg["Attributes"]["ApproximateReceiveCount"]) < 5:
                            continue
                        query_api.record_dead_letter(self.worker, dict(body=msg["Body"], eventSourceARN=dead))
                    self.worker.sqs.delete_message(QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"])


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, data, kind="application/json", filename=None):
        if isinstance(data, dict):
            data = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.dispatch()

    def do_POST(self):
        self.dispatch()

    def dispatch(self):
        port = self.server.server_port
        hosts = {f"localhost:{port}", f"127.0.0.1:{port}"}
        if self.headers.get("Host") not in hosts or self.headers.get("Origin") not in (
                None, *(f"http://{host}" for host in hosts)):
            return self.reply(403, {"error": "Use the local server origin"})
        url = urlsplit(self.path)
        if self.command == "GET" and url.path in ASSETS:
            name, kind = ASSETS[url.path]
            return self.reply(200, (BACKEND.parent / "frontend" / name).read_bytes(), kind + "; charset=utf-8")
        try:
            size = int(self.headers.get("Content-Length", 0))
            if not 0 <= size <= 65536:
                return self.reply(413, {"error": "Request body is too large"})
            body = self.rfile.read(size).decode()
            if url.path == "/api/wizard/draft" and self.command == "POST":
                payload = json.loads(body)
                text = payload.get("text") if isinstance(payload, dict) else None
                if not isinstance(text, str) or not text.strip():
                    return self.reply(400, {"error": "Enter a query description"})
                return self.reply(200, self.server.provider(text))
            with self.server.lock:
                self.query(url, body)
        except (ValueError, UnicodeError):
            self.reply(400, {"error": "Invalid request"})
        except Exception:
            logging.exception("Local request failed")
            self.reply(500, {"error": "Local request failed. See the server log."})

    def query(self, url, body):
        route = url.path
        match = RUN_PATH.fullmatch(route)
        run = None
        stream = False
        if match:
            run, suffix = match.groups()
            stream = suffix == "/file" and self.command == "GET"
            route = "/api/query/{run_id}" + ("/download" if stream else suffix or "")
        elif route not in ("/api/query", "/api/query/validate"):
            return self.reply(404, {"error": "Unknown route"})
        params = dict(parse_qsl(url.query))
        event = dict(routeKey=f"{self.command} {route}", body=body, headers=dict(self.headers),
                     pathParameters={"run_id": run}, queryStringParameters=params,
                     requestContext={"authorizer": {"jwt": {"claims": {"iss": "local", "sub": "researcher"}}}})
        response = query_api.entrypoint(event, None)
        result = json.loads(response["body"])
        if result.get("status") == "ready" and run:
            fmt = params["format"]
            if stream:
                worker = self.server.worker
                obj = worker.s3.get_object(**worker.object(run, f"result.{fmt}"))
                return self.reply(200, obj["Body"].read(), obj["ContentType"], f"{run}.{fmt}")
            result["url"] = f"/api/query/{run}/file?format={fmt}"
            result.pop("expires_in_seconds", None)
        self.reply(response["statusCode"], result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open", action="store_true")
    args = parser.parse_args()
    module, func = os.environ.get("PDBI_WIZARD_PROVIDER", "wizard_provider:draft_query").split(":")
    provider = getattr(importlib.import_module(module), func)
    os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="testing",
                      AWS_SECRET_ACCESS_KEY="testing", AWS_EC2_METADATA_DISABLED="true")
    with mock_aws(), LocalServer(("127.0.0.1", args.port), Handler) as server:
        server.setup_workers(provider)
        url = f"http://127.0.0.1:{server.server_port}"
        print(f"pdbi local workspace: {url}", flush=True)
        if args.open:
            webbrowser.open(url)
        try:
            server.serve_forever(poll_interval=0.25)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
