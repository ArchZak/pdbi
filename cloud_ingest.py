"""SQS page ingestion with S3 checkpoints, immutable page parts and replay."""
import argparse
import io
import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

import live_sources
from cloud_exports import publish
from pipeline import PART, SOURCES, checkpoint, normalize, part_directory, reconcile


def status(exc):
    return exc.response["ResponseMetadata"]["HTTPStatusCode"]


def initial(attempt=1, restarts=0):
    return dict(page=0, attempt=attempt, restarts=restarts, paused=False, cursor=None, release=None, complete=False)


class Ingestion:
    def __init__(self, bucket, queue):
        self.s3 = boto3.client("s3")
        self.sqs = boto3.client("sqs")
        self.bucket = bucket
        self.queue = queue

    def object(self, run, name):
        return dict(Bucket=self.bucket, Key=f"runs/{UUID(run)}/{name}")

    def read_json(self, run, name):
        obj = self.s3.get_object(**self.object(run, name))
        return json.loads(obj["Body"].read()), obj["ETag"]

    def send(self, run, source, progress):
        message = dict(run_id=run, source=source, attempt=progress["attempt"], page=progress["page"])
        self.sqs.send_message(QueueUrl=self.queue, MessageBody=json.dumps(message))

    def submit(self, run, organism, motif):
        if organism <= 0:
            raise ValueError("Expected a positive taxonomy ID")
        query = dict(organism=organism, motif=normalize(motif))
        state = dict(query=query, sources={s: initial() for s in SOURCES}, complete=False)
        try:
            self.s3.put_object(**self.object(run, "query.json"), Body=json.dumps(state), IfNoneMatch="*")
        except ClientError as exc:
            if status(exc) != 412:
                raise
            state, _ = self.read_json(run, "query.json")
            if state["query"] != query:
                raise ValueError("Run ID belongs to a different query") from exc
        self.resume(run)

    def resume(self, run):
        state, etag = self.read_json(run, "query.json")
        if state["complete"]:
            return
        pending = [s for s in SOURCES if not state["sources"][s]["complete"]]
        if any(state["sources"][s]["paused"] for s in pending):
            for source in pending:
                state["sources"][source].update(paused=False, restarts=0)
            self.s3.put_object(**self.object(run, "query.json"), Body=json.dumps(state), IfMatch=etag)
        for source in pending:
            self.send(run, source, state["sources"][source])
        if not pending:
            self.send(run, "finalize", initial())

    def manifest(self, run, progress, organism):
        name = f'parts/pdb/{progress["attempt"]}/ids.json'
        try:
            return self.read_json(run, name)[0]
        except ClientError as exc:
            if status(exc) != 404:
                raise
        ids = live_sources.pdb_ids(organism)
        try:
            self.s3.put_object(**self.object(run, name), Body=json.dumps(ids), IfNoneMatch="*")
            return ids
        except ClientError as exc:
            if status(exc) != 412:
                raise
            return self.read_json(run, name)[0]

    def part(self, run, source, progress, query):
        key = self.object(run, f'parts/{source}/{progress["attempt"]}/{progress["page"]}.parquet')
        try:
            return json.loads(self.s3.head_object(**key)["Metadata"]["continuation"])
        except ClientError as exc:
            if status(exc) != 404:
                raise
        if source == "uniprot":
            rows, cursor, release = live_sources.uniprot_page(query, progress["cursor"], progress["release"])
        else:
            ids = self.manifest(run, progress, query["organism"])
            rows, cursor, release = live_sources.pdb_page(query, ids, progress["cursor"])
        if cursor is not None and cursor == progress["cursor"]:
            raise ValueError("Source cursor did not advance")
        continuation = dict(cursor=cursor, release=release, complete=cursor is None)
        stream = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=PART), stream)
        try:
            self.s3.put_object(**key, Body=stream.getvalue(), IfNoneMatch="*",
                               Metadata={"continuation": json.dumps(continuation)})
        except ClientError as exc:
            if status(exc) != 412:
                raise
            return json.loads(self.s3.head_object(**key)["Metadata"]["continuation"])
        return continuation

    def finalize(self, run, state):
        if not all(p["complete"] for p in state["sources"].values()):
            raise ValueError("Cannot finalize unfinished sources")
        with TemporaryDirectory() as scratch:
            local = Path(scratch)
            for source, progress in state["sources"].items():
                dest = part_directory(local, source, progress)
                dest.mkdir(parents=True)
                for page in range(progress["page"]):
                    name = f"{page}.parquet"
                    key = self.object(run, str((dest / name).relative_to(local)))["Key"]
                    self.s3.download_file(self.bucket, key, str(dest / name))
            reconcile(local, state)
            checkpoint(local, dict(state, complete=True))
            publish(self.s3, self.bucket, run, local)

    def step(self, msg):
        for retry in range(5):
            try:
                return self._step(msg)
            except ClientError as exc:
                if status(exc) not in (409, 412) or retry == 4:
                    raise

    def _step(self, msg):
        run, source = msg["run_id"], msg["source"]
        state, etag = self.read_json(run, "query.json")
        if state["complete"]:
            return
        if source == "finalize":
            self.finalize(run, state)
            state["complete"] = True
        else:
            progress = state["sources"][source]
            if progress["paused"]:
                return
            token = (msg["attempt"], msg["page"])
            current = (progress["attempt"], progress["page"])
            if token != current or progress["complete"]:
                # A prior invocation may have committed and died before sending its successor.
                if progress["complete"]:
                    if all(p["complete"] for p in state["sources"].values()):
                        self.send(run, "finalize", initial())
                elif token == (current[0], current[1] - 1) or (token[0] == current[0] - 1 and current[1] == 0):
                    self.send(run, source, progress)
                return
            try:
                following = self.part(run, source, progress, state["query"])
                progress.update(following, page=progress["page"] + 1)
            except live_sources.ExpiredCursor:
                fresh = initial(progress["attempt"] + 1, progress["restarts"] + 1)
                fresh["paused"] = fresh["restarts"] >= 3
                state["sources"][source] = fresh
        # On contention, SQS retries the page and adopts the already persisted part.
        self.s3.put_object(**self.object(run, "query.json"), Body=json.dumps(state), IfMatch=etag)
        if not state["complete"]:
            if state["sources"][source]["paused"]:
                return
            if not state["sources"][source]["complete"]:
                self.send(run, source, state["sources"][source])
            elif all(p["complete"] for p in state["sources"].values()):
                self.send(run, "finalize", initial())


def handler(event, context):
    worker = Ingestion(os.environ["RESULT_BUCKET"], os.environ["INGEST_QUEUE_URL"])
    failures = []
    for record in event["Records"]:
        try:
            worker.step(json.loads(record["body"]))
        except Exception:
            logging.exception("Ingestion failed for message %s", record["messageId"])
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["submit", "resume", "status"])
    parser.add_argument("run_id", type=UUID)
    parser.add_argument("--organism", type=int)
    parser.add_argument("--motif")
    args = parser.parse_args()
    worker = Ingestion(os.environ["RESULT_BUCKET"], os.environ["INGEST_QUEUE_URL"])
    run = str(args.run_id)
    if args.command == "submit":
        if not args.organism or not args.motif:
            parser.error("submit requires --organism and --motif")
        worker.submit(run, args.organism, args.motif)
    elif args.command == "resume":
        worker.resume(run)
    print(json.dumps(worker.read_json(run, "query.json")[0], indent=2))


if __name__ == "__main__":
    main()
