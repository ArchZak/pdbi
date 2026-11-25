"""Publish a completed run and materialize its CSV on demand from SQS."""
import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import boto3
from botocore.exceptions import ClientError

from pipeline import download, write_csv


def result_key(run_id, fmt):
    return f"runs/{UUID(run_id)}/result.{fmt}"


def put_once(s3, bucket, key, path, metadata, content_type):
    """Publish atomically and verify an existing winner on replay."""
    dest = dict(Bucket=bucket, Key=key)
    attrs = dict(Metadata=metadata, ContentType=content_type)
    try:
        with path.open("rb") as body:
            if path.stat().st_size <= 64 * 1024**2:
                s3.put_object(**dest, **attrs, Body=body, IfNoneMatch="*")
                return
            upload = s3.create_multipart_upload(**dest, **attrs)["UploadId"]
            try:
                parts = []
                chunk_size = max(16 * 1024**2, (path.stat().st_size + 9999) // 10000)
                while chunk := body.read(chunk_size):
                    number = len(parts) + 1
                    part = s3.upload_part(**dest, UploadId=upload, PartNumber=number, Body=chunk)
                    parts.append(dict(PartNumber=number, ETag=part["ETag"]))
                s3.complete_multipart_upload(**dest, UploadId=upload,
                                             MultipartUpload={"Parts": parts}, IfNoneMatch="*")
            except Exception:
                s3.abort_multipart_upload(**dest, UploadId=upload)
                raise
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise
        if s3.head_object(**dest)["Metadata"] != metadata:
            raise ValueError(f"Conflicting saved result at {key}") from exc


def publish(s3, bucket, run_id, run):
    parquet = download(run, "parquet")
    with parquet.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    key = result_key(run_id, "parquet")
    put_once(s3, bucket, key, parquet, {"sha256": digest}, "application/vnd.apache.parquet")
    return key


def materialize_csv(s3, bucket, run_id):
    src = result_key(run_id, "parquet")
    dest = result_key(run_id, "csv")
    source = s3.head_object(Bucket=bucket, Key=src)
    metadata = {"parquet-sha256": source["Metadata"]["sha256"]}
    try:
        prior = s3.head_object(Bucket=bucket, Key=dest)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
    else:
        if prior["Metadata"] != metadata:
            raise ValueError(f"Conflicting saved result at {dest}")
        return
    with TemporaryDirectory() as scratch:
        parquet = Path(scratch) / "input.parquet"
        csv_file = Path(scratch) / "result.csv"
        s3.download_file(bucket, src, str(parquet))
        write_csv(parquet, csv_file)
        put_once(s3, bucket, dest, csv_file, metadata, "text/csv; charset=utf-8")


def handler(event, context):
    failures = []
    s3 = boto3.client("s3")
    for record in event["Records"]:
        try:
            materialize_csv(s3, os.environ["RESULT_BUCKET"], json.loads(record["body"])["run_id"])
        except Exception:
            logging.exception("CSV export failed for SQS message %s", record["messageId"])
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    put = commands.add_parser("publish")
    put.add_argument("run", type=Path)
    put.add_argument("bucket")
    put.add_argument("run_id", type=UUID)
    request = commands.add_parser("request-csv")
    request.add_argument("queue_url")
    request.add_argument("run_id", type=UUID)
    args = parser.parse_args()
    if args.command == "publish":
        print(publish(boto3.client("s3"), args.bucket, str(args.run_id), args.run))
    else:
        msg = json.dumps({"run_id": str(args.run_id)})
        print(boto3.client("sqs").send_message(QueueUrl=args.queue_url, MessageBody=msg)["MessageId"])


if __name__ == "__main__":
    main()
