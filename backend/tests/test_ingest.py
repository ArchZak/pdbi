import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

import boto3
from moto import mock_aws

from cloud_ingest import Ingestion, handler
import live_sources
import pipeline


@mock_aws
class PageWorkers(unittest.TestCase):
    def setUp(self):
        cfg = patch.dict(os.environ, AWS_DEFAULT_REGION="us-east-1")
        cfg.start()
        self.addCleanup(cfg.stop)
        queue = boto3.client("sqs").create_queue(QueueName="ingest")["QueueUrl"]
        self.worker = Ingestion("pdbi-ingest-test", queue)
        self.worker.s3.create_bucket(Bucket=self.worker.bucket)
        self.run = str(uuid4())
        env = patch.dict(os.environ, RESULT_BUCKET=self.worker.bucket, INGEST_QUEUE_URL=queue)
        env.start()
        self.addCleanup(env.stop)
        self.worker.submit(self.run, 9606, "ACD")

    def drain(self):
        for _ in range(30):
            response = self.worker.sqs.receive_message(QueueUrl=self.worker.queue)
            if not response.get("Messages"):
                return
            msg = response["Messages"][0]
            event = {"Records": [{"messageId": msg["MessageId"], "body": msg["Body"]}]}
            self.assertEqual(handler(event, None), {"batchItemFailures": []})
            self.worker.sqs.delete_message(QueueUrl=self.worker.queue, ReceiptHandle=msg["ReceiptHandle"])
        self.fail("Unexpected continuation loop")

    def test_replay_adopts_part_and_repairs_delivery_gap(self):
        source = "uniprot"
        first = dict(run_id=self.run, source=source, attempt=1, page=0)
        rows = [dict(source=source, sequence="MACDEFG", organism=9606, id="DEMO-U1")]
        with patch("live_sources.uniprot_page", return_value=(rows, "next", "v1")) as fetch:
            real = self.worker.s3.put_object
            with patch.object(self.worker.s3, "put_object") as put:
                def crash(**kwargs):
                    if "IfMatch" in kwargs:
                        raise RuntimeError("before checkpoint")
                    return real(**kwargs)
                put.side_effect = crash
                with self.assertRaisesRegex(RuntimeError, "before checkpoint"):
                    self.worker.step(first)
            with patch.object(self.worker, "send", side_effect=RuntimeError("after checkpoint")):
                with self.assertRaisesRegex(RuntimeError, "after checkpoint"):
                    self.worker.step(first)
            self.worker.step(first)
            self.assertEqual(fetch.call_count, 1)
        with patch("live_sources.uniprot_page", return_value=([], None, "v1")), \
             patch("live_sources.pdb_ids", return_value=["one"]), \
             patch("live_sources.pdb_page", return_value=([dict(rows[0], source="pdb", id="one")], None, None)):
            self.drain()
        state = self.worker.read_json(self.run, "query.json")[0]
        self.assertTrue(state["complete"])
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "out"
            self.worker.s3.download_file(self.worker.bucket, f"runs/{self.run}/result.parquet", str(path))
            result = pipeline.pq.read_table(path).to_pylist()
        self.assertEqual(len(result), 1)
        self.assertEqual([r["id"] for r in result[0]["source_records"]], ["one", "DEMO-U1"])

    def test_expired_release_replaces_one_attempt(self):
        with patch("live_sources.uniprot_page", side_effect=[
                ([dict(source="uniprot", sequence="ACD", organism=9606, id="stale")], "old", "v1"),
                live_sources.ExpiredCursor(), ([], None, "v2")]), \
             patch("live_sources.pdb_ids", return_value=[]):
            self.drain()
        state = self.worker.read_json(self.run, "query.json")[0]
        self.assertTrue(state["complete"])
        self.assertEqual(state["sources"]["uniprot"]["attempt"], 2)
        self.assertEqual(state["sources"]["pdb"]["attempt"], 1)
        obj = self.worker.s3.get_object(**self.worker.object(self.run, "result.parquet"))
        self.assertEqual(pipeline.pq.read_table(pipeline.pa.BufferReader(obj["Body"].read())).num_rows, 0)

    def test_repeated_expiry_pauses_until_resume(self):
        with patch("live_sources.uniprot_page", side_effect=live_sources.ExpiredCursor), \
             patch("live_sources.pdb_ids", return_value=[]):
            self.drain()
        state = self.worker.read_json(self.run, "query.json")[0]
        self.assertTrue(state["sources"]["uniprot"]["paused"])
        self.assertFalse(state["complete"])
        self.worker.resume(self.run)
        with patch("live_sources.uniprot_page", return_value=([], None, "new")):
            self.drain()
        self.assertTrue(self.worker.read_json(self.run, "query.json")[0]["complete"])
