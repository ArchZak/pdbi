import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

import boto3
from moto import mock_aws

import cloud_exports as cloud
import pipeline


@mock_aws
class CloudExportTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, AWS_DEFAULT_REGION="us-east-1", RESULT_BUCKET="pdbi-results")
        self.env.start()
        self.addCleanup(self.env.stop)
        self.s3 = boto3.client("s3")
        self.bucket = os.environ["RESULT_BUCKET"]
        self.s3.create_bucket(Bucket=self.bucket)
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.run = Path(tmp.name) / "run"
        pipeline.start(self.run, Path(pipeline.__file__).with_name("fixtures"), 9606, "ACD")
        self.run_id = str(uuid4())

    def test_publish_replay_and_sqs_retry(self):
        key = cloud.publish(self.s3, self.bucket, self.run_id, self.run)
        self.assertEqual(cloud.publish(self.s3, self.bucket, self.run_id, self.run), key)
        sqs = boto3.client("sqs")
        url = sqs.create_queue(QueueName="exports")["QueueUrl"]
        sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"run_id": self.run_id}))
        message = sqs.receive_message(QueueUrl=url)["Messages"][0]
        record = {"messageId": message["MessageId"], "body": message["Body"]}
        event = {"Records": [record, {"messageId": "invalid", "body": "{}"}]}
        with self.assertLogs(level="ERROR"):
            result = cloud.handler(event, None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "invalid"}]})
        with patch.object(cloud, "write_csv", side_effect=AssertionError("cache missed")):
            self.assertEqual(cloud.handler({"Records": [record]}, None), {"batchItemFailures": []})
        saved = self.s3.get_object(Bucket=self.bucket, Key=cloud.result_key(self.run_id, "csv"))
        self.assertEqual(saved["Body"].read(), pipeline.download(self.run, "csv").read_bytes())

    def test_conflicting_publish_preserves_original(self):
        key = cloud.publish(self.s3, self.bucket, self.run_id, self.run)
        other = self.run.parent / "other"
        pipeline.start(other, Path(pipeline.__file__).with_name("fixtures"), 9606, "XXX")
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            cloud.publish(self.s3, self.bucket, self.run_id, other)
        self.assertEqual(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read(),
                         pipeline.download(self.run, "parquet").read_bytes())
