import json
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import boto3
from moto import mock_aws

import cloud_exports
from cloud_ingest import Ingestion
from query_api import entrypoint


@mock_aws
class QueryApiTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, AWS_DEFAULT_REGION="us-east-1")
        env.start()
        self.addCleanup(env.stop)
        sqs = boto3.client("sqs")
        ingest = sqs.create_queue(QueueName="ingest")["QueueUrl"]
        csv = sqs.create_queue(QueueName="csv")["QueueUrl"]
        env = patch.dict(os.environ, RESULT_BUCKET="result-bucket", INGEST_QUEUE_URL=ingest,
                         CSV_QUEUE_URL=csv, CSV_DLQ_ARN="csv-dlq")
        env.start()
        self.addCleanup(env.stop)
        self.worker = Ingestion("result-bucket", ingest)
        self.worker.s3.create_bucket(Bucket=self.worker.bucket)
        self.criteria = {"organism": 9606, "motif": "a cd"}
        self.key = str(uuid4())

    def call(self, route, *, criteria=None, user="alice", run=None, fmt=None):
        claims = {"sub": user, "iss": "cognito-issuer"} if user else {}
        event = dict(routeKey=route, requestContext={"authorizer": {"jwt": {"claims": claims}}},
                     headers={"Idempotency-Key": self.key}, body=json.dumps(criteria),
                     pathParameters={"run_id": run}, queryStringParameters={"format": fmt})
        response = entrypoint(event, None)
        return response["statusCode"], json.loads(response["body"])

    def test_end_to_end(self):
        submit = "POST /api/query"
        status = "GET /api/query/{run_id}"
        resume = "POST /api/query/{run_id}/resume"
        request = "POST /api/query/{run_id}/download"
        poll = "GET /api/query/{run_id}/download"
        self.assertEqual(self.call(submit + "/validate", criteria=self.criteria),
                         (200, {"query": {"organism": 9606, "motif": "ACD"}}))
        self.assertEqual(self.worker.s3.list_objects_v2(Bucket=self.worker.bucket)["KeyCount"], 0)
        code, first = self.call(submit, criteria=self.criteria)
        self.assertEqual(code, 202)
        run = first["run_id"]
        self.assertEqual(self.call(submit, criteria=self.criteria), (202, first))
        self.assertEqual(self.call(submit, criteria=dict(self.criteria, organism=1))[0], 409)
        for route in (status, resume, request, poll):
            self.assertEqual(self.call(route, user="bob", run=run, fmt="csv"), (404, {"error": "Unknown run"}))
        record = {"messageId": "dead", "eventSourceARN": "ingest-dlq",
                  "body": json.dumps(dict(run_id=run, source="uniprot", attempt=1, page=0))}
        self.assertEqual(entrypoint({"Records": [record]}, None)["batchItemFailures"], [])
        self.call(submit, criteria=self.criteria)
        self.assertEqual(self.call(status, run=run)[1]["status"], "needs_resume")
        self.assertEqual(self.call(resume, run=run)[1]["status"], "running")
        entrypoint({"Records": [record]}, None)  # Late delivery from before resume.
        self.assertFalse(self.call(status, run=run)[1]["can_resume"])
        rows = [dict(source="uniprot", id="A", sequence="MACD", organism=9606)]
        with patch("live_sources.uniprot_page", return_value=(rows, None, "v1")), \
             patch("live_sources.pdb_ids", return_value=[]):
            for _ in range(20):
                msgs = self.worker.sqs.receive_message(QueueUrl=self.worker.queue).get("Messages", [])
                if not msgs:
                    break
                msg = msgs[0]
                self.worker.step(json.loads(msg["Body"]))
                self.worker.sqs.delete_message(QueueUrl=self.worker.queue, ReceiptHandle=msg["ReceiptHandle"])
        self.assertTrue(self.call(status, run=run)[1]["can_download"])
        self.assertIn("X-Amz-Signature=", self.call(request, run=run, fmt="parquet")[1]["url"])
        self.assertEqual(self.call(request, run=run, fmt="csv"), (202, {"status": "preparing"}))
        self.assertEqual(self.call(poll, run=run, fmt="csv"), (202, {"status": "preparing"}))
        queue = os.environ["CSV_QUEUE_URL"]
        msg = self.worker.sqs.receive_message(QueueUrl=queue)["Messages"][0]
        record = {"messageId": "csv-dead", "eventSourceARN": "csv-dlq", "body": msg["Body"]}
        entrypoint({"Records": [record]}, None)
        self.assertEqual(self.call(poll, run=run, fmt="csv")[1]["status"], "needs_retry")
        self.call(request, run=run, fmt="csv")
        msg = self.worker.sqs.receive_message(QueueUrl=queue)["Messages"][0]
        cloud_exports.handler({"Records": [{"messageId": msg["MessageId"], "body": msg["Body"]}]}, None)
        self.assertEqual(self.call(poll, run=run, fmt="csv")[1]["status"], "ready")
        self.assertEqual(self.call(poll, user="bob", run=run, fmt="csv")[0], 404)
        self.assertNotEqual(run, self.call(submit, user="bob", criteria=self.criteria)[1]["run_id"])

    def test_invalid_inputs(self):
        route = "POST /api/query/validate"
        self.assertEqual(self.call(route, user=None)[0], 401)
        code, body = self.call(route, criteria={"organism": True, "motif": "*", "owner": "bob"})
        self.assertEqual(code, 400)
        self.assertEqual(set(body["fields"]), {"organism", "motif", "owner"})
        self.key = ""
        self.assertEqual(self.call("POST /api/query", criteria=self.criteria)[0], 400)

    def test_retry_after_partial_submit(self):
        actual_send = Ingestion.send
        def partial_send(worker, run, source, progress):
            if source == "pdb":
                from botocore.exceptions import ClientError
                raise ClientError({"Error": {"Code": "ServiceUnavailable"},
                                   "ResponseMetadata": {"HTTPStatusCode": 503}}, "SendMessage")
            actual_send(worker, run, source, progress)
        with patch.object(Ingestion, "send", partial_send), self.assertLogs(level="ERROR"):
            self.assertEqual(self.call("POST /api/query", criteria=self.criteria)[0], 503)
        code, result = self.call("POST /api/query", criteria=self.criteria)
        self.assertEqual(code, 202)
        state, _ = self.worker.read_json(result["run_id"], "query.json")
        self.assertFalse(state["dispatch_pending"])
        self.assertEqual(self.worker.s3.list_objects_v2(Bucket=self.worker.bucket)["KeyCount"], 1)
