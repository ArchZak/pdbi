"""API Gateway v2 handler for reviewing, running and downloading a structured query."""
import base64
import json
import logging
import os
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

from botocore.exceptions import ClientError

from cloud_ingest import Ingestion, status
from pipeline import normalize


class ApiError(Exception):
    def __init__(self, code, message, fields=None):
        self.code = code
        self.body = {"error": message}
        if fields:
            self.body["fields"] = fields


def validate(event):
    raw = event.get("body") or ""
    try:
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw, validate=True)
        data = json.loads(raw)
    except (ValueError, TypeError):
        raise ApiError(400, "Expected a JSON object")
    if not isinstance(data, dict):
        raise ApiError(400, "Expected a JSON object")
    fields = {key: "Unknown field" for key in data.keys() - {"organism", "motif"}}
    organism = data.get("organism")
    if type(organism) is not int or not 0 < organism < 2**63:
        fields["organism"] = "Enter a positive integer taxonomy ID"
    try:
        motif = normalize(data["motif"] if isinstance(data.get("motif"), str) else "")
    except ValueError:
        fields["motif"] = "Enter a literal motif using one-letter amino-acid codes"
    if fields:
        raise ApiError(400, "Invalid query", fields)
    return dict(organism=organism, motif=motif)


def summary(run, state):
    sources = {s: {"pages_completed": p["page"], "complete": p["complete"], "paused": p["paused"]}
               for s, p in state["sources"].items()}
    paused = bool(state.get("finalize_paused") or any(p["paused"] for p in sources.values()))
    label = "running"
    if state["complete"]:
        label = "complete"
    elif paused:
        label = "needs_resume"
    return dict(run_id=run, query=state["query"], status=label, sources=sources,
                can_resume=label == "needs_resume", can_download=state["complete"])


def csv_request(worker, run, enqueue):
    obj = worker.object(run, "csv-request.json")
    try:
        job, etag = worker.read_json(run, "csv-request.json")
    except ClientError as exc:
        if status(exc) != 404:
            raise
        job, etag = {}, None
    if not enqueue:
        return job.get("state", "not_requested")
    if job.get("state") == "queued":
        return "preparing"
    if job.get("state") != "sending":
        job = dict(state="sending", request_id=str(uuid4()))
        condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        etag = worker.s3.put_object(**obj, Body=json.dumps(job), **condition)["ETag"]
    worker.sqs.send_message(QueueUrl=os.environ["CSV_QUEUE_URL"],
                            MessageBody=json.dumps(dict(run_id=run, request_id=job["request_id"])))
    worker.s3.put_object(**obj, Body=json.dumps(dict(job, state="queued")), IfMatch=etag)
    return "preparing"


def handle(event):
    claims = (event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims") or {})
    if not claims.get("iss") or not claims.get("sub"):
        raise ApiError(401, "Sign in to run queries")
    owner = dict(issuer=claims["iss"], subject=claims["sub"])
    route = event["routeKey"]
    if route == "POST /api/query/validate":
        return 200, {"query": validate(event)}
    worker = Ingestion(os.environ["RESULT_BUCKET"], os.environ["INGEST_QUEUE_URL"])
    if route == "POST /api/query":
        query = validate(event)
        headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        try:
            key = str(UUID(headers.get("idempotency-key", "")))
        except ValueError:
            raise ApiError(400, "Supply a UUID Idempotency-Key header")
        run = str(uuid5(NAMESPACE_URL, json.dumps([owner["issuer"], owner["subject"], key])))
        try:
            worker.submit(run, **query, owner=owner)
        except ValueError:
            raise ApiError(409, "This key already identifies a different query")
        except PermissionError:
            raise ApiError(404, "Unknown run")
        return 202, dict(run_id=run, status_url=f"/api/query/{run}")

    try:
        run = str(UUID((event.get("pathParameters") or {}).get("run_id", "")))
        state, _ = worker.read_json(run, "query.json")
    except ValueError:
        raise ApiError(404, "Unknown run")
    except ClientError as exc:
        if status(exc) == 404:
            raise ApiError(404, "Unknown run")
        raise
    if state.get("owner") != owner:
        raise ApiError(404, "Unknown run")
    if route == "GET /api/query/{run_id}":
        return 200, summary(run, state)
    if route == "POST /api/query/{run_id}/resume":
        worker.resume(run)
        return 202, summary(run, worker.read_json(run, "query.json")[0])
    if route not in ("POST /api/query/{run_id}/download", "GET /api/query/{run_id}/download"):
        raise ApiError(404, "Unknown route")
    fmt = (event.get("queryStringParameters") or {}).get("format")
    if fmt not in ("csv", "parquet"):
        raise ApiError(400, "Choose csv or parquet")
    if not state["complete"]:
        raise ApiError(409, "Run is incomplete")
    obj = worker.object(run, f"result.{fmt}")
    if fmt == "csv":
        try:
            worker.s3.head_object(**obj)
        except ClientError as exc:
            if status(exc) != 404:
                raise
            phase = csv_request(worker, run, route.startswith("POST"))
            if phase in ("needs_retry", "not_requested"):
                return 200, {"status": phase}
            return 202, {"status": "preparing"}
    url = worker.s3.generate_presigned_url("get_object", Params=obj, ExpiresIn=300)
    return 200, {"status": "ready", "url": url, "expires_in_seconds": 300}


def handler(event, context):
    for retry in range(4):
        try:
            code, body = handle(event)
            break
        except ApiError as exc:
            code, body = exc.code, exc.body
            break
        except ClientError as exc:
            if status(exc) in (409, 412) and retry < 3:
                continue
            logging.exception("AWS request failed")
            code, body = 503, {"error": "Temporarily unavailable. Retry the request."}
            break
    return {"statusCode": code, "headers": {"content-type": "application/json", "cache-control": "no-store"},
            "body": json.dumps(body)}


def record_dead_letter(worker, record):
    msg = json.loads(record["body"])
    run = msg["run_id"]
    if record["eventSourceARN"] == os.environ["CSV_DLQ_ARN"]:
        if "request_id" not in msg:  # Older trusted CLI export request.
            return
        name = "csv-request.json"
        state, etag = worker.read_json(run, name)
        if state["request_id"] != msg["request_id"]:
            return
        state["state"] = "needs_retry"
    else:
        name = "query.json"
        state, etag = worker.read_json(run, name)
        if state["complete"]:
            return
        source = msg["source"]
        if source == "finalize":
            if msg.get("generation", 0) != state.get("finalize_generation", 0):
                return
            state["finalize_paused"] = True
        else:
            p = state["sources"][source]
            if msg.get("generation", 0) != p.get("generation", 0):
                return
            if p["complete"]:
                if not all(s["complete"] for s in state["sources"].values()):
                    return
                state["finalize_paused"] = True
            elif (msg["attempt"] == p["attempt"] and msg["page"] in (p["page"], p["page"] - 1)
                  or msg["attempt"] + 1 == p["attempt"] and p["page"] == 0):
                p["paused"] = True
            else:
                return
    worker.s3.put_object(**worker.object(run, name), Body=json.dumps(state), IfMatch=etag)


def entrypoint(event, context):
    if "Records" not in event:
        return handler(event, context)
    worker = Ingestion(os.environ["RESULT_BUCKET"], os.environ["INGEST_QUEUE_URL"])
    failures = []
    for record in event["Records"]:
        try:
            record_dead_letter(worker, record)
        except Exception:
            logging.exception("Failed to record dead letter %s", record["messageId"])
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
