# pdbi
CS1660 Project

## Repository layout

- `backend/` contains the API, ingestion and export workers, local server, wizard provider, tests, fixtures, and Lambda Dockerfile.
- `frontend/` contains the browser workspace.
- `infra/` contains the Terraform modules.
- `docs/` contains the glossary and architecture decisions.

Run the commands below from the repository root.

## Local browser workspace

From the repository root run `pixi run dev`. The fixture workspace opens at
http://127.0.0.1:8765. For headless use or a different port, run
`pixi run -e test python backend/dev_server.py --port 8766`.

Choose **Draft with wizard**, review taxon 9606 and motif ACD, then **Run query**.
The manual form uses the same fields. Once both sources finish, download Parquet
or CSV. The sample yields three distinct sequences. Ctrl-C ends the session
and clears emulated runs. Downloaded files stay on your machine.

The loopback server supplies a fixed development identity, dispatches to the
shared query API, and processes fixture pages through the ingestion and export
workers with Moto S3 and SQS. Local downloads stream the saved object after
the API verifies access. No AWS account or model key is required.

### Plug in a wizard

The server loads `wizard_provider:draft_query` by default. To use your own
Python function, set `PDBI_WIZARD_PROVIDER=my_module:draft_query` when starting
the server. Place the module in `backend/` or on the Python import path. The function accepts a
plain string and returns either `{"organism": 9606, "motif": "ACD"}` or
`{"clarification": "Which organism should I search?"}`. The researcher can revise
the prompt and draft again. Keep model credentials in the server environment.
The browser sends proposed fields through shared validation and waits for Run.

## Local query pipeline

Run from the repository root with Pixi on Apple Silicon macOS or x86-64 Linux.

```sh
pixi install --locked
pixi run pipeline run /tmp/pdbi-demo --organism 9606 --motif ACD
pixi run pipeline download /tmp/pdbi-demo parquet
pixi run pipeline download /tmp/pdbi-demo csv
pixi run -e test test
```

Use a fresh run directory for each new query. To continue an interrupted run, use
`pixi run pipeline resume /tmp/pdbi-demo`. A download command prints the path
to the saved file. CSV conversion streams from the completed Parquet file on
first request.

The two paginated JSON fixtures contain synthetic records that stand in for
UniProt and PDB responses. Use `--fixtures DIRECTORY` to supply a different
pair of files in the same format. Each outer array contains pages. Each record
has a source identifier, source-organism taxonomy ID, and one-letter sequence.
PDB fixture sequences represent deposited canonical polymer entity sequences.
An optional parent accession stays in the page data as a mapping.

The example returns three sequences. MACDEFG carries both source identifiers,
ACD is the shorter PDB construct, and WACDW appears only in UniProt. A matching
sequence from another organism is excluded. Checkpoints track each source
independently. Changing an unfinished fixture invalidates that source attempt.
A completed run can be downloaded even when the fixtures are unavailable.

Reconciliation uses a temporary SQLite index on disk to collect unique
sequence/source pairs. A fixed row order, sorted provenance, fixed row-group
size, and pinned Parquet writer keep exports repeatable in the same runtime.

## AWS pipeline exports

The Terraform module in `infra/pipeline` provisions a private results bucket, export
queue, dead-letter queue, and Lambda consumer. Its inputs are an environment-specific
`name` and the `image_uri` of the worker in regional ECR, pinned by digest.
Build the image from this directory for `linux/amd64` using
`docker buildx build -f backend/Dockerfile --platform linux/amd64 --provenance=false --load -t pdbi-export .`.
The environment stack supplies its own AWS provider and state backend.

An internal producer can publish a completed local run and request its CSV using
the module outputs and the same UUID for both operations:

```sh
pixi run python backend/cloud_exports.py publish /tmp/pdbi-demo "$RESULT_BUCKET" "$RUN_ID"
pixi run python backend/cloud_exports.py request-csv "$CSV_QUEUE_URL" "$RUN_ID"
```

The publisher needs permission to put and read the Parquet object and abort its
multipart uploads. The requester needs permission to send to the export queue.
The worker consumes `{"run_id":"<uuid>"}` and derives both keys under
`runs/<uuid>/`. A second delivery reuses the saved CSV. Failed messages are
retried and redriven to the dead-letter queue after five receives. The API can
serve a download once it verifies ownership and finds the requested object.

The worker stages the input and output under Lambda temporary storage. Together
they must fit in 10 GiB and the conversion must finish within 15 minutes. Larger
runs need another conversion strategy. Live ingestion uses the separate page worker below.


## Live query ingestion

The ingestion Lambda uses the same container image and a separate queue. Its role
can write source parts, query checkpoints, and final Parquet results. To submit a
query, set RESULT_BUCKET and INGEST_QUEUE_URL from the Terraform outputs, and
choose a fresh UUID for RUN_ID. The trusted submitter needs S3 read/write access
to the run checkpoint and permission to send to the ingestion queue.

```sh
pixi run python backend/cloud_ingest.py submit "$RUN_ID" --organism 9606 --motif ACD
pixi run python backend/cloud_ingest.py status "$RUN_ID"
pixi run python backend/cloud_ingest.py resume "$RUN_ID"
```

UniProt pages use the release header and cursor. RCSB searches by taxonomy
lineage, saves a sorted manifest of protein entity IDs for the attempt, then
hydrates up to 100 entities per message and checks the exact source taxon and
literal motif locally. The manifest fixes membership. Entity sequences reflect
their individual retrieval times. Structure-to-UniProt references remain in the
staged parts.

The run checkpoint is `runs/<uuid>/query.json`. Each immutable page records its
continuation with its Parquet bytes. A replay can recover that continuation
without another API call. After three consecutive source restarts the worker
pauses that source. Resume queues unfinished work, including after an SQS
dead-letter event. Once both sources finish, a finalizer reconciles only their
successful attempts and publishes the saved Parquet. Finalization uses Lambda's
temporary disk for the parts, SQLite index, and export. Very large runs can
exceed its disk or time budget.

## Frontend and wizard query API

Enable the HTTP API by supplying the pipeline module's optional `query_auth`
object with `issuer` (Cognito user pool issuer URL), `audience` (app client ID),
and `origins` (browser origins). The module exports `query_api_url`. All routes
take `Authorization: Bearer <token>`. The gateway verifies the JWT. Run ownership
comes from the verified issuer and subject, and each run-specific route checks it.

| Method and path | Request | Response |
| --- | --- | --- |
| POST /api/query/validate | JSON with integer `organism` and string `motif` | 200 with normalized `query`, or 400 with a `fields` map |
| POST /api/query | Same JSON and a UUID `Idempotency-Key` header | 202 with `run_id` and `status_url` |
| GET /api/query/{run_id} | | 200 with query, status, per-source progress, `can_resume`, and `can_download` |
| POST /api/query/{run_id}/resume | | 202 with current run status |
| POST /api/query/{run_id}/download?format=csv or parquet | | 200 with a signed URL when ready, or 202 while preparing CSV |
| GET /api/query/{run_id}/download?format=csv or parquet | | Poll for the download; GET leaves the job unchanged |

For example, send `{"organism":9606,"motif":"a cd"}` to validation and display
the returned `{"query":{"organism":9606,"motif":"ACD"}}`. A researcher clicking
**Run** confirms that query. Generate a key with `crypto.randomUUID()` for that
click and keep it through network retries. Different criteria under the same
key return 409. **Run again** gets a fresh key. Replaying submission preserves
a paused run. Explicit resume continues it.

Poll the returned status URL with backoff. Run status is `running`,
`needs_resume`, or `complete`. Each entry in `sources` has `pages_completed`,
`paused`, and `complete`. Progress counts pages in the current source attempt.
A completed run enables Parquet download and on-demand CSV conversion. An
early download returns 409. A ready response contains `status: ready`, `url`,
and `expires_in_seconds: 300`. Treat the URL as a temporary bearer credential.
When a CSV request returns 202, poll the GET download route. A failed conversion
returns `needs_retry`; POST the download route again to retry. GET before the
first CSV request returns `not_requested`.

An unknown run and another user's run both return 404. Older, ownerless CLI runs
remain accessible through the trusted pipeline tools. Account history can index
the same run IDs. Local API tests use API Gateway event fixtures and Moto.
