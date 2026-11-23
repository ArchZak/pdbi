# Resume the same query run from durable source checkpoints

Track progress independently for each required source. Persist each page before advancing its checkpoint. Processing a page again must preserve exactly one copy of each sequence and each associated source identifier. Keep completed source work when another source fails.

Retry transient failures automatically. Once retries are exhausted, the researcher can resume the same run with its progress intact. Publish the download when every required source completes, including sources that return zero matches.

Verify recovery by interrupting a worker after page persistence and before queue acknowledgement. Resuming the run over the same inputs must produce an export byte-for-byte identical to uninterrupted processing.

When a checkpoint expires or becomes unsafe to reuse, restart the affected source under the same run ID. Replace its interrupted attempt as a unit. Keep completed work from the other source.

Persist each fetched page as an immutable Parquet part identified by run, source, attempt, and page. Advance the source checkpoint after the part is durable. Once both sources complete, reconcile only the parts from their successful attempts into the downloadable Parquet file.

Use one SQS item per source page. Store the continuation alongside its immutable S3 Parquet object, then conditionally advance the run checkpoint using its ETag. On redelivery, adopt an existing page and repair a missing successor message. RCSB pagination uses an attempt-specific entity manifest and UniProt pagination tracks its release and cursor. Finalization reads the completed attempts.
