# Working in pdbi

Aarash owns the query pipeline and structured query validation. Nick owns accounts and account-to-result metadata. Both contribute to the LLM wizard. Keep the query runner callable by the manual form and the wizard through the same validated fields.

## Development

Use Pixi from the repository root. Keep pixi.toml and pixi.lock in sync. Run `pixi run -e test test` for the local and emulated AWS suites. The default environment supplies the runtime dependencies. The test environment adds Moto.

Read GLOSSARY.md and docs/adr before changing query semantics, recovery, or exports. Update this file alongside changes to ownership, entry points, or verification commands.

## Pipeline

- The first milestone searches UniProt and PDB by source organism and literal sequence motif. The fixture runner uses local checkpoints. The live entry point is cloud_ingest.py. Each SQS page message targets a single source and attempt. The S3 checkpoint uses an ETag precondition.
- Page writes precede checkpoints. A source restart replaces the entire interrupted attempt. Preserve work from completed sources.
- Group by UniProt-compatible CRC64 and compare normalized sequences. Keep distinct sequences on collisions. Preserve contributing source identifiers.
- A completed run owns immutable Parquet bytes. Local and cloud CSV downloads use the same converter and preserve row order and provenance.
- The SQS export worker consumes a trusted run UUID. The account/API layer checks ownership before requesting an export or signing a download URL.
- Validate infrastructure changes with `terraform -chdir=infra/pipeline fmt -check`, `terraform -chdir=infra/pipeline init -backend=false`, and `terraform -chdir=infra/pipeline validate`.
