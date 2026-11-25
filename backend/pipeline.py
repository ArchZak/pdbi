"""Local, resumable UniProt/PDB query pipeline using paginated fixture sources."""

import argparse
from contextlib import contextmanager
import csv
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq


SOURCES = ("uniprot", "pdb")
PART = pa.schema([
    ("source", pa.string()), ("id", pa.string()), ("sequence", pa.string()),
    ("organism", pa.int64()), ("parent_id", pa.string()),
])
RESULT = pa.schema([
    ("crc64", pa.string()), ("sequence", pa.string()),
    ("source_records", pa.list_(pa.struct([("source", pa.string()), ("id", pa.string())]))),
])


def crc_table_entry(byte):
    for _ in range(8):
        byte = (byte >> 1) ^ (0xD800000000000000 if byte & 1 else 0)
    return byte


CRC_TABLE = tuple(map(crc_table_entry, range(256)))


def crc64(sequence):
    value = 0
    for byte in sequence.encode("ascii"):
        value = (value >> 8) ^ CRC_TABLE[(value ^ byte) & 255]
    return f"{value:016X}"


def normalize(raw):
    letters = "".join(raw.split())
    if not re.fullmatch("[A-Za-z]+", letters):
        raise ValueError("Expected a nonempty one-letter amino-acid sequence")
    return letters.upper()


@contextmanager
def atomic_path(target):
    """Publish a complete file before a dependent checkpoint becomes visible."""
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=target.parent, prefix=".pending-")
    os.close(fd)
    path = Path(name)
    try:
        yield path
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(path, target)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        path.unlink(missing_ok=True)


@contextmanager
def run_lock(run):
    with (run / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def checkpoint(run, state):
    with atomic_path(run / "run.json") as path:
        path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def ingest(run, state, source):
    progress = state["sources"][source]
    if progress.get("complete"):
        return
    fixtures = Path(state["fixtures"])
    bundled = Path(__file__).resolve().with_name("fixtures")
    # Older checkpoints store the original absolute path to the bundled fixtures.
    if fixtures == bundled.parent.parent / "fixtures" and not fixtures.exists():
        fixtures = bundled
    data = (fixtures / f"{source}.json").read_bytes()
    version = hashlib.sha256(data).hexdigest()
    pages = json.loads(data)
    if progress.get("version") != version:
        progress.update(version=version, attempt=progress.get("attempt", 0) + 1, page=0)
        checkpoint(run, state)
    query = state["query"]
    for index in range(progress["page"], len(pages)):
        target = part_directory(run, source, progress) / f"{index}.parquet"
        if not target.exists():
            rows = []
            for record in pages[index]:
                sequence = normalize(record["sequence"])
                if record["organism"] == query["organism"] and query["motif"] in sequence:
                    rows.append(dict(record, source=source, sequence=sequence))
            with atomic_path(target) as path:
                pq.write_table(pa.Table.from_pylist(rows, schema=PART), path)
        progress["page"] = index + 1
        checkpoint(run, state)
    progress["complete"] = True
    checkpoint(run, state)


def part_directory(run, source, progress):
    return run / "parts" / source / str(progress["attempt"])


def write_result(db, path):
    fields = "crc64, sequence, source, id"
    cursor = db.execute(f"SELECT {fields} FROM records ORDER BY {fields}")
    rows = []
    with pq.ParquetWriter(path, RESULT, compression="snappy") as writer:
        for (digest, sequence), records in itertools.groupby(cursor, key=lambda row: row[:2]):
            refs = [dict(source=source, id=identifier) for _, _, source, identifier in records]
            rows.append(dict(crc64=digest, sequence=sequence, source_records=refs))
            if len(rows) == 1024:
                writer.write_table(pa.Table.from_pylist(rows, schema=RESULT))
                rows.clear()
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=RESULT))


def reconcile(run, state):
    # A disk-backed unique index bounds memory use and keeps digest collisions separate.
    with tempfile.TemporaryDirectory(dir=run) as scratch:
        db = sqlite3.connect(Path(scratch) / "records.db")
        try:
            db.execute("""CREATE TABLE records (crc64 TEXT, sequence TEXT, source TEXT, id TEXT,
                       PRIMARY KEY (crc64, sequence, source, id)) WITHOUT ROWID""")
            for source, progress in state["sources"].items():
                for page in range(progress["page"]):
                    part = part_directory(run, source, progress) / f"{page}.parquet"
                    for batch in pq.ParquetFile(part).iter_batches(columns=["sequence", "source", "id"]):
                        db.executemany("INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?)", (
                            (crc64(row["sequence"]), row["sequence"], row["source"], row["id"])
                            for row in batch.to_pylist()
                        ))
            with atomic_path(run / "result.parquet") as path:
                write_result(db, path)
        finally:
            db.close()


def process(run, state):
    for source in SOURCES:
        for retry in range(3):
            try:
                ingest(run, state, source)
                break
            except OSError:
                if retry == 2:
                    raise
                time.sleep(0.1 * (2 ** retry))
                state = load_state(run)
    reconcile(run, state)
    state["complete"] = True
    checkpoint(run, state)


def load_state(run):
    return json.loads((run / "run.json").read_text(encoding="utf-8"))


def start(run, fixtures, organism, motif):
    motif = normalize(motif)
    if organism < 1:
        raise ValueError("Expected a positive organism taxonomy ID")
    run.mkdir(parents=True, exist_ok=False)
    with run_lock(run):
        state = dict(query=dict(organism=organism, motif=motif), fixtures=str(fixtures.resolve()),
                     sources={source: {} for source in SOURCES}, complete=False)
        checkpoint(run, state)
        process(run, state)


def resume(run):
    with run_lock(run):
        state = load_state(run)
        if not state["complete"]:
            process(run, state)


def write_csv(parquet, path):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(RESULT.names)
        for batch in pq.ParquetFile(parquet).iter_batches():
            for row in batch.to_pylist():
                refs = json.dumps(row["source_records"], separators=(",", ":"))
                writer.writerow([row["crc64"], row["sequence"], refs])


def download(run, fmt):
    with run_lock(run):
        if not load_state(run)["complete"]:
            raise ValueError("Run is incomplete. Resume it before downloading.")
        parquet = run / "result.parquet"
        if fmt == "parquet":
            return parquet
        target = run / "result.csv"
        if not target.exists():
            with atomic_path(target) as path:
                write_csv(parquet, path)
        return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("run")
    create.add_argument("run_dir", type=Path)
    create.add_argument("--fixtures", type=Path, default=Path(__file__).with_name("fixtures"))
    create.add_argument("--organism", type=int, required=True)
    create.add_argument("--motif", required=True)
    commands.add_parser("resume").add_argument("run_dir", type=Path)
    export = commands.add_parser("download")
    export.add_argument("run_dir", type=Path)
    export.add_argument("format", choices=("parquet", "csv"))
    args = parser.parse_args()
    try:
        if args.command == "download":
            print(download(args.run_dir, args.format))
            return
        if args.command == "run":
            start(args.run_dir, args.fixtures, args.organism, args.motif)
        else:
            resume(args.run_dir)
        print(args.run_dir / "result.parquet")
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
