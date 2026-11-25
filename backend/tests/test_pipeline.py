import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

import pipeline as p


class PipelineTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.run_dir = self.root / "run"
        self.fixtures = self.root / "fixtures"
        shutil.copytree(Path(p.__file__).with_name("fixtures"), self.fixtures)

    def start(self, motif="ACD"):
        p.start(self.run_dir, self.fixtures, 9606, motif)

    def crash(self):
        # Kill a separate worker after the first PDB part is published but before its checkpoint.
        program = """
import os, sys
from pathlib import Path
import pipeline as p
save = p.checkpoint
def crash(run, state):
    if state["sources"]["pdb"].get("page") == 1:
        os._exit(83)
    save(run, state)
p.checkpoint = crash
p.start(Path(sys.argv[1]), Path(sys.argv[2]), 9606, "ACD")
"""
        proc = subprocess.run([sys.executable, "-c", program, str(self.run_dir), str(self.fixtures)],
                              cwd=Path(p.__file__).parent)
        self.assertEqual(proc.returncode, 83)
        self.assertEqual(p.load_state(self.run_dir)["sources"]["pdb"]["page"], 0)
        self.assertTrue((self.run_dir / "parts/pdb/1/0.parquet").exists())

    def test_resume_bytes_and_downloads(self):
        self.crash()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            p.download(self.run_dir, "csv")
        p.resume(self.run_dir)
        clean = self.root / "clean"
        p.start(clean, self.fixtures, 9606, "ACD")
        for fmt in ("parquet", "csv"):
            self.assertEqual(p.download(clean, fmt).read_bytes(), p.download(self.run_dir, fmt).read_bytes())
        rows = pq.read_table(p.download(self.run_dir, "parquet")).to_pylist()
        with p.download(self.run_dir, "csv").open(newline="") as stream:
            csv_rows = list(csv.DictReader(stream))
        for row in csv_rows:
            row["source_records"] = json.loads(row["source_records"])
        self.assertEqual(rows, csv_rows)
        refs = {r["sequence"]: [s["id"] for s in r["source_records"]] for r in rows}
        self.assertEqual(refs, {"MACDEFG": ["DEMO-P1_1", "DEMO-U1"],
                                "ACD": ["DEMO-P2_1"], "WACDW": ["DEMO-U2"]})
        parts = pq.read_table(self.run_dir / "parts/pdb/1/0.parquet").to_pylist()
        self.assertEqual(parts[1]["parent_id"], "DEMO-U1")
        with patch.object(p.pq, "ParquetFile", side_effect=AssertionError("unexpected conversion")):
            self.assertTrue(p.download(self.run_dir, "csv").exists())

    def test_expired_checkpoint_replaces_only_incomplete_source(self):
        self.crash()
        (self.fixtures / "uniprot.json").unlink()  # completed source is never fetched again
        (self.fixtures / "pdb.json").write_text("[[]]")
        p.resume(self.run_dir)
        sources = p.load_state(self.run_dir)["sources"]
        self.assertEqual([sources[s]["attempt"] for s in p.SOURCES], [1, 2])
        rows = pq.read_table(p.download(self.run_dir, "parquet")).to_pylist()
        self.assertEqual([r["sequence"] for r in rows], ["MACDEFG", "WACDW"])
        self.assertTrue(all(ref["source"] == "uniprot" for r in rows for ref in r["source_records"]))

    def test_crc_and_forced_collision(self):
        self.assertEqual(p.crc64("IHATEMATH"), "E3DCADD69B01ADD1")
        with patch.object(p, "crc64", return_value="0000000000000001"):
            self.start()
        rows = pq.read_table(p.download(self.run_dir, "parquet")).to_pylist()
        self.assertEqual([r["sequence"] for r in rows], ["ACD", "MACDEFG", "WACDW"])
        self.assertTrue(all(r["crc64"] == "0000000000000001" for r in rows))

    def test_empty_result(self):
        self.start("XXX")
        self.assertEqual(pq.read_table(p.download(self.run_dir, "parquet")).num_rows, 0)
        self.assertEqual(p.download(self.run_dir, "csv").read_text(), "crc64,sequence,source_records\n")


if __name__ == "__main__":
    unittest.main()
