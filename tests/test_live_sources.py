import unittest
from unittest.mock import patch

import live_sources as src


class LiveSources(unittest.TestCase):
    @patch("live_sources.request")
    def test_uniprot_release_and_cursor(self, fetch):
        query = dict(organism=9606, motif="ACD")
        entry = dict(primaryAccession="P1", sequence={"value": "macd"}, organism={"taxonId": 9606})
        fetch.return_value = ({"results": [entry]}, {"x-uniprot-release": "v1",
                              "Link": '<https://rest.uniprot.org/uniprotkb/search?cursor=abc>; rel="next"'})
        rows, cursor, version = src.uniprot_page(query, None, None)
        self.assertEqual((rows[0]["sequence"], cursor, version), ("MACD", "abc", "v1"))
        with self.assertRaises(src.ExpiredCursor):
            src.uniprot_page(query, cursor, "previous")

    @patch("live_sources.request")
    def test_pdb_requires_exact_taxon_and_preserves_mapping(self, fetch):
        entity = dict(rcsb_id="X_1", entity_poly={"pdbx_seq_one_letter_code_can": "MACD",
                      "rcsb_entity_polymer_type": "Protein"}, rcsb_entity_source_organism=[{"ncbi_taxonomy_id": 9606}],
                      rcsb_polymer_entity_container_identifiers={"reference_sequence_identifiers": [
                          {"database_name": "UniProt", "database_accession": "REF"}]})
        other = dict(entity, rcsb_id="X_2", rcsb_entity_source_organism=[{"ncbi_taxonomy_id": 999}])
        fetch.return_value = ({"data": {"polymer_entities": [entity, other]}}, {})
        rows, cursor, _ = src.pdb_page(dict(organism=9606, motif="ACD"), ["X_1", "X_2"], None)
        self.assertIsNone(cursor)
        self.assertEqual([(r["id"], r["parent_id"]) for r in rows], [("X_1", "REF")])
