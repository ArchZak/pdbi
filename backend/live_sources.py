"""Read one bounded page of protein sequences from a public source."""
import json
import re
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen

from pipeline import normalize


class ExpiredCursor(Exception):
    """The current source attempt must restart."""


def request(url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = Request(url, data, {"Content-Type": "application/json", "Accept": "application/json",
                              "User-Agent": "pdbi-query/0.1"})
    with urlopen(req, timeout=45) as response:
        body = response.read()
        return (json.loads(body) if body else None), response.headers


def uniprot_page(query, cursor, release):
    params = dict(query=f'organism_id:{query["organism"]}', fields="accession,organism_id,sequence",
                  format="json", size=250, sort="accession asc")
    if cursor:
        params["cursor"] = cursor
    try:
        body, headers = request("https://rest.uniprot.org/uniprotkb/search?" + urlencode(params))
    except HTTPError as exc:
        if cursor and exc.code in (400, 410):
            raise ExpiredCursor from exc
        raise
    current = headers["x-uniprot-release"]
    if not current:
        raise ValueError("Missing UniProt release")
    if release and release != current:
        raise ExpiredCursor
    rows = []
    for item in body["results"]:
        seq = normalize(item["sequence"]["value"])
        if query["motif"] in seq and item["organism"]["taxonId"] == query["organism"]:
            rows.append(dict(source="uniprot", id=item["primaryAccession"], sequence=seq,
                             organism=query["organism"]))
    link = re.search(r'<([^>]+)>;\s*rel="next"', headers.get("Link", ""))
    following = parse_qs(urlsplit(link[1]).query)["cursor"][0] if link else None
    return rows, following, current


def pdb_ids(organism):
    def term(attribute, operator, value):
        return {"type": "terminal", "service": "text",
                "parameters": dict(attribute=attribute, operator=operator, value=value)}
    body, _ = request("https://search.rcsb.org/rcsbsearch/v2/query", {
        "query": {"type": "group", "logical_operator": "and", "nodes": [
            term("rcsb_entity_source_organism.taxonomy_lineage.id", "exact_match", str(organism)),
            term("entity_poly.rcsb_entity_polymer_type", "exact_match", "Protein")]},
        "return_type": "polymer_entity", "request_options": {"return_all_hits": True}})
    if body is None:
        return []
    ids = sorted({item["identifier"] for item in body["result_set"]})
    if len(ids) != body["total_count"]:
        raise ValueError("Incomplete PDB entity manifest")
    return ids


def pdb_page(query, ids, offset):
    start = int(offset or 0)
    selected = ids[start:start + 100]
    if not selected:
        return [], None, None
    body, _ = request("https://data.rcsb.org/graphql", {"query": """
        query($ids: [String!]!) {
          polymer_entities(entity_ids: $ids) {
            rcsb_id entity_poly { pdbx_seq_one_letter_code_can rcsb_entity_polymer_type }
            rcsb_entity_source_organism { ncbi_taxonomy_id }
            rcsb_polymer_entity_container_identifiers {
              reference_sequence_identifiers { database_name database_accession }
            }
          }
        }""", "variables": {"ids": selected}})
    if body.get("errors"):
        raise ValueError("RCSB returned GraphQL errors")
    entities = body["data"]["polymer_entities"]
    if {item["rcsb_id"] for item in entities if item} != set(selected):
        raise ExpiredCursor
    rows = []
    for item in entities:
        poly = item["entity_poly"]
        taxa = {str(o["ncbi_taxonomy_id"]) for o in item["rcsb_entity_source_organism"] or []}
        if str(query["organism"]) not in taxa or poly["rcsb_entity_polymer_type"] != "Protein":
            continue
        seq = normalize(poly["pdbx_seq_one_letter_code_can"])
        if query["motif"] in seq:
            refs = item["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"]
            parents = sorted({r["database_accession"] for r in refs or [] if r["database_name"] == "UniProt"})
            rows.append(dict(source="pdb", id=item["rcsb_id"], sequence=seq, organism=query["organism"],
                             parent_id=",".join(parents) or None))
    end = start + len(selected)
    return rows, str(end) if end < len(ids) else None, None
