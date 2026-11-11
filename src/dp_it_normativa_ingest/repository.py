"""SQL of one act: history replaced, units / act / Akoma Ntoso upserted (port of the legacy ``BulkWriter``).

All statements run inside one transaction opened by the caller; the dataset schema is on the search_path (names are unqualified).
"""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable

PLAN_SCHEMA_VERSION = 1
UNIT_COLUMNS = (
    "urn", "act", "act_urn", "article_num", "comma_num", "materialized_path", "rubrica", "text_raw", "text_contextual",
    "version_date", "collection",
)

DELETE_HISTORY = "DELETE FROM unit_versions WHERE urn = ANY(%s)"
INSERT_HISTORY = "INSERT INTO unit_versions (urn, valid_from, valid_to, text_raw) VALUES (%s, %s, %s, %s)"

# Text changed -> the embedding is stale: NULL it; identical text keeps it. Collection: the same act can ship in several collections
# and the column is single-valued, so a label ranking higher in the priority list is never downgraded (unknown labels rank equal-last;
# the first writer stays).
UPSERT_UNITS = f"""
INSERT INTO units ({", ".join(UNIT_COLUMNS)})
VALUES ({", ".join(["%s"] * len(UNIT_COLUMNS))})
ON CONFLICT (urn) DO UPDATE SET
    act = EXCLUDED.act,
    act_urn = EXCLUDED.act_urn,
    article_num = EXCLUDED.article_num,
    comma_num = EXCLUDED.comma_num,
    materialized_path = EXCLUDED.materialized_path,
    rubrica = EXCLUDED.rubrica,
    text_contextual = EXCLUDED.text_contextual,
    version_date = EXCLUDED.version_date,
    collection = CASE
        WHEN units.collection IS NULL THEN EXCLUDED.collection
        WHEN COALESCE(array_position(%s::text[], units.collection), 2147483647)
             <= COALESCE(array_position(%s::text[], EXCLUDED.collection), 2147483647) THEN units.collection
        ELSE EXCLUDED.collection END,
    embedding = CASE WHEN units.text_raw IS DISTINCT FROM EXCLUDED.text_raw THEN NULL ELSE units.embedding END,
    text_raw = EXCLUDED.text_raw
"""

UPSERT_ACT = """
INSERT INTO acts (act_urn, title, denominazione, year, number, kind, collection, versions_count, first_valid_from, last_valid_from,
                  units_count, ingested_at, source_url)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
ON CONFLICT (act_urn) DO UPDATE SET
    title = EXCLUDED.title, denominazione = EXCLUDED.denominazione, year = EXCLUDED.year, number = EXCLUDED.number, kind = EXCLUDED.kind,
    collection = EXCLUDED.collection, versions_count = EXCLUDED.versions_count, first_valid_from = EXCLUDED.first_valid_from,
    last_valid_from = EXCLUDED.last_valid_from, units_count = EXCLUDED.units_count, ingested_at = EXCLUDED.ingested_at,
    source_url = EXCLUDED.source_url
"""

UPSERT_AKN = """
INSERT INTO akn_documents (act_urn, version_date, sha256, xml) VALUES (%s, %s, %s, %s)
ON CONFLICT (act_urn) DO UPDATE SET version_date = EXCLUDED.version_date, sha256 = EXCLUDED.sha256, xml = EXCLUDED.xml
"""


class Cursor(Protocol):
    def execute(self, sql: Any, params: Any = None) -> Any: ...

    def executemany(self, sql: Any, params: Iterable[Any]) -> Any: ...


def write_act(cursor: Cursor, doc: dict[str, Any], akn: bytes, priority: list[str]) -> dict[str, int]:
    """Writes one act (history, units, act, Akoma Ntoso) and returns the row counts per table."""
    act = doc["act"]
    cursor.execute(DELETE_HISTORY, (doc["seen_urns"],))
    rows = [(r["urn"], r["valid_from"], r["valid_to"], r["text_raw"]) for r in doc["version_rows"]]
    if rows:
        cursor.executemany(INSERT_HISTORY, rows)
    units = [(*(u[column] for column in UNIT_COLUMNS), priority, priority) for u in doc["units"]]
    if units:
        cursor.executemany(UPSERT_UNITS, units)
    cursor.execute(
        UPSERT_ACT,
        (act["act_urn"], act["title"], act["denominazione"], act["year"], act["number"], act["kind"], act["collection"],
         act["versions_count"],
         act["first_valid_from"], act["last_valid_from"], act["units_count"], act["source_url"]),
    )
    cursor.execute(UPSERT_AKN, (act["act_urn"], act["last_valid_from"], hashlib.sha256(akn).hexdigest(), akn.decode("utf-8")))
    return {"acts": 1, "units": len(units), "unit_versions": len(rows), "akn_documents": 1}
