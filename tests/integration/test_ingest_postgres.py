"""Real Postgres + pgvector (Testcontainers) with the package migrations applied; no Jina call (embeddings are set by SQL)."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from builders import ACT_URN, item, plan_doc, unit, write_files
from dp_it_normativa_ingest.db import connect
from dp_it_normativa_ingest.errors import IngestError
from dp_it_normativa_ingest.ingest import run_ingest

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

SCHEMA = "ds_it_normativa"
MIGRATIONS = Path(__file__).resolve().parents[4] / "dp-it-normativa" / "schema" / "migrations"
VECTOR = "[" + ",".join(["0.25"] * 1024) + "]"


class Db:
    def __init__(self, container: PostgresContainer) -> None:
        self.dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        self.conn_string = (
            f"Host={container.get_container_host_ip()};Port={container.get_exposed_port(5432)};"
            f"Database={container.dbname};Username={container.username};Password={container.password}"
        )

    def scalar(self, sql: str) -> Any:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(sql).fetchone()
            assert row is not None
            return row[0]

    def execute(self, sql: str) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(sql)

    def ingest(self, directory: Path, items: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
        params = {"items": items or [item()], "dirIn": str(directory), "connString": self.conn_string, "schema": SCHEMA, **extra}
        return run_ingest(params, connect, lambda *_: None)


@pytest.fixture(scope="module")
def container() -> Iterator[PostgresContainer]:
    with PostgresContainer("pgvector/pgvector:pg17", username="lex", password="pw", dbname="lex") as pg:
        yield pg


@pytest.fixture
def db(container: PostgresContainer) -> Db:
    database = Db(container)
    database.execute("CREATE EXTENSION IF NOT EXISTS vector")
    database.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    database.execute(f"CREATE SCHEMA {SCHEMA}")
    with psycopg.connect(database.dsn, autocommit=True) as conn:
        conn.execute(f"SET search_path TO {SCHEMA}, public")
        for migration in sorted(MIGRATIONS.glob("*.sql")):
            conn.execute(migration.read_text(encoding="utf-8"))
    return database


def count(db: Db, table: str, where: str = "true") -> int:
    return int(db.scalar(f"SELECT count(*) FROM {SCHEMA}.{table} WHERE {where}"))


def test_first_ingest_writes_all_five_tables(db: Db, tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc(), xml="<akomaNtoso>è</akomaNtoso>")

    result = db.ingest(tmp_path)

    assert (count(db, "acts"), count(db, "units"), count(db, "unit_versions"), count(db, "akn_documents")) == (1, 2, 2, 1)
    assert db.scalar(f"SELECT xml FROM {SCHEMA}.akn_documents") == "<akomaNtoso>è</akomaNtoso>"
    assert db.scalar(f"SELECT deep_link FROM {SCHEMA}.units WHERE comma_num = '1' AND article_num = '1'").endswith("~art1")
    assert db.scalar(f"SELECT ingested_at IS NOT NULL FROM {SCHEMA}.acts") is True
    assert result["metrics"][f"written.{SCHEMA}.units"] == 2


def test_second_ingest_of_the_same_plan_is_a_no_op_and_keeps_the_embedding(db: Db, tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    db.ingest(tmp_path)
    db.execute(f"UPDATE {SCHEMA}.units SET embedding = '{VECTOR}'::vector")

    db.ingest(tmp_path)

    assert (count(db, "acts"), count(db, "units"), count(db, "unit_versions"), count(db, "akn_documents")) == (1, 2, 2, 1)
    assert count(db, "units", "embedding IS NOT NULL") == 2


def test_changed_text_nulls_only_that_embedding(db: Db, tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    db.ingest(tmp_path)
    db.execute(f"UPDATE {SCHEMA}.units SET embedding = '{VECTOR}'::vector")
    changed = plan_doc(units=[unit("1", "uno MODIFICATO"), unit("2", "due")])
    write_files(tmp_path, changed)

    db.ingest(tmp_path)

    assert count(db, "units", "embedding IS NULL") == 1
    assert db.scalar(f"SELECT text_raw FROM {SCHEMA}.units WHERE embedding IS NULL") == "uno MODIFICATO"


def test_history_rows_are_replaced_not_duplicated(db: Db, tmp_path: Path) -> None:
    first = plan_doc()
    write_files(tmp_path, first)
    db.ingest(tmp_path)
    urn = f"{ACT_URN}~art1-com1"
    second = plan_doc(
        units=[unit("1", "uno v2", version_date="2021-01-01"), unit("2", "due")],
        rows=[
            {"urn": urn, "valid_from": "2020-01-01", "valid_to": "2021-01-01", "text_raw": "uno"},
            {"urn": urn, "valid_from": "2021-01-01", "valid_to": None, "text_raw": "uno v2"},
            {"urn": f"{ACT_URN}~art2-com1", "valid_from": "2020-01-01", "valid_to": None, "text_raw": "due"},
        ],
    )
    write_files(tmp_path, second)

    db.ingest(tmp_path)
    db.ingest(tmp_path)

    assert count(db, "unit_versions") == 3
    assert count(db, "unit_versions", f"urn = '{urn}' AND valid_to IS NULL") == 1


def test_a_failure_in_the_middle_rolls_back_the_whole_act(db: Db, tmp_path: Path) -> None:
    # valid_to <= valid_from violates the table check after the units were already written in the same transaction
    bad = plan_doc(rows=[{"urn": f"{ACT_URN}~art1-com1", "valid_from": "2021-01-01", "valid_to": "2020-01-01", "text_raw": "x"}])
    write_files(tmp_path, bad)

    with pytest.raises(IngestError):
        db.ingest(tmp_path)

    assert (count(db, "acts"), count(db, "units"), count(db, "unit_versions"), count(db, "akn_documents")) == (0, 0, 0, 0)


def test_two_acts_in_one_run(db: Db, tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    other = "urn:nir:stato:legge:2001-01-01;2"
    write_files(tmp_path, plan_doc(act_urn=other, units=[{**unit("1", "x"), "urn": f"{other}~art1-com1", "act_urn": other}]),
                stem="normattiva_LEGGE_2001_2")

    result = db.ingest(tmp_path, [item(), item("normattiva:LEGGE:2001:2")])

    assert (count(db, "acts"), count(db, "units"), count(db, "akn_documents")) == (2, 3, 2)
    assert result["metrics"]["acts"] == 2


def test_collection_is_never_downgraded_by_the_priority_list(db: Db, tmp_path: Path) -> None:
    priority = ["COSTITUZIONE", "CODICI", "NORMATTIVA"]
    write_files(tmp_path, plan_doc(units=[unit("1", "uno", collection="CODICI")]))
    db.ingest(tmp_path, collectionPriority=priority)
    write_files(tmp_path, plan_doc(units=[unit("1", "uno", collection="NORMATTIVA")]))

    db.ingest(tmp_path, collectionPriority=priority)

    assert db.scalar(f"SELECT collection FROM {SCHEMA}.units") == "CODICI"

    write_files(tmp_path, plan_doc(units=[unit("1", "uno", collection="COSTITUZIONE")]))
    db.ingest(tmp_path, collectionPriority=priority)

    assert db.scalar(f"SELECT collection FROM {SCHEMA}.units") == "COSTITUZIONE"


def test_a_null_collection_is_filled_by_the_first_label(db: Db, tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc(units=[unit("1", "uno")]))
    db.ingest(tmp_path)
    db.execute(f"UPDATE {SCHEMA}.units SET collection = NULL")

    db.ingest(tmp_path)

    assert db.scalar(f"SELECT collection FROM {SCHEMA}.units") == "NORMATTIVA"
