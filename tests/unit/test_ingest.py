"""run_ingest with a fake connection: statement order, parameters, transactions, failure handling, secrets."""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import pytest

from builders import ACT_ID, ACT_URN, STEM, FakeConnection, item, plan_doc, unit, write_files
from dp_it_normativa_ingest.errors import CONFIG_ERROR, SYSTEM_ERROR, TRANSIENT_ERROR, IngestError
from dp_it_normativa_ingest.ingest import run_ingest

CONN = "Host=db;Port=5432;Database=lex;Username=ds_it_normativa_rw;Password=s3cr3t-pw"
SCHEMA = "ds_it_normativa"


def params(directory: Path, **extra: Any) -> dict[str, Any]:
    return {"items": [item()], "dirIn": str(directory), "connString": CONN, "schema": SCHEMA, **extra}


def kinds(connection: FakeConnection) -> list[str]:
    """First words of the executed statements, in order."""
    return [" ".join(sql.split()[:3]) for _, sql, _ in connection.log]


def test_statements_run_in_one_transaction_in_the_documented_order(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc(), xml="<x>é</x>")
    connection = FakeConnection()

    result = run_ingest(params(tmp_path), lambda _dsn: connection, lambda *_: None)

    assert connection.events == ["begin", "commit"]
    assert kinds(connection) == [
        "SET search_path TO",
        "DELETE FROM unit_versions",
        "INSERT INTO unit_versions",
        "INSERT INTO units",
        "INSERT INTO acts",
        "INSERT INTO akn_documents",
    ]
    assert connection.closed
    assert result["acts"] == [{"key": f"{ACT_ID}|v1", "act_urn": ACT_URN, "units": 2, "version_rows": 2}]


def test_history_is_replaced_by_the_seen_urns_and_rows_are_inserted(tmp_path: Path) -> None:
    doc = plan_doc(rows=[{"urn": f"{ACT_URN}~art1-com1", "valid_from": "2020-01-01", "valid_to": "2021-01-01", "text_raw": "a"},
                         {"urn": f"{ACT_URN}~art1-com1", "valid_from": "2021-01-01", "valid_to": None, "text_raw": "b"}])
    write_files(tmp_path, doc)
    connection = FakeConnection()

    run_ingest(params(tmp_path), lambda _dsn: connection, lambda *_: None)

    delete = next(entry for entry in connection.log if "DELETE FROM unit_versions" in entry[1])
    assert delete[2] == (doc["seen_urns"],)
    insert = next(entry for entry in connection.log if entry[0] == "executemany" and "unit_versions" in entry[1])
    assert insert[2] == [
        (f"{ACT_URN}~art1-com1", "2020-01-01", "2021-01-01", "a"),
        (f"{ACT_URN}~art1-com1", "2021-01-01", None, "b"),
    ]


def test_units_are_upserted_with_embedding_reset_on_changed_text_and_the_collection_priority(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    connection = FakeConnection()

    run_ingest(params(tmp_path, collectionPriority=["COSTITUZIONE", "CODICI", "NORMATTIVA"]), lambda _dsn: connection, lambda *_: None)

    upsert = next(entry for entry in connection.log if "INSERT INTO units" in entry[1])
    sql = " ".join(upsert[1].split())
    assert "ON CONFLICT (urn) DO UPDATE" in sql
    assert "embedding = CASE WHEN units.text_raw IS DISTINCT FROM EXCLUDED.text_raw THEN NULL ELSE units.embedding END" in sql
    assert "array_position" in sql
    assert upsert[0] == "executemany"
    first = upsert[2][0]
    assert first[0] == f"{ACT_URN}~art1-com1"
    assert ["COSTITUZIONE", "CODICI", "NORMATTIVA"] in [p for p in first if isinstance(p, list)]


def test_act_and_akn_document_are_upserted_with_the_sha256_of_the_xml(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc(), xml="<x>è</x>")
    connection = FakeConnection()

    run_ingest(params(tmp_path), lambda _dsn: connection, lambda *_: None)

    act = next(entry for entry in connection.log if "INSERT INTO acts" in entry[1])
    assert "ON CONFLICT (act_urn) DO UPDATE" in act[1]
    assert act[2][0] == ACT_URN
    akn = next(entry for entry in connection.log if "INSERT INTO akn_documents" in entry[1])
    assert "ON CONFLICT (act_urn) DO UPDATE" in akn[1]
    assert akn[2] == (ACT_URN, "2020-01-01", hashlib.sha256("<x>è</x>".encode()).hexdigest(), "<x>è</x>")


def test_metrics_count_the_rows_written_per_table(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())

    result = run_ingest(params(tmp_path), lambda _dsn: FakeConnection(), lambda *_: None)

    assert result["metrics"] == {
        "acts": 1,
        f"written.{SCHEMA}.acts": 1,
        f"written.{SCHEMA}.units": 2,
        f"written.{SCHEMA}.unit_versions": 2,
        f"written.{SCHEMA}.akn_documents": 1,
    }


def test_two_acts_get_two_transactions(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    other_urn = "urn:nir:stato:legge:2001-01-01;2"
    other = plan_doc(act_urn=other_urn, units=[{**unit("1", "x"), "urn": f"{other_urn}~art1-com1", "act_urn": other_urn}])
    write_files(tmp_path, other, stem="normattiva_LEGGE_2001_2")
    connection = FakeConnection()
    items = [item(), item("normattiva:LEGGE:2001:2")]

    result = run_ingest({**params(tmp_path), "items": items}, lambda _dsn: connection, lambda *_: None)

    assert connection.events == ["begin", "commit", "begin", "commit"]
    assert result["metrics"]["acts"] == 2


def test_a_failure_rolls_back_that_act_the_others_are_kept_and_the_run_reports_system_error(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    bad_urn = "urn:nir:x:2;2"
    write_files(tmp_path, plan_doc(act_urn=bad_urn, units=[{**unit("1", "x"), "urn": f"{bad_urn}~art1-com1", "act_urn": bad_urn}]),
                stem="normattiva_LEGGE_2001_2")
    connection = FakeConnection(fail_params=bad_urn)

    with pytest.raises(IngestError) as error:
        run_ingest({**params(tmp_path), "items": [item("normattiva:LEGGE:2001:2"), item()]}, lambda _dsn: connection, lambda *_: None)

    assert error.value.code == SYSTEM_ERROR
    assert "normattiva:LEGGE:2001:2|v1" in str(error.value)
    assert ACT_ID not in str(error.value)
    assert connection.events == ["begin", "rollback", "begin", "commit"]


def test_a_missing_plan_file_fails_the_item(tmp_path: Path) -> None:
    with pytest.raises(IngestError) as error:
        run_ingest(params(tmp_path), lambda _dsn: FakeConnection(), lambda *_: None)

    assert error.value.code == SYSTEM_ERROR
    assert f"{ACT_ID}|v1" in str(error.value)


def test_a_malformed_plan_fails_the_item(tmp_path: Path) -> None:
    write_files(tmp_path, {**plan_doc(), "schema_version": 2})

    with pytest.raises(IngestError) as error:
        run_ingest(params(tmp_path), lambda _dsn: FakeConnection(), lambda *_: None)

    assert "schema_version" in str(error.value)


def test_connection_failure_is_transient_and_never_shows_the_password(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())

    def refuse(dsn: str) -> Any:
        msg = f"cannot connect with {dsn}"
        raise OSError(msg)

    with pytest.raises(IngestError) as error:
        run_ingest(params(tmp_path), refuse, lambda *_: None)

    assert error.value.code == TRANSIENT_ERROR
    assert "s3cr3t-pw" not in str(error.value)
    assert "ds_it_normativa_rw" in str(error.value)


def test_the_connection_string_is_never_logged_or_in_errors(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    write_files(tmp_path, plan_doc())
    caplog.set_level(logging.DEBUG)

    with pytest.raises(IngestError) as error:
        run_ingest(params(tmp_path), lambda _dsn: FakeConnection(fail_on="INSERT INTO acts"), lambda *_: None)

    assert "s3cr3t-pw" not in str(error.value)
    assert "s3cr3t-pw" not in caplog.text


@pytest.mark.parametrize(
    "bad",
    [
        {"items": []},
        {"items": "x"},
        {"items": [{"key": "k"}]},
        {"items": [{"key": "k", "meta": {"kind": "other", "actId": "a"}}]},
        {"dirIn": ""},
        {"connString": ""},
        {"schema": "public"},
        {"schema": "ds_x; DROP SCHEMA public"},
        {"collectionPriority": "NORMATTIVA"},
        {"collectionPriority": [1]},
    ],
)
def test_bad_parameters_are_config_errors_before_connecting(tmp_path: Path, bad: dict[str, Any]) -> None:
    calls: list[str] = []

    def connect(dsn: str) -> Any:
        calls.append(dsn)
        return FakeConnection()

    with pytest.raises(IngestError) as error:
        run_ingest({**params(tmp_path), **bad}, connect, lambda *_: None)

    assert error.value.code == CONFIG_ERROR
    assert calls == []


def test_progress_is_reported_per_act(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())
    seen: list[int] = []

    run_ingest(params(tmp_path), lambda _dsn: FakeConnection(), lambda percent, _m: seen.append(percent))

    assert seen == [100]
    assert STEM
