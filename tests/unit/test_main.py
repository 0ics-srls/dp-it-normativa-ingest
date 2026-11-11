"""Proc-protocol loop and manifest of the ingest class."""
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import main
from builders import ACT_ID, FakeConnection, item, plan_doc, write_files

ROOT = Path(__file__).resolve().parent.parent.parent
CONN = "Host=db;Port=5432;Database=lex;Username=ds_it_normativa_rw;Password=s3cr3t-pw"


def lines_of(text: str, connection: FakeConnection | None = None) -> tuple[int, list[dict[str, Any]]]:
    out = io.StringIO()
    fake = connection or FakeConnection()
    code = main.main(io.StringIO(text), out, lambda _dsn: fake)
    return code, [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]


def request(action: str, **params: Any) -> str:
    return json.dumps({"_action": action, "_meta": {"executionId": "e1"}, **params}) + "\n"


def execute(directory: Path, **extra: Any) -> str:
    return request("execute", items=[item()], dirIn=str(directory), connString=CONN, schema="ds_it_normativa", **extra)


def test_execute_emits_progress_then_one_result(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())

    code, lines = lines_of(execute(tmp_path))

    assert code == 0
    assert lines[-1]["type"] == "result"
    assert all(m["type"] == "progress" for m in lines[:-1])
    assert lines[-1]["data"]["metrics"]["acts"] == 1


def test_a_failing_act_prints_one_error_line_without_the_password(tmp_path: Path) -> None:
    write_files(tmp_path, plan_doc())

    code, lines = lines_of(execute(tmp_path), FakeConnection(fail_on="INSERT INTO acts"))

    errors = [m for m in lines if m["type"] == "error"]
    assert code != 0
    assert len(errors) == 1
    assert errors[0]["code"] == "system_error"
    assert ACT_ID in errors[0]["message"]
    assert "s3cr3t-pw" not in json.dumps(lines)


def test_bad_line_unknown_action_and_terminate() -> None:
    _, lines = lines_of("not json\n" + request("bogus"))
    assert [m["code"] for m in lines] == ["config_error", "config_error"]

    _, after = lines_of(request("terminate") + request("bogus"))
    assert after == []


def test_manifest_declares_execute_with_a_secret_connection_string() -> None:
    manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["name"] == "dp-it-normativa-ingest"
    assert set(manifest["actions"]) == {"execute", "terminate"}
    properties = manifest["actions"]["execute"]["input"]["properties"]
    assert properties["connString"]["x-secret"] is True
    assert {"items", "dirIn", "connString", "schema", "collectionPriority"} <= set(properties)
    assert manifest["actions"]["execute"]["input"]["required"] == ["items", "dirIn", "connString", "schema"]
