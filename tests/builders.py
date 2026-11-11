"""Test builders: plan documents (the convert output) and a recording fake database connection."""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

ACT_URN = "urn:nir:stato:legge:2000-01-01;1"
ACT_ID = "normattiva:LEGGE:2000:1"
STEM = "normattiva_LEGGE_2000_1"


def unit(
    article: str, text: str, comma: str | None = "1", version_date: str = "2020-01-01", collection: str = "NORMATTIVA"
) -> dict[str, Any]:
    urn = f"{ACT_URN}~art{article}" + (f"-com{comma}" if comma else "")
    return {
        "urn": urn,
        "act": "Atto",
        "act_urn": ACT_URN,
        "article_num": article,
        "comma_num": comma,
        "materialized_path": f"2000-01-01-1~art{article}/art{article}",
        "rubrica": None,
        "text_raw": text,
        "text_contextual": f"Atto > {text}",
        "version_date": version_date,
        "collection": collection,
    }


def plan_doc(act_urn: str = ACT_URN, units: list[dict[str, Any]] | None = None, rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    units = units if units is not None else [unit("1", "uno"), unit("2", "due")]
    rows = (
        rows
        if rows is not None
        else [{"urn": u["urn"], "valid_from": "2020-01-01", "valid_to": None, "text_raw": u["text_raw"]} for u in units]
    )
    return {
        "schema_version": 1,
        "act": {
            "act_urn": act_urn,
            "title": "Atto",
            "denominazione": "LEGGE",
            "year": 2000,
            "number": 1,
            "kind": "normattiva",
            "collection": "NORMATTIVA",
            "versions_count": 1,
            "first_valid_from": "2020-01-01",
            "last_valid_from": "2020-01-01",
            "units_count": len(units),
            "source_url": f"https://www.normattiva.it/uri-res/N2Ls?{act_urn}",
        },
        "units": units,
        "version_rows": rows,
        "seen_urns": [u["urn"] for u in units],
        "versions_count": 1,
    }


def write_files(directory: Path, doc: dict[str, Any], stem: str = STEM, xml: str = "<akomaNtoso/>") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{stem}.plan.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    (directory / f"{stem}.akn.xml").write_text(xml, encoding="utf-8")


def item(act_id: str = ACT_ID) -> dict[str, Any]:
    return {"key": f"{act_id}|v1", "meta": {"kind": "normattiva", "actId": act_id}}


class FakeCursor:
    def __init__(self, log: list[tuple[str, str, Any]], fail_on: str | None, fail_params: str | None = None) -> None:
        self._log = log
        self._fail_on = fail_on
        self._fail_params = fail_params

    def execute(self, sql: Any, params: Any = None) -> None:
        text = str(sql)
        self._log.append(("execute", text, params))
        if (self._fail_on and self._fail_on in text) or (self._fail_params and self._fail_params in repr(params)):
            msg = f"boom on {self._fail_on}"
            raise RuntimeError(msg)

    def executemany(self, sql: Any, params: Any) -> None:
        self._log.append(("executemany", str(sql), list(params)))


class FakeConnection:
    """Records statements and transaction boundaries; ``fail_on`` makes the statement containing that text raise."""

    def __init__(self, fail_on: str | None = None, fail_params: str | None = None) -> None:
        self.log: list[tuple[str, str, Any]] = []
        self.events: list[str] = []
        self.closed = False
        self._fail_on = fail_on
        self._fail_params = fail_params

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        self.events.append("commit")

    @contextmanager
    def cursor(self) -> Iterator[FakeCursor]:
        yield FakeCursor(self.log, self._fail_on, self._fail_params)

    def execute(self, sql: Any, params: Any = None) -> None:
        FakeCursor(self.log, self._fail_on, self._fail_params).execute(sql, params)

    def close(self) -> None:
        self.closed = True
