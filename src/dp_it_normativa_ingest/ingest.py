"""The ``execute`` action: loads the plan files of the given acts, one transaction per act."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from dp_it_normativa_ingest.db import role_of
from dp_it_normativa_ingest.errors import CONFIG_ERROR, SYSTEM_ERROR, TRANSIENT_ERROR, DbConnectError, IngestError
from dp_it_normativa_ingest.repository import PLAN_SCHEMA_VERSION, write_act

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    from dp_it_normativa_ingest.repository import Cursor

KINDS = ("normattiva", "gu")
_SCHEMA = re.compile(r"^ds_[a-z0-9_]+$")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


class Connection(Protocol):
    def transaction(self) -> AbstractContextManager[Any]: ...

    def cursor(self) -> AbstractContextManager[Cursor]: ...

    def execute(self, sql: Any, params: Any = None) -> Any: ...

    def close(self) -> None: ...


def safe_name(act_id: str) -> str:
    """File stem of an act id (identical to the grab and convert classes)."""
    return _UNSAFE.sub("_", act_id.replace(" ", "-"))


def _validate(params: dict[str, Any]) -> tuple[list[dict[str, Any]], Path, str, str, list[str]]:
    items = params.get("items")
    if not isinstance(items, list) or not items:
        raise IngestError(CONFIG_ERROR, "items must be a non-empty array")
    for entry in items:
        meta = entry.get("meta") if isinstance(entry, dict) else None
        if not isinstance(entry, dict) or not entry.get("key") or not isinstance(meta, dict) or meta.get("kind") not in KINDS:
            raise IngestError(CONFIG_ERROR, f"item needs a key and meta.kind one of {list(KINDS)}: {entry!r}")
        if not isinstance(meta.get("actId"), str) or not meta["actId"]:
            raise IngestError(CONFIG_ERROR, f"item {entry['key']}: meta.actId is required")
    directory, conn_string, schema = params.get("dirIn"), params.get("connString"), params.get("schema")
    if not isinstance(directory, str) or not directory:
        raise IngestError(CONFIG_ERROR, "dirIn is required")
    if not isinstance(conn_string, str) or not conn_string:
        raise IngestError(CONFIG_ERROR, "connString is required")
    if not isinstance(schema, str) or not _SCHEMA.match(schema):
        raise IngestError(CONFIG_ERROR, "schema must be a dataset schema name (ds_...)")
    priority = params.get("collectionPriority", [])
    if not isinstance(priority, list) or not all(isinstance(p, str) for p in priority):
        raise IngestError(CONFIG_ERROR, "collectionPriority must be an array of collection names")
    return items, Path(directory), conn_string, schema, priority


def _load(entry: dict[str, Any], directory: Path) -> tuple[dict[str, Any], bytes]:
    stem = safe_name(entry["meta"]["actId"])
    doc = json.loads((directory / f"{stem}.plan.json").read_text(encoding="utf-8"))
    if doc.get("schema_version") != PLAN_SCHEMA_VERSION:
        msg = f"unsupported plan schema_version {doc.get('schema_version')}"
        raise ValueError(msg)
    return doc, (directory / f"{stem}.akn.xml").read_bytes()


def _items(  # noqa: PLR0913, PLR0917
    connection: Connection,
    schema: str,
    priority: list[str],
    items: list[dict[str, Any]],
    directory: Path,
    on_progress: Callable[[int, str], None],
) -> Iterator[tuple[dict[str, Any], dict[str, int] | Exception]]:
    for index, entry in enumerate(items):
        try:
            doc, akn = _load(entry, directory)
            with connection.transaction():
                connection.execute(f"SET search_path TO {schema}, public")  # schema is validated against ^ds_[a-z0-9_]+$
                with connection.cursor() as cursor:
                    counts = write_act(cursor, doc, akn, priority)
            yield {**entry, "act_urn": doc["act"]["act_urn"]}, counts
        except Exception as error:  # noqa: BLE001 - any failure of one act is reported, the next act still runs
            yield entry, error
        on_progress(int((index + 1) * 100 / len(items)), f"{index + 1}/{len(items)} {entry['key']}")


def run_ingest(params: dict[str, Any], connect: Callable[[str], Connection], on_progress: Callable[[int, str], None]) -> dict[str, Any]:
    """Ingests every item; failed acts are rolled back and reported together as one system_error after the good ones were committed."""
    items, directory, conn_string, schema, priority = _validate(params)
    try:
        connection = connect(conn_string)
    except (OSError, DbConnectError) as ex:
        msg = f"cannot connect to the dataset database as role {role_of(conn_string)} ({type(ex).__name__})"
        raise IngestError(TRANSIENT_ERROR, msg) from None
    metrics = {"acts": 0, f"written.{schema}.acts": 0, f"written.{schema}.units": 0, f"written.{schema}.unit_versions": 0,
               f"written.{schema}.akn_documents": 0}
    written: list[dict[str, Any]] = []
    failures: list[str] = []
    try:
        for entry, outcome in _items(connection, schema, priority, items, directory, on_progress):
            if isinstance(outcome, Exception):
                failures.append(f"{entry['key']} ({type(outcome).__name__}: {outcome})")
                continue
            metrics["acts"] += 1
            for table in ("acts", "units", "unit_versions", "akn_documents"):
                metrics[f"written.{schema}.{table}"] += outcome[table]
            written.append(
                {"key": entry["key"], "act_urn": entry["act_urn"], "units": outcome["units"], "version_rows": outcome["unit_versions"]}
            )
    finally:
        connection.close()
    if failures:
        msg = f"{len(failures)} of {len(items)} acts failed: {'; '.join(failures)}"
        raise IngestError(SYSTEM_ERROR, msg)
    return {"metrics": metrics, "acts": written}
