"""Entrypoint of dp-it-normativa-ingest: proc-protocol stdin loop. Action ``execute`` loads the plan files of the given acts; ``terminate``
(or stdin EOF) shuts down. stdout carries protocol events only; the connection string never reaches a log or an error."""
from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, TextIO

import procclass
from dp_it_normativa_ingest.db import connect
from dp_it_normativa_ingest.errors import CONFIG_ERROR, SYSTEM_ERROR, IngestError
from dp_it_normativa_ingest.ingest import run_ingest

if TYPE_CHECKING:
    from collections.abc import Callable

    from dp_it_normativa_ingest.ingest import Connection

log = logging.getLogger("dp_it_normativa_ingest")


def main(stdin: TextIO = sys.stdin, stdout: TextIO = sys.stdout, open_connection: Callable[[str], Connection] = connect) -> int:
    """One JSON request per stdin line until terminate/EOF; returns 1 if any request failed."""
    exit_code = 0
    for line in stdin:
        if not line.strip():
            continue
        try:
            request = procclass.parse(line)
        except procclass.ProcProtocolError as ex:
            procclass.emit_error(stdout, CONFIG_ERROR, str(ex))
            exit_code = 1
            continue
        if request.action == "terminate":
            break
        if request.action != "execute":
            procclass.emit_error(stdout, CONFIG_ERROR, f"unknown action: {request.action}")
            exit_code = 1
            continue
        try:
            result = run_ingest(request.params, open_connection, lambda percent, message: procclass.emit_progress(stdout, percent, message))
        except IngestError as error:
            procclass.emit_error(stdout, error.code, str(error))
            exit_code = 1
            continue
        except Exception:
            log.exception("unexpected failure in execute")  # the connection string is in no exception message of this class
            procclass.emit_error(stdout, SYSTEM_ERROR, "unexpected failure in execute")
            exit_code = 1
            continue
        procclass.emit_result(stdout, result)
    return exit_code


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    sys.exit(main())
