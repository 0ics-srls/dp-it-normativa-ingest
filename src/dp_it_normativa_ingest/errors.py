"""The three Proc-protocol error codes and the run-level failures that carry them."""
from __future__ import annotations

CONFIG_ERROR = "config_error"
TRANSIENT_ERROR = "transient_error"
SYSTEM_ERROR = "system_error"


class IngestError(Exception):
    """A failure carrying one of the three protocol error codes."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class DbConnectError(Exception):
    """The dataset database cannot be reached; the message names the role and the error class only (never the password)."""
