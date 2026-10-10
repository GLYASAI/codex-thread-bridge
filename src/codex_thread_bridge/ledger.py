"""Durable request deduplication, including interrupted and partial operations."""

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path


class Ledger:
    def __init__(self, path: Path):
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(descriptor)
        self.db = sqlite3.connect(path, timeout=10)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS operations (
                request_id TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                receipt TEXT NOT NULL
            )
        """)
        self.db.commit()

    @staticmethod
    def _fingerprint(request_id: str, method: str, params: dict):
        if not request_id or len(request_id) > 128:
            raise ValueError("request_id must contain 1–128 characters")
        return hashlib.sha256(
            json.dumps([method, params], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def lookup(self, request_id: str, method: str, params: dict, *, legacy_params=None):
        fingerprint = self._fingerprint(request_id, method, params)
        row = self.db.execute(
            "SELECT fingerprint, receipt FROM operations WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            return None
        receipt = json.loads(row[1])
        if row[0] != fingerprint:
            version = receipt.get("fingerprintVersion", 1)
            legacy_match = (
                version in {1, 2}
                and legacy_params is not None
                and any(
                    row[0] == self._fingerprint(request_id, method, previous)
                    for previous in legacy_params(version)
                )
            )
            if not legacy_match:
                raise ValueError(
                    "request_id already belongs to different arguments; no action taken"
                )
        return receipt

    def begin(self, request_id: str, method: str, params: dict, *, legacy_params=None):
        fingerprint = self._fingerprint(request_id, method, params)
        receipt = {
            "requestId": request_id,
            "operation": method,
            "status": "in_progress_or_unknown",
            "startedAt": time.time(),
            "retrySafe": False,
            "fingerprintVersion": 3,
        }
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO operations VALUES (?, ?, ?)",
                (request_id, fingerprint, json.dumps(receipt)),
            ).rowcount
            existing = self.lookup(request_id, method, params, legacy_params=legacy_params)
        return bool(inserted), existing

    def save(self, receipt: dict):
        receipt = {**receipt, "updatedAt": time.time()}
        with self.db:
            self.db.execute(
                "UPDATE operations SET receipt = ? WHERE request_id = ?",
                (json.dumps(receipt), receipt["requestId"]),
            )
        return receipt

    def get(self, request_id: str):
        row = self.db.execute(
            "SELECT receipt FROM operations WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise ValueError("Unknown request_id")
        return json.loads(row[0])

    def close(self):
        self.db.close()

    def import_legacy(self, path: Path):
        """Copy an old alias ledger without losing or silently resolving conflicting receipts."""
        source = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            rows = source.execute(
                "SELECT request_id, fingerprint, receipt FROM operations"
            ).fetchall()
        finally:
            source.close()
        with self.db:
            for request_id, fingerprint, receipt in rows:
                existing = self.db.execute(
                    "SELECT fingerprint, receipt FROM operations WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if existing is not None:
                    if existing[0] != fingerprint or json.loads(existing[1]) != json.loads(receipt):
                        raise ValueError(
                            f"Conflicting retained request {request_id!r} in legacy socket ledger "
                            f"{path}; no requests will be dispatched. Preserve both ledgers."
                        )
                else:
                    self.db.execute(
                        "INSERT INTO operations VALUES (?, ?, ?)",
                        (request_id, fingerprint, receipt),
                    )


def open_endpoint_ledger(socket_path: Path, state_dir: Path, endpoint_id: str | None = None):
    """Use one ledger for a canonical endpoint, importing the supplied legacy alias if present.

    With endpoint_id (remote mode) the ledger is keyed by that stable name instead of the socket
    path, because a forwarded local socket path can differ on every launch.
    """
    if endpoint_id is not None:
        state_dir = state_dir.expanduser().resolve()
        digest = hashlib.sha256(("endpoint-id:" + endpoint_id).encode()).hexdigest()[:16]
        return (
            socket_path.expanduser().absolute(),
            Ledger(state_dir / f"operations-{digest}.sqlite3"),
        )
    supplied = socket_path.expanduser().absolute()
    canonical = supplied.resolve()
    state_dir = state_dir.expanduser().resolve()

    def filename(path):
        endpoint = hashlib.sha256(str(path).encode()).hexdigest()[:16]
        return state_dir / f"operations-{endpoint}.sqlite3"

    ledger = Ledger(filename(canonical))
    legacy = filename(supplied)
    try:
        if legacy != filename(canonical) and legacy.exists():
            ledger.import_legacy(legacy)
    except BaseException:
        ledger.close()
        raise
    return canonical, ledger
