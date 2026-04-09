"""Rate Limiter Service — Bucket service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class RateProcessor:
    """Business-logic service for Bucket operations in Rate Limiter Service."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("RateProcessor started")

    def check(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the check workflow for a new Bucket."""
        if "remaining" not in payload:
            raise ValueError("Missing required field: remaining")
        record = self._repo.insert(
            payload["remaining"], payload.get("reset_at"),
            **{k: v for k, v in payload.items()
              if k not in ("remaining", "reset_at")}
        )
        if self._events:
            self._events.emit("bucket.checkd", record)
        return record

    def reset(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Bucket and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Bucket {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("bucket.resetd", updated)
        return updated

    def deny(self, rec_id: str) -> None:
        """Remove a Bucket and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Bucket {rec_id!r} not found")
        if self._events:
            self._events.emit("bucket.denyd", {"id": rec_id})

    def search(
        self,
        remaining: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search buckets by *remaining* and/or *status*."""
        filters: Dict[str, Any] = {}
        if remaining is not None:
            filters["remaining"] = remaining
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search buckets: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Bucket counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
