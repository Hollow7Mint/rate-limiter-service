"""Rate Limiter Service — Window manager layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class RateManager:
    """Window manager for the Rate Limiter Service application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._client_id = self._cfg.get("client_id", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def throttle_window(
        self, client_id: Any, limit: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Window record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "client_id": client_id,
            "limit": limit,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("throttle_window: created %s", saved["id"])
        return saved

    def get_window(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Window by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_window: %s not found", record_id)
        return record

    def consume_window(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Window."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Window {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def deny_window(self, record_id: str) -> bool:
        """Remove a Window; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("deny_window: removed %s", record_id)
        return True

    def list_windows(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Window records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_windows: %d results", len(results))
        return results

    def iter_windows(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Window records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_windows(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
