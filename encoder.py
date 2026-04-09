"""Rate Limiter Service — utility helpers for quota operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def allow_quota(data: Dict[str, Any]) -> Dict[str, Any]:
    """Quota allow — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "reset_at" not in result:
        raise ValueError(f"Quota must include 'reset_at'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["reset_at"]).encode()).hexdigest()[:12]
    return result


def consume_quotas(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Quota records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("consume_quotas: %d items after filter", len(out))
    return out[:limit]


def check_quota(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "limit" in updated and not isinstance(updated["limit"], (int, float)):
        try:
            updated["limit"] = float(updated["limit"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_quota(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Quota invariants."""
    required = ["reset_at", "limit", "remaining"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_quota: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def reset_quota_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk reset."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
