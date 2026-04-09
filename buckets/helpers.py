"""Rate Limiter Service — utility helpers for bucket operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def reset_bucket(data: Dict[str, Any]) -> Dict[str, Any]:
    """Bucket reset — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "window_secs" not in result:
        raise ValueError(f"Bucket must include 'window_secs'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["window_secs"]).encode()).hexdigest()[:12]
    return result


def consume_buckets(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Bucket records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("consume_buckets: %d items after filter", len(out))
    return out[:limit]


def throttle_bucket(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "burst" in updated and not isinstance(updated["burst"], (int, float)):
        try:
            updated["burst"] = float(updated["burst"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_bucket(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Bucket invariants."""
    required = ["window_secs", "burst", "remaining"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_bucket: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def check_bucket_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk check."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
