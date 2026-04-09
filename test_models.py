"""Rate Limiter Service — exception types."""

class RateLimiterServiceError(Exception):
    """Base exception for all Rate Limiter Service errors."""

    def __init__(self, message: str, code: int = 0) -> None:
        super().__init__(message)
        self.code = code

    def __str__(self) -> str:
        return f"[{self.code}] {super().__str__()}"


class QuotaNotFoundError(RateLimiterServiceError):
    """Raised when a Quota record cannot be located."""

    def __init__(self, record_id: str) -> None:
        super().__init__(f"Quota {record_id!r} not found", code=404)
        self.record_id = record_id


class QuotaValidationError(RateLimiterServiceError):
    """Raised when a Quota fails field validation."""

    def __init__(self, field: str, reason: str) -> None:
        super().__init__(f"Invalid {field!r}: {reason}", code=422)
        self.field  = field
        self.reason = reason


class RateLimiterServiceConflictError(RateLimiterServiceError):
    """Raised on duplicate or conflicting Rate Limiter Service state."""

    def __init__(self, detail: str) -> None:
        super().__init__(f"Conflict: {detail}", code=409)


def raise_if_none(value: object, label: str = "Quota") -> object:
    """Raise QuotaNotFoundError if *value* is None."""
    if value is None:
        raise QuotaNotFoundError(label)
    return value
