"""Small HTTP contracts shared by API, dashboard and smoke checks."""
import base64
from datetime import datetime, timezone
import json
import math


class ServingError(Exception):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def cursor_encode(value):
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":"), allow_nan=False).encode()).decode().rstrip("=")


def cursor_decode(value):
    try:
        if len(value) > 2048:
            raise ValueError()
        result = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if not isinstance(result, dict):
            raise ValueError()
        return result
    except (ValueError, UnicodeError, TypeError):
        raise ServingError(422, "INVALID_CURSOR", "Cursor không hợp lệ") from None


def utc(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except (ValueError, AttributeError):
        raise ServingError(422, "INVALID_TIME", "Thời gian phải là ISO 8601 có múi giờ") from None


def percentile95(values):
    return sorted(values)[math.ceil(len(values) * .95) - 1] if values else None
