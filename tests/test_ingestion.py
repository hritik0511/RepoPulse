import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))
import gharchive  # noqa: E402


def test_hour_key_is_not_zero_padded():
    assert gharchive.hour_key(datetime(2024, 1, 1, 0, tzinfo=timezone.utc)) == "2024-01-01-0"
    assert gharchive.hour_key(datetime(2024, 1, 1, 15, tzinfo=timezone.utc)) == "2024-01-01-15"


def test_hour_range_crosses_midnight():
    start = datetime(2024, 1, 1, 23, tzinfo=timezone.utc)
    keys = [gharchive.hour_key(d) for d in gharchive.hour_range(start, 3)]
    assert keys == ["2024-01-01-23", "2024-01-02-0", "2024-01-02-1"]
