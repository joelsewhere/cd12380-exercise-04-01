"""
Generate fake data for two intervals to demonstrate schema evolution and
new-table arrival in a lakehouse pipeline.

Layout produced:

  output/
    interval_1/
      dataset_a/  ← initial schema
      dataset_b/  ← initial schema
    interval_2/
      dataset_a/  ← new column added
      dataset_b/  ← new column added
      dataset_c/  ← new dataset entirely
"""
from __future__ import annotations

import json
import random
import uuid
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUTPUT_DIR     = Path("landing")
RECORDS_PER_DS = 100
SEED           = 42

random.seed(SEED)


def write_jsonl_per_row(records: list[dict], dest: Path) -> None:
    """Write each record as a single-line JSON file under dest/."""
    dest.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        (dest / f"record_{i:06d}.json").write_text(json.dumps(record))

def write_csv(records: list[dict], dest: Path) -> None:
    """Write all records to a single CSV file under dest/."""
    dest.mkdir(parents=True, exist_ok=True)
    if not records:
        return
    pd.DataFrame(records).to_csv(dest / "data.csv", index=False)


def random_timestamp(start: datetime, days: int) -> str:
    """ISO timestamp within `days` days after `start`."""
    delta = timedelta(seconds=random.randint(0, days * 86_400))
    return (start + delta).isoformat()


WRITERS = {
    "json": write_jsonl_per_row,
    "csv" : write_csv,
}

# ── Interval 1 generators ──────────────────────────────────────────────────

def gen_dataset_a_v1(n: int) -> list[dict]:
    """User events — initial schema."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "user_id"  : f"u_{random.randint(1, 50):04d}",
            "action"   : random.choice(["click", "view", "scroll"]),
            "timestamp": random_timestamp(start, days=7),
        }
        for _ in range(n)
    ]


def gen_dataset_b_v1(n: int) -> list[dict]:
    """Product catalog — initial schema."""
    return [
        {
            "product_id": f"p_{i:05d}",
            "name"      : f"Product {i}",
            "price"     : round(random.uniform(5, 500), 2),
        }
        for i in range(n)
    ]


# ── Interval 2 generators (with schema evolution) ──────────────────────────

def gen_dataset_a_v2(n: int) -> list[dict]:
    """User events — adds session_id and device columns."""
    start = datetime(2026, 1, 8, tzinfo=timezone.utc)
    return [
        {
            "user_id"   : f"u_{random.randint(1, 50):04d}",
            "action"    : random.choice(["click", "view", "scroll", "purchase"]),
            "timestamp" : random_timestamp(start, days=7),
            "session_id": str(uuid.uuid4()),
            "device"    : random.choice(["mobile", "desktop", "tablet"]),
        }
        for _ in range(n)
    ]


def gen_dataset_b_v2(n: int) -> list[dict]:
    """Product catalog — adds category and in_stock columns."""
    return [
        {
            "product_id": f"p_{i:05d}",
            "name"      : f"Product {i}",
            "price"     : round(random.uniform(5, 500), 2),
            "category"  : random.choice(["electronics", "apparel", "home", "books"]),
            "in_stock"  : random.choice([True, False]),
        }
        for i in range(n)
    ]


def gen_dataset_c_v1(n: int) -> list[dict]:
    """Reviews — new dataset introduced in interval 2."""
    start = datetime(2026, 1, 8, tzinfo=timezone.utc)
    return [
        {
            "review_id" : str(uuid.uuid4()),
            "product_id": f"p_{random.randint(0, 99):05d}",
            "user_id"   : f"u_{random.randint(1, 50):04d}",
            "rating"    : random.randint(1, 5),
            "timestamp" : random_timestamp(start, days=7),
        }
        for _ in range(n)
    ]


# ── Orchestration ──────────────────────────────────────────────────────────

INTERVALS = {
    "2026-01-01": {
        "dataset_a": (gen_dataset_a_v1, "json"),
        "dataset_b": (gen_dataset_b_v1, "json"),
    },
    "2026-01-02": {
        "dataset_a": (gen_dataset_a_v2, "json"),
        "dataset_b": (gen_dataset_b_v2, "json"),
        "dataset_c": (gen_dataset_c_v1, "csv"),
    },
}


def main() -> None:
    for interval, datasets in INTERVALS.items():
        for dataset_name, (generator, fmt) in datasets.items():
            records = generator(RECORDS_PER_DS)
            dest    = OUTPUT_DIR / interval / dataset_name
            WRITERS[fmt](records, dest)
            print(f"Wrote {len(records)} records to {dest} ({fmt})")

    print(f"\nDone. Layout under {OUTPUT_DIR}/:")
    for interval in INTERVALS:
        print(f"  {interval}/")
        for dataset_name in INTERVALS[interval]:
            print(f"    {dataset_name}/")


if __name__ == "__main__":
    main()