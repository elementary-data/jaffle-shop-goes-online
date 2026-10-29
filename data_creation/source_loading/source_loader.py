"""
Source loader.

Replaces `dbt seed` for the business data. Reads the generated CSVs and writes
them into warehouse tables that the dbt project consumes via `source()`:

    generated_data/training/raw_*_training.csv     -> raw_customers / raw_orders / ...
    generated_data/validation/raw_*_validation.csv -> raw_customers / raw_orders / ...
    generated_data/ads/stg_*_ads.csv               -> stg_google_ads / ...
    generated_data/sessions/stg_*_sessions.csv     -> stg_app_sessions / ...

The `--select` values mirror the old dbt seed selectors (training / validation /
ads / sessions) so orchestration is a drop-in swap. `training` and `validation`
load the SAME raw tables from different CSV variants, matching real life where
a single table's contents change over time.
"""

import argparse
import logging
import os
from datetime import datetime
from typing import List, Optional, Tuple

from utils.csv import split_csv_to_headers_and_data
from data_creation.source_loading.warehouse_connection import (
    REPO_ROOT,
    WarehouseConnection,
)

logger = logging.getLogger(__name__)

GENERATED_DATA_DIR = os.path.join(REPO_ROOT, "generated_data")
INSERT_BATCH_SIZE = 5000

Column = Tuple[str, str]

RAW_COLUMNS = {
    "raw_customers": [("id", "integer"), ("first_name", "varchar"), ("last_name", "varchar")],
    "raw_orders": [
        ("id", "integer"),
        ("user_id", "integer"),
        ("order_date", "timestamp_ntz"),
        ("status", "varchar"),
    ],
    "raw_payments": [
        ("id", "integer"),
        ("order_id", "integer"),
        ("payment_method", "varchar"),
        ("amount", "integer"),
    ],
    "raw_signups": [
        ("id", "integer"),
        ("user_id", "integer"),
        ("user_email", "varchar"),
        ("hashed_password", "varchar"),
        ("signup_date", "timestamp_ntz"),
    ],
}

ADS_COLUMNS = [
    ("ad_id", "varchar"),
    ("date", "date"),
    ("utm_medium", "varchar"),
    ("utm_campain", "varchar"),
    ("cost", "float"),
]

SESSION_COLUMNS = [
    ("session_id", "varchar"),
    ("customer_id", "integer"),
    ("started_at", "timestamp_ntz"),
    ("ended_at", "timestamp_ntz"),
    ("utm_source", "varchar"),
    ("ad_id", "varchar"),
]


class TableLoad:
    def __init__(self, table: str, columns: List[Column], csv_path: str):
        self.table = table
        self.columns = columns
        self.csv_path = csv_path


def _raw_loads(variant: str) -> List[TableLoad]:
    directory = os.path.join(GENERATED_DATA_DIR, variant)
    return [
        TableLoad(
            table=table,
            columns=columns,
            csv_path=os.path.join(directory, f"{table}_{variant}.csv"),
        )
        for table, columns in RAW_COLUMNS.items()
    ]


def _ads_loads() -> List[TableLoad]:
    directory = os.path.join(GENERATED_DATA_DIR, "ads")
    return [
        TableLoad(table, ADS_COLUMNS, os.path.join(directory, f"{table}.csv"))
        for table in ("stg_google_ads", "stg_facebook_ads", "stg_instagram_ads")
    ]


def _session_loads() -> List[TableLoad]:
    directory = os.path.join(GENERATED_DATA_DIR, "sessions")
    return [
        TableLoad(table, SESSION_COLUMNS, os.path.join(directory, f"{table}.csv"))
        for table in ("stg_app_sessions", "stg_website_sessions")
    ]


def _loads_for_select(select: str) -> List[TableLoad]:
    if select in ("training", "validation"):
        return _raw_loads(select)
    if select == "ads":
        return _ads_loads()
    if select == "sessions":
        return _session_loads()
    raise ValueError(f"Unknown select '{select}'.")


def _convert(value: str, col_type: str):
    value = value.strip() if isinstance(value, str) else value
    if value is None or value == "":
        return None
    if col_type == "integer":
        return int(float(value))
    if col_type == "float":
        return float(value)
    if col_type == "date":
        return datetime.strptime(value, "%Y-%m-%d").date()
    if col_type == "timestamp_ntz":
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return value


def _load_table(cursor, database: str, schema: str, load: TableLoad) -> int:
    fqn = f"{database}.{schema}.{load.table}"
    columns_ddl = ", ".join(f"{name} {col_type}" for name, col_type in load.columns)
    cursor.execute(f"create or replace table {fqn} ({columns_ddl})")

    _, rows = split_csv_to_headers_and_data(csv_path=load.csv_path)
    if not rows:
        logger.warning("No rows found for %s (%s); created empty table.", load.table, load.csv_path)
        return 0

    col_types = [col_type for _, col_type in load.columns]
    col_names = ", ".join(name for name, _ in load.columns)
    placeholders = ", ".join(["%s"] * len(load.columns))
    insert_sql = f"insert into {fqn} ({col_names}) values ({placeholders})"

    converted = [
        [_convert(cell, col_types[i]) for i, cell in enumerate(row[: len(col_types)])]
        for row in rows
    ]

    for start in range(0, len(converted), INSERT_BATCH_SIZE):
        cursor.executemany(insert_sql, converted[start : start + INSERT_BATCH_SIZE])

    return len(converted)


def load(
    select: str,
    target: Optional[str] = None,
    profiles_dir: Optional[str] = None,
) -> None:
    """Load a group of source tables into the warehouse. `select` mirrors the old
    dbt seed selectors: training | validation | ads | sessions."""
    loads = _loads_for_select(select)
    with WarehouseConnection(target=target, profiles_dir=profiles_dir) as warehouse:
        cursor = warehouse.cursor()
        try:
            cursor.execute(
                f"create schema if not exists {warehouse.database}.{warehouse.schema}"
            )
            for table_load in loads:
                count = _load_table(
                    cursor, warehouse.database, warehouse.schema, table_load
                )
                logger.info("Loaded %s rows into %s", count, table_load.table)
                print(f"Loaded {count} rows into {table_load.table}")
        finally:
            cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Load demo source tables into the warehouse.")
    parser.add_argument(
        "--select",
        required=True,
        help="Comma-separated groups to load: training / validation / ads / sessions",
    )
    parser.add_argument("--target", help="dbt target to read connection details from")
    parser.add_argument("--profiles-dir", help="Directory containing profiles.yml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    for select in [s.strip() for s in args.select.split(",") if s.strip()]:
        load(select=select, target=args.target, profiles_dir=args.profiles_dir)


if __name__ == "__main__":
    main()
