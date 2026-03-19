"""
Snowflake bulk data loader using PUT + COPY INTO pattern.

Optimized for high-volume Parquet loading with parallel uploads,
warehouse scaling, and comprehensive error handling.

Usage:
    python load_to_snowflake.py --manifest ./generated_data/manifest.json
    python load_to_snowflake.py --manifest ./generated_data/manifest.json --verify-only
"""

import argparse
import json
import time
from pathlib import Path

import snowflake.connector
from tqdm import tqdm

from config import snowflake_config


class SnowflakeLoader:
    """Bulk data loader using Snowflake PUT + COPY INTO pattern."""

    LOAD_CONFIG = {
        "counterparties": {
            "stage": "@RAW.STG_COUNTERPARTY_DATA",
            "table": "RAW.RAW_COUNTERPARTIES",
            "warehouse": "SANCTIONS_LOADING_WH_XS",
            "file_format": "RAW.FF_PARQUET",
        },
        "vessels": {
            "stage": "@RAW.STG_VESSEL_DATA/master",
            "table": "RAW.RAW_VESSELS",
            "warehouse": "SANCTIONS_LOADING_WH_XS",
            "file_format": "RAW.FF_PARQUET",
        },
        "sanctions_lists": {
            "stage": "@RAW.STG_SANCTIONS_LISTS",
            "table": "RAW.RAW_SANCTIONS_LISTS",
            "warehouse": "SANCTIONS_LOADING_WH_XS",
            "file_format": "RAW.FF_PARQUET",
        },
        "trades": {
            "stage": "@RAW.STG_TRADE_DATA",
            "table": "RAW.RAW_TRADES",
            "warehouse": "SANCTIONS_LOADING_WH_M",
            "file_format": "RAW.FF_PARQUET",
        },
        "vessel_movements": {
            "stage": "@RAW.STG_VESSEL_DATA/movements",
            "table": "RAW.RAW_VESSEL_MOVEMENTS",
            "warehouse": "SANCTIONS_LOADING_WH_M",
            "file_format": "RAW.FF_PARQUET",
        },
        "screening_results": {
            "stage": "@RAW.STG_SCREENING_DATA",
            "table": "RAW.RAW_SCREENING_RESULTS",
            "warehouse": "SANCTIONS_LOADING_WH_XS",
            "file_format": "RAW.FF_PARQUET",
        },
    }

    def __init__(self):
        self.conn = snowflake.connector.connect(
            account=snowflake_config.account,
            user=snowflake_config.user,
            password=snowflake_config.password,
            role=snowflake_config.role,
            database=snowflake_config.database,
            schema=snowflake_config.schema,
            warehouse=snowflake_config.warehouse,
        )
        self.cursor = self.conn.cursor()
        self._configure_session()

    def _configure_session(self):
        """Set optimal session parameters for bulk loading."""
        self.cursor.execute("ALTER SESSION SET BINARY_INPUT_FORMAT = 'BASE64'")
        self.cursor.execute("ALTER SESSION SET BINARY_OUTPUT_FORMAT = 'BASE64'")
        self.cursor.execute("ALTER SESSION SET AUTOCOMMIT = TRUE")
        self.cursor.execute(
            "ALTER SESSION SET QUERY_TAG = "
            "'{\"application\": \"sanctions_loader\", \"phase\": \"data_ingestion\"}'"
        )

    def _put_files(self, local_path: Path, stage: str, parallel: int = 8) -> dict:
        """Upload files to Snowflake internal stage."""
        put_sql = (
            f"PUT 'file://{local_path}/*.parquet' {stage}/ "
            f"PARALLEL = {parallel} "
            f"AUTO_COMPRESS = FALSE "
            f"OVERWRITE = TRUE"
        )

        print(f"  Uploading files from {local_path} -> {stage}")
        start = time.time()
        result = self.cursor.execute(put_sql).fetchall()
        elapsed = time.time() - start

        uploaded = sum(1 for r in result if r[6] == "UPLOADED")
        skipped = sum(1 for r in result if r[6] == "SKIPPED")

        print(f"  PUT: {uploaded} uploaded, {skipped} skipped in {elapsed:.1f}s")

        return {"uploaded": uploaded, "skipped": skipped, "elapsed_seconds": elapsed}

    def _copy_into(self, stage: str, table: str, file_format: str) -> dict:
        """Execute COPY INTO with optimal settings for Parquet."""
        copy_sql = f"""
            COPY INTO {table}
            FROM {stage}/
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'SKIP_FILE'
            PURGE = FALSE
        """

        print(f"  Loading data into {table}...")
        start = time.time()
        result = self.cursor.execute(copy_sql).fetchall()
        elapsed = time.time() - start

        total_rows = sum(r[3] for r in result if r[3])
        total_errors = sum(r[5] for r in result if r[5])

        print(f"  Loaded {total_rows:,} rows in {elapsed:.1f}s ({total_errors} errors)")

        return {
            "rows_loaded": total_rows,
            "errors": total_errors,
            "elapsed_seconds": elapsed,
        }

    def load_data_type(self, data_type: str, data_dir: Path) -> dict:
        """Load a specific data type end-to-end."""
        config = self.LOAD_CONFIG[data_type]

        print(f"\n{'=' * 50}")
        print(f"Loading: {data_type.upper()}")
        print(f"{'=' * 50}")

        # Switch to appropriate warehouse
        self.cursor.execute(f"USE WAREHOUSE {config['warehouse']}")

        # Find all Parquet files (including nested date partitions)
        parquet_files = list(data_dir.rglob("*.parquet"))

        if not parquet_files:
            print(f"  No parquet files found in {data_dir}")
            return {"total_rows": 0, "total_errors": 0}

        print(f"  Found {len(parquet_files)} parquet files")

        parent_dirs = sorted(set(f.parent for f in parquet_files))

        total_rows = 0
        total_errors = 0

        for parent_dir in tqdm(parent_dirs, desc=f"Loading {data_type}"):
            self._put_files(parent_dir, config["stage"])

            copy_result = self._copy_into(
                config["stage"],
                config["table"],
                config["file_format"],
            )

            total_rows += copy_result["rows_loaded"]
            total_errors += copy_result["errors"]

        print(f"\n  TOTAL: {total_rows:,} rows loaded, {total_errors} errors")

        return {"total_rows": total_rows, "total_errors": total_errors}

    def load_all(self, manifest_path: Path) -> dict:
        """Load all data types based on manifest."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        base_dir = manifest_path.parent
        results = {}

        # Load order matters - dimensions first, then facts
        load_order = [
            "counterparties",
            "vessels",
            "sanctions_lists",
            "trades",
            "vessel_movements",
            "screening_results",
        ]

        for data_type in load_order:
            if data_type in manifest["files"] and manifest["files"][data_type]:
                data_dir = base_dir / data_type
                result = self.load_data_type(data_type, data_dir)
                results[data_type] = result

        return results

    def verify_loads(self):
        """Verify data was loaded correctly."""
        print(f"\n{'=' * 60}")
        print("VERIFICATION - Row Counts")
        print(f"{'=' * 60}")

        tables = [
            "RAW.RAW_COUNTERPARTIES",
            "RAW.RAW_VESSELS",
            "RAW.RAW_SANCTIONS_LISTS",
            "RAW.RAW_TRADES",
            "RAW.RAW_VESSEL_MOVEMENTS",
            "RAW.RAW_SCREENING_RESULTS",
        ]

        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"  {table:40s}: {count:>15,}")

        print(f"{'=' * 60}")

    def close(self):
        """Close connection."""
        self.cursor.close()
        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load data to Snowflake")
    parser.add_argument("--manifest", required=True, help="Path to manifest.json")
    parser.add_argument("--verify-only", action="store_true", help="Only verify existing loads")

    args = parser.parse_args()

    loader = SnowflakeLoader()

    try:
        if args.verify_only:
            loader.verify_loads()
        else:
            loader.load_all(Path(args.manifest))
            loader.verify_loads()
    finally:
        loader.close()


if __name__ == "__main__":
    main()
