"""
Snowflake bulk data loader using PUT + COPY INTO pattern.

Loads generated Parquet files into Snowflake raw tables via internal stages.
Supports parallel uploads and warehouse scaling per data type.

Usage:
    python load_to_snowflake.py --manifest ./generated_data/manifest.json
    python load_to_snowflake.py --manifest ./generated_data/manifest.json --verify-only
"""

import argparse
import json
import logging
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from config import snowflake_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Mapping of data type to stage, table, warehouse, and file format
LOAD_CONFIG = {
    "counterparties": {
        "stage": "STG_COUNTERPARTY_DATA",
        "table": "RAW_COUNTERPARTIES",
        "warehouse": "SANCTIONS_LOADING_WH_XS",
        "file_format": "FF_PARQUET",
    },
    "sanctions_lists": {
        "stage": "STG_SANCTIONS_LISTS",
        "table": "RAW_SANCTIONS_LISTS",
        "warehouse": "SANCTIONS_LOADING_WH_XS",
        "file_format": "FF_PARQUET",
    },
    "vessels": {
        "stage": "STG_VESSEL_DATA",
        "table": "RAW_VESSELS",
        "warehouse": "SANCTIONS_LOADING_WH_XS",
        "file_format": "FF_PARQUET",
    },
    "trades": {
        "stage": "STG_TRADE_DATA",
        "table": "RAW_TRADES",
        "warehouse": "SANCTIONS_LOADING_WH_M",
        "file_format": "FF_PARQUET",
    },
    "vessel_movements": {
        "stage": "STG_VESSEL_DATA",
        "table": "RAW_VESSEL_MOVEMENTS",
        "warehouse": "SANCTIONS_LOADING_WH_M",
        "file_format": "FF_PARQUET",
    },
    "screening_results": {
        "stage": "STG_SCREENING_DATA",
        "table": "RAW_SCREENING_RESULTS",
        "warehouse": "SANCTIONS_LOADING_WH_M",
        "file_format": "FF_PARQUET",
    },
}


class SnowflakeLoader:
    """Load generated Parquet files into Snowflake via PUT + COPY INTO."""

    def __init__(self):
        self.conn = snowflake.connector.connect(
            account=snowflake_config.account,
            user=snowflake_config.user,
            password=snowflake_config.password,
            role=snowflake_config.role,
            warehouse=snowflake_config.warehouse,
            database=snowflake_config.database,
            schema=snowflake_config.schema,
        )
        logger.info(f"Connected to Snowflake: {snowflake_config.database}")

    def _put_files(self, file_paths: list[str], stage: str, parallel: int = 8) -> None:
        """Upload files to Snowflake internal stage via PUT."""
        cursor = self.conn.cursor()
        for file_path in file_paths:
            sql = f"PUT 'file://{file_path}' @{stage}/ AUTO_COMPRESS=FALSE PARALLEL={parallel} OVERWRITE=TRUE"
            logger.info(f"  PUT {Path(file_path).name} → @{stage}")
            cursor.execute(sql)
        cursor.close()

    def _copy_into(self, stage: str, table: str, file_format: str) -> int:
        """Execute COPY INTO from stage to table. Returns rows loaded."""
        cursor = self.conn.cursor()
        sql = f"""
            COPY INTO {table}
            FROM @{stage}/
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'SKIP_FILE'
            PURGE = TRUE
        """
        cursor.execute(sql)
        result = cursor.fetchall()
        total_rows = sum(row[3] for row in result if len(row) > 3)
        cursor.close()
        return total_rows

    def load_data_type(self, data_type: str, file_paths: list[str]) -> int:
        """Load a specific data type: switch warehouse, PUT files, COPY INTO."""
        config = LOAD_CONFIG[data_type]
        cursor = self.conn.cursor()

        # Switch to appropriate warehouse
        cursor.execute(f"USE WAREHOUSE {config['warehouse']}")
        logger.info(f"Loading {data_type}: {len(file_paths)} files → {config['table']}")

        # PUT files to stage
        self._put_files(file_paths, config["stage"])

        # COPY INTO table
        rows = self._copy_into(config["stage"], config["table"], config["file_format"])
        logger.info(f"  Loaded {rows:,} rows into {config['table']}")

        cursor.close()
        return rows

    def load_all(self, manifest_path: str) -> dict:
        """Load all data types from a manifest file."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        results = {}
        for data_type, info in manifest.get("files", {}).items():
            file_paths = info.get("files", [])
            if not file_paths:
                logger.warning(f"No files found for {data_type}, skipping")
                continue
            rows = self.load_data_type(data_type, file_paths)
            results[data_type] = rows

        return results

    def verify_loads(self) -> dict:
        """Verify row counts in all raw tables."""
        cursor = self.conn.cursor()
        counts = {}

        for data_type, config in LOAD_CONFIG.items():
            cursor.execute(f"SELECT COUNT(*) FROM {config['table']}")
            count = cursor.fetchone()[0]
            counts[config["table"]] = count
            logger.info(f"  {config['table']}: {count:,} rows")

        cursor.close()
        return counts

    def close(self) -> None:
        """Close Snowflake connection."""
        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load data to Snowflake")
    parser.add_argument("--manifest", required=True, help="Path to manifest.json")
    parser.add_argument("--verify-only", action="store_true", help="Only verify row counts")
    args = parser.parse_args()

    loader = SnowflakeLoader()

    try:
        if args.verify_only:
            logger.info("Verifying table row counts...")
            loader.verify_loads()
        else:
            logger.info(f"Loading data from manifest: {args.manifest}")
            results = loader.load_all(args.manifest)
            logger.info(f"Load complete. Results: {json.dumps(results, indent=2)}")
            logger.info("Verifying final counts...")
            loader.verify_loads()
    finally:
        loader.close()


if __name__ == "__main__":
    main()
