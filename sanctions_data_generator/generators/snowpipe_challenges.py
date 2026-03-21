"""
Snowpipe challenge simulator for enterprise-scale Snowflake ingestion.

Generates realistic file-level and pipeline-level problems that large
organisations encounter when running Snowpipe at scale (millions of files
per day across multiple pipes).  Each challenge scenario produces:

    1. **Problem files** — malformed, partial, duplicate, or mis-formatted data
       that trigger real Snowpipe failure modes.
    2. **Correction files / SQL** — the remediation a data engineer would apply
       in production (reload, dedup, schema-migration, etc.).

Challenge categories (mapped to real-world root causes):

    ┌──────────────────────────────────────────────────────────────────────┐
    │  #  │ Challenge                       │ Snowpipe Symptom            │
    │─────┼─────────────────────────────────┼─────────────────────────────│
    │  1  │ Schema evolution / drift         │ COPY fails, silent drops   │
    │  2  │ File format mismatch             │ LOAD_FAILED status         │
    │  3  │ Partial / truncated files        │ row_parsed < row_count     │
    │  4  │ Duplicate file reprocessing      │ Already-loaded skip        │
    │  5  │ Zero-byte / empty files          │ No rows loaded             │
    │  6  │ Oversized single files           │ Warehouse timeout          │
    │  7  │ Character encoding corruption    │ Parse errors, mojibake     │
    │  8  │ Notification queue failures      │ Files stuck in stage       │
    │  9  │ Pipe stale after DDL             │ Pipe goes STALE            │
    │ 10  │ ON_ERROR silent data loss        │ SKIP_FILE hides bad rows   │
    │ 11  │ File ordering dependencies       │ FK violations on load      │
    │ 12  │ Concurrent writer conflicts      │ Corrupt / interleaved data │
    │ 13  │ Load latency / SLA breaches      │ Files queued > threshold   │
    │ 14  │ Numeric precision overflow       │ Implicit truncation        │
    │ 15  │ Timestamp format inconsistency   │ Parse failures, wrong TZ   │
    │ 16  │ Stage permission / IAM failures  │ Access denied on COPY      │
    │ 17  │ COPY history metadata expiry     │ 14-day window re-load      │
    │ 18  │ Retry storm / thundering herd    │ Warehouse credit spike     │
    │ 19  │ Column name case sensitivity     │ Column mapping failures    │
    │ 20  │ Multi-byte path / filename       │ Stage listing failures     │
    └──────────────────────────────────────────────────────────────────────┘

Usage:
    simulator = SnowpipeChallengeSimulator(seed=42)
    manifest = simulator.generate_all_challenges("./snowpipe_challenges")
"""

import json
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Challenge configuration
# ---------------------------------------------------------------------------


@dataclass
class SnowpipeChallengeConfig:
    """Controls which challenges are generated and at what intensity."""

    # Master toggle per challenge (True = generate)
    schema_evolution: bool = True
    file_format_mismatch: bool = True
    partial_files: bool = True
    duplicate_files: bool = True
    empty_files: bool = True
    oversized_files: bool = True
    encoding_corruption: bool = True
    notification_failures: bool = True
    pipe_stale: bool = True
    on_error_data_loss: bool = True
    file_ordering: bool = True
    concurrent_writers: bool = True
    load_latency: bool = True
    numeric_overflow: bool = True
    timestamp_formats: bool = True
    stage_permissions: bool = True
    copy_history_expiry: bool = True
    retry_storm: bool = True
    column_case_sensitivity: bool = True
    multibyte_paths: bool = True

    # Records per challenge scenario
    records_per_scenario: int = 1000

    # Number of variant files per challenge
    variants_per_challenge: int = 3


# ---------------------------------------------------------------------------
# Core simulator
# ---------------------------------------------------------------------------


class SnowpipeChallengeSimulator:
    """
    Generates file sets that exercise every major Snowpipe failure mode.

    For each challenge, produces:
      - problem/  — files that trigger the issue
      - corrected/ — files with the fix applied
      - manifest.json — metadata describing what was injected and how to fix it
    """

    # Reference schema matching RAW_TRADES
    TRADE_COLUMNS = [
        ("trade_id", "VARCHAR(20)"),
        ("trade_reference", "VARCHAR(50)"),
        ("trade_timestamp", "TIMESTAMP_NTZ"),
        ("trade_date", "DATE"),
        ("trade_type", "VARCHAR(20)"),
        ("trade_status", "VARCHAR(20)"),
        ("buyer_counterparty_id", "VARCHAR(20)"),
        ("seller_counterparty_id", "VARCHAR(20)"),
        ("commodity_code", "VARCHAR(20)"),
        ("commodity_name", "VARCHAR(200)"),
        ("total_value_usd", "NUMBER(18,2)"),
        ("currency", "VARCHAR(3)"),
        ("origin_country", "VARCHAR(3)"),
        ("destination_country", "VARCHAR(3)"),
        ("vessel_id", "VARCHAR(20)"),
        ("sanctions_risk_score", "NUMBER(5,2)"),
        ("source_system", "VARCHAR(50)"),
        ("_loaded_at", "TIMESTAMP_NTZ"),
    ]

    VESSEL_COLUMNS = [
        ("vessel_id", "VARCHAR(20)"),
        ("imo_number", "VARCHAR(10)"),
        ("mmsi", "VARCHAR(9)"),
        ("vessel_name", "VARCHAR(200)"),
        ("vessel_type", "VARCHAR(50)"),
        ("flag_state", "VARCHAR(3)"),
        ("dwt", "NUMBER(12,2)"),
        ("gross_tonnage", "NUMBER(12,2)"),
        ("year_built", "INTEGER"),
        ("status", "VARCHAR(20)"),
        ("is_flagged", "BOOLEAN"),
        ("registered_owner", "VARCHAR(500)"),
        ("source_system", "VARCHAR(50)"),
        ("_loaded_at", "TIMESTAMP_NTZ"),
    ]

    def __init__(
        self,
        config: SnowpipeChallengeConfig | None = None,
        seed: int = 42,
    ):
        self.config = config or SnowpipeChallengeConfig()
        self.seed = seed
        self.rng = np.random.RandomState(seed)
        random.seed(seed)
        self.fake_names = [
            "Petrobras Trading",
            "Glencore International",
            "Vitol Asia",
            "Trafigura Maritime",
            "Gunvor Group",
            "Mercuria Energy",
            "Koch Supply",
            "Litasco SA",
            "Socar Trading",
            "NIOC International",
        ]
        self.commodities = [
            ("CRD_BRT", "Brent Crude"),
            ("CRD_WTI", "WTI Crude"),
            ("PRD_ULSD", "Ultra Low Sulphur Diesel"),
            ("LNG_SPOT", "LNG Spot"),
            ("MET_COPP", "Copper"),
        ]

    # ─── Helpers ───────────────────────────────────────────────────────────

    def _generate_base_trades(self, n: int) -> pd.DataFrame:
        """Generate n clean trade records as a baseline."""
        records = []
        base_ts = datetime(2025, 6, 15, 8, 0, 0)
        for i in range(n):
            commodity = self.commodities[i % len(self.commodities)]
            records.append(
                {
                    "trade_id": f"TRD-{100000 + i}",
                    "trade_reference": f"REF-{200000 + i}",
                    "trade_timestamp": (base_ts + timedelta(seconds=i * 5)).isoformat(),
                    "trade_date": "2025-06-15",
                    "trade_type": ["PHYSICAL", "PAPER", "SWAP", "FUTURE", "OPTION"][
                        i % 5
                    ],
                    "trade_status": "CONFIRMED",
                    "buyer_counterparty_id": f"CP-{10000 + (i % 500)}",
                    "seller_counterparty_id": f"CP-{20000 + (i % 500)}",
                    "commodity_code": commodity[0],
                    "commodity_name": commodity[1],
                    "total_value_usd": round(self.rng.uniform(10000, 50000000), 2),
                    "currency": "USD",
                    "origin_country": ["US", "SA", "RU", "AE", "NG"][i % 5],
                    "destination_country": ["CN", "IN", "JP", "KR", "DE"][i % 5],
                    "vessel_id": f"VSL-{30000 + (i % 200)}",
                    "sanctions_risk_score": round(self.rng.uniform(0, 100), 2),
                    "source_system": "ETRM_PRIMARY",
                    "_loaded_at": datetime.now().isoformat(),
                }
            )
        return pd.DataFrame(records)

    def _generate_base_vessels(self, n: int) -> pd.DataFrame:
        """Generate n clean vessel records."""
        records = []
        for i in range(n):
            records.append(
                {
                    "vessel_id": f"VSL-{30000 + i}",
                    "imo_number": f"{9100000 + i}",
                    "mmsi": f"{21000000 + i}",
                    "vessel_name": f"M/V {self.fake_names[i % len(self.fake_names)].split()[0].upper()} SPIRIT",
                    "vessel_type": ["VLCC", "SUEZMAX", "AFRAMAX", "LNG_CARRIER"][i % 4],
                    "flag_state": ["PA", "LR", "MH", "GR", "SG"][i % 5],
                    "dwt": round(self.rng.uniform(50000, 320000), 2),
                    "gross_tonnage": round(self.rng.uniform(30000, 170000), 2),
                    "year_built": int(self.rng.randint(1995, 2024)),
                    "status": "ACTIVE",
                    "is_flagged": bool(self.rng.random() < 0.05),
                    "registered_owner": self.fake_names[i % len(self.fake_names)],
                    "source_system": "VESSEL_REGISTRY",
                    "_loaded_at": datetime.now().isoformat(),
                }
            )
        return pd.DataFrame(records)

    def _write_parquet(self, df: pd.DataFrame, path: Path) -> Path:
        """Write DataFrame to Parquet with Snappy compression."""
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path, compression="snappy")
        return path

    def _write_csv(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> Path:
        """Write DataFrame to CSV."""
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, **kwargs)
        return path

    def _write_json_lines(self, df: pd.DataFrame, path: Path) -> Path:
        """Write DataFrame as JSON lines."""
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(path, orient="records", lines=True, date_format="iso")
        return path

    # ─── Challenge 1: Schema Evolution / Drift ─────────────────────────────

    def challenge_schema_evolution(self, out_dir: Path) -> dict:
        """
        Simulate schema evolution where source adds/removes/renames columns.

        Real-world cause: Upstream ETRM system deploys a release that adds
        new columns, renames existing ones, or changes data types.  Snowpipe
        MATCH_BY_COLUMN_NAME silently drops unknown columns or errors on
        type mismatches.
        """
        challenge_dir = out_dir / "01_schema_evolution"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: New column added by source (unknown to Snowflake table)
        v_a = base.copy()
        v_a["compliance_officer_id"] = [f"CO-{i}" for i in range(n)]
        v_a["risk_review_required"] = self.rng.choice([True, False], n)
        problems.append(
            self._write_parquet(v_a, challenge_dir / "problem" / "new_columns.parquet")
        )
        # Correction: Strip unknown columns to match target schema
        v_a_fix = v_a.drop(columns=["compliance_officer_id", "risk_review_required"])
        corrections.append(
            self._write_parquet(
                v_a_fix, challenge_dir / "corrected" / "new_columns_stripped.parquet"
            )
        )

        # Variant B: Column renamed (trade_type → transaction_type)
        v_b = base.copy()
        v_b = v_b.rename(columns={"trade_type": "transaction_type"})
        problems.append(
            self._write_parquet(
                v_b, challenge_dir / "problem" / "renamed_column.parquet"
            )
        )
        v_b_fix = v_b.rename(columns={"transaction_type": "trade_type"})
        corrections.append(
            self._write_parquet(
                v_b_fix, challenge_dir / "corrected" / "renamed_column_fixed.parquet"
            )
        )

        # Variant C: Column type change (total_value_usd becomes string)
        v_c = base.copy()
        v_c["total_value_usd"] = v_c["total_value_usd"].apply(lambda x: f"${x:,.2f}")
        problems.append(
            self._write_parquet(v_c, challenge_dir / "problem" / "type_change.parquet")
        )
        v_c_fix = base.copy()
        corrections.append(
            self._write_parquet(
                v_c_fix, challenge_dir / "corrected" / "type_change_fixed.parquet"
            )
        )

        # Variant D: Column removed by source
        v_d = base.drop(columns=["vessel_id", "sanctions_risk_score"])
        problems.append(
            self._write_parquet(
                v_d, challenge_dir / "problem" / "missing_columns.parquet"
            )
        )
        v_d_fix = base.copy()
        corrections.append(
            self._write_parquet(
                v_d_fix,
                challenge_dir / "corrected" / "missing_columns_restored.parquet",
            )
        )

        return {
            "challenge": "schema_evolution",
            "description": (
                "Source system schema changes: new columns, renamed columns, "
                "type changes, and dropped columns"
            ),
            "root_cause": (
                "Upstream ETRM/KYC system deploys without coordinating "
                "schema changes with the data platform team"
            ),
            "snowpipe_symptom": (
                "MATCH_BY_COLUMN_NAME silently drops unknown columns; "
                "type mismatches cause LOAD_FAILED; missing columns load as NULL"
            ),
            "correction_strategy": (
                "1. ALTER TABLE ADD COLUMN for new fields\n"
                "2. CREATE OR REPLACE PIPE with updated COPY INTO\n"
                "3. Use COPY transformation to rename/cast columns\n"
                "4. Backfill NULLs from corrected reload"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 2: File Format Mismatch ─────────────────────────────────

    def challenge_file_format_mismatch(self, out_dir: Path) -> dict:
        """
        Simulate wrong file format landing in Snowpipe stage.

        Real-world cause: Upstream job misconfigured to produce CSV instead of
        Parquet (or vice versa), or delimiter/quoting changes after a deploy.
        """
        challenge_dir = out_dir / "02_file_format_mismatch"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: CSV file in a Parquet-configured pipe
        problems.append(
            self._write_csv(base, challenge_dir / "problem" / "trades_as_csv.parquet")
        )
        corrections.append(
            self._write_parquet(
                base, challenge_dir / "corrected" / "trades_as_parquet.parquet"
            )
        )

        # Variant B: Pipe-delimited instead of comma-delimited CSV
        problems.append(
            self._write_csv(
                base,
                challenge_dir / "problem" / "pipe_delimited.csv",
                sep="|",
            )
        )
        corrections.append(
            self._write_csv(
                base,
                challenge_dir / "corrected" / "comma_delimited.csv",
            )
        )

        # Variant C: JSON lines where Parquet expected
        problems.append(
            self._write_json_lines(
                base, challenge_dir / "problem" / "trades_as_jsonl.parquet"
            )
        )
        corrections.append(
            self._write_parquet(
                base, challenge_dir / "corrected" / "trades_correct_format.parquet"
            )
        )

        return {
            "challenge": "file_format_mismatch",
            "description": (
                "Wrong file format lands in stage: CSV with .parquet extension, "
                "pipe-delimited instead of comma, JSON lines in Parquet pipe"
            ),
            "root_cause": (
                "Upstream ETL job misconfiguration after deployment, or "
                "new source system onboarded without format validation"
            ),
            "snowpipe_symptom": (
                "LOAD_FAILED with 'Not a Parquet file' or parse errors; "
                "SKIP_FILE causes entire batch to be lost"
            ),
            "correction_strategy": (
                "1. Validate file headers in pre-load Lambda/Function\n"
                "2. Create FORMAT_NAME variants for each expected format\n"
                "3. Use VALIDATION_MODE = RETURN_ERRORS before production load\n"
                "4. Re-export from source in correct format and re-stage"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 3: Partial / Truncated Files ────────────────────────────

    def challenge_partial_files(self, out_dir: Path) -> dict:
        """
        Simulate files that are incomplete — written to stage while still
        being produced, or truncated due to network/storage failures.
        """
        challenge_dir = out_dir / "03_partial_files"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: Parquet file truncated mid-write (corrupt footer)
        full_path = challenge_dir / "problem" / "truncated_parquet.parquet"
        self._write_parquet(base, full_path)
        raw_bytes = full_path.read_bytes()
        # Truncate at 60% — destroys the Parquet footer
        truncated = raw_bytes[: int(len(raw_bytes) * 0.6)]
        full_path.write_bytes(truncated)
        problems.append(full_path)

        corrections.append(
            self._write_parquet(
                base, challenge_dir / "corrected" / "complete_parquet.parquet"
            )
        )

        # Variant B: CSV file cut mid-row
        csv_path = challenge_dir / "problem" / "truncated_csv.csv"
        self._write_csv(base, csv_path)
        csv_bytes = csv_path.read_bytes()
        # Cut in the middle of the last 30% of rows
        cut_point = int(len(csv_bytes) * 0.7)
        # Find the nearest non-newline character to make it a true mid-row cut
        csv_path.write_bytes(csv_bytes[:cut_point])
        problems.append(csv_path)

        corrections.append(
            self._write_csv(base, challenge_dir / "corrected" / "complete_csv.csv")
        )

        # Variant C: Only header row, no data rows
        header_only = base.head(0)
        problems.append(
            self._write_csv(header_only, challenge_dir / "problem" / "header_only.csv")
        )
        corrections.append(
            self._write_csv(base, challenge_dir / "corrected" / "header_with_data.csv")
        )

        return {
            "challenge": "partial_files",
            "description": (
                "Incomplete files: truncated Parquet (corrupt footer), "
                "CSV cut mid-row, header-only files"
            ),
            "root_cause": (
                "File uploaded to S3/GCS/Azure while still being written; "
                "network timeout during PUT; producer OOM-killed mid-write"
            ),
            "snowpipe_symptom": (
                "row_parsed < row_count in COPY_HISTORY; "
                "'Invalid Parquet file' errors; zero rows loaded from header-only"
            ),
            "correction_strategy": (
                "1. Implement write-to-temp-then-rename pattern in producers\n"
                "2. Add file size / checksum validation in notification handler\n"
                "3. Use _SUCCESS marker files to signal complete batches\n"
                "4. Re-trigger source to regenerate the complete file"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 4: Duplicate File Reprocessing ──────────────────────────

    def challenge_duplicate_files(self, out_dir: Path) -> dict:
        """
        Simulate duplicate files that cause double-loading or are
        silently skipped by Snowpipe's COPY metadata.
        """
        challenge_dir = out_dir / "04_duplicate_files"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: Exact same filename re-uploaded (Snowpipe skips it)
        problems.append(
            self._write_parquet(
                base, challenge_dir / "problem" / "trades_batch_001.parquet"
            )
        )
        # Second upload with DIFFERENT data but SAME filename
        updated = base.copy()
        updated["trade_status"] = "AMENDED"
        updated["total_value_usd"] = updated["total_value_usd"] * 1.05
        problems.append(
            self._write_parquet(
                updated, challenge_dir / "problem" / "trades_batch_001_v2.parquet"
            )
        )
        # Correction: Use unique filenames with timestamp
        corrections.append(
            self._write_parquet(
                updated,
                challenge_dir
                / "corrected"
                / "trades_batch_001_20250615_120500.parquet",
            )
        )

        # Variant B: Same data, different filenames (causes duplicates in table)
        problems.append(
            self._write_parquet(
                base, challenge_dir / "problem" / "trades_source_a.parquet"
            )
        )
        problems.append(
            self._write_parquet(
                base, challenge_dir / "problem" / "trades_source_a_retry.parquet"
            )
        )
        # Correction: Deduplicated dataset
        corrections.append(
            self._write_parquet(
                base, challenge_dir / "corrected" / "trades_deduplicated.parquet"
            )
        )

        return {
            "challenge": "duplicate_files",
            "description": (
                "Duplicate file scenarios: same filename re-uploaded (skipped by "
                "Snowpipe metadata), and same data under different filenames "
                "(causes duplicate rows)"
            ),
            "root_cause": (
                "Producer retry logic re-uploads files; CDC replay after "
                "recovery; multiple sources feeding same stage path"
            ),
            "snowpipe_symptom": (
                "Same filename: silently skipped (data update lost). "
                "Different filename: duplicate rows in target table"
            ),
            "correction_strategy": (
                "1. Always include timestamp/UUID in filenames\n"
                "2. Implement MERGE-based loading instead of INSERT\n"
                "3. Add dedup logic: QUALIFY ROW_NUMBER() OVER "
                "(PARTITION BY trade_id ORDER BY _loaded_at DESC) = 1\n"
                "4. Use METADATA$FILENAME in COPY to track file provenance"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 5: Zero-Byte / Empty Files ─────────────────────────────

    def challenge_empty_files(self, out_dir: Path) -> dict:
        """Simulate zero-byte and logically empty files."""
        challenge_dir = out_dir / "05_empty_files"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # Variant A: Zero-byte file
        zero_path = challenge_dir / "problem" / "zero_byte.parquet"
        zero_path.parent.mkdir(parents=True, exist_ok=True)
        zero_path.write_bytes(b"")
        problems.append(zero_path)

        # Variant B: Parquet with schema but zero rows
        empty_df = self._generate_base_trades(0)
        problems.append(
            self._write_parquet(
                empty_df, challenge_dir / "problem" / "empty_parquet.parquet"
            )
        )

        # Variant C: CSV with only whitespace
        ws_path = challenge_dir / "problem" / "whitespace_only.csv"
        ws_path.parent.mkdir(parents=True, exist_ok=True)
        ws_path.write_text("\n\n   \n\n")
        problems.append(ws_path)

        # Corrections: Proper files with data
        base = self._generate_base_trades(n)
        corrections.append(
            self._write_parquet(
                base, challenge_dir / "corrected" / "proper_data.parquet"
            )
        )

        return {
            "challenge": "empty_files",
            "description": (
                "Zero-byte files, schema-only Parquet (0 rows), "
                "whitespace-only CSV files"
            ),
            "root_cause": (
                "Producer runs but has no data to emit (off-hours, holidays); "
                "file creation race condition; S3 eventual consistency"
            ),
            "snowpipe_symptom": (
                "COPY returns 0 rows loaded; notification consumed but "
                "no data ingested; monitoring alerts on zero-row loads"
            ),
            "correction_strategy": (
                "1. Producer should NOT emit files when no data is available\n"
                "2. Add pre-load file size check (reject < 100 bytes)\n"
                "3. Monitor COPY_HISTORY for zero row_count loads\n"
                "4. Alert on consecutive zero-row batches"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 6: Oversized Single Files ──────────────────────────────

    def challenge_oversized_files(self, out_dir: Path) -> dict:
        """
        Simulate files that are too large for efficient Snowpipe processing.

        Snowflake recommends 100-250 MB compressed files for optimal COPY.
        Files > 1GB cause warehouse timeouts and block other files.
        """
        challenge_dir = out_dir / "06_oversized_files"
        # Generate a larger dataset to simulate the issue
        n = self.config.records_per_scenario * 10  # 10x normal
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Single monolithic file
        problems.append(
            self._write_parquet(
                base, challenge_dir / "problem" / "monolithic_batch.parquet"
            )
        )

        # Correction: Split into optimally-sized chunks
        chunk_size = self.config.records_per_scenario
        for i in range(0, n, chunk_size):
            chunk = base.iloc[i : i + chunk_size]
            corrections.append(
                self._write_parquet(
                    chunk,
                    challenge_dir
                    / "corrected"
                    / f"split_batch_{i // chunk_size:04d}.parquet",
                )
            )

        return {
            "challenge": "oversized_files",
            "description": (
                "Single file contains 10x normal volume, blocking the pipe "
                "and causing warehouse timeout on COPY INTO"
            ),
            "root_cause": (
                "Upstream batch job runs monthly instead of daily; "
                "backfill job dumps entire history into one file; "
                "file splitting logic disabled after deploy"
            ),
            "snowpipe_symptom": (
                "Warehouse timeout; COPY runs for hours blocking other loads; "
                "serverless compute costs spike; SLA breach on downstream tables"
            ),
            "correction_strategy": (
                "1. Enforce max file size (250 MB compressed) at producer\n"
                "2. Use S3 event filter to quarantine oversized files\n"
                "3. Split-and-reload: chunk file into 100-250 MB parts\n"
                "4. Monitor COPY duration and alert on > 5 min single file"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 7: Character Encoding Corruption ───────────────────────

    def challenge_encoding_corruption(self, out_dir: Path) -> dict:
        """
        Simulate character encoding issues that cause parse failures
        or mojibake in loaded data.
        """
        challenge_dir = out_dir / "07_encoding_corruption"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: Latin-1 encoded file declared as UTF-8
        corrupted = base.copy()
        latin1_names = [
            "Société Générale Trading",
            "Ørsted Energy A/S",
            "Ångström Metals GmbH",
            "São Paulo Commodities Ltda",
            "François Müller & Cie",
            "Düsseldorf Handelsbank AG",
            "Malmö Shipping AB",
            "Zürich Commodity House",
            "Kraków Steel Works",
            "České Energetické Závody",
        ]
        corrupted["commodity_name"] = [
            latin1_names[i % len(latin1_names)] for i in range(n)
        ]
        # Write as Latin-1 with .csv extension
        csv_path = challenge_dir / "problem" / "latin1_as_utf8.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        corrupted.to_csv(csv_path, index=False, encoding="latin-1")
        problems.append(csv_path)

        # Correction: Properly encoded as UTF-8
        corrections.append(
            self._write_csv(
                corrupted,
                challenge_dir / "corrected" / "proper_utf8.csv",
                encoding="utf-8",
            )
        )

        # Variant B: BOM (Byte Order Mark) prefix
        bom_path = challenge_dir / "problem" / "with_bom.csv"
        bom_path.parent.mkdir(parents=True, exist_ok=True)
        csv_content = corrupted.to_csv(index=False)
        bom_path.write_bytes(b"\xef\xbb\xbf" + csv_content.encode("utf-8"))
        problems.append(bom_path)

        # Correction: BOM stripped
        corrections.append(
            self._write_csv(corrupted, challenge_dir / "corrected" / "no_bom.csv")
        )

        # Variant C: Null bytes embedded in strings
        null_byte_df = base.copy()
        null_byte_df["commodity_name"] = null_byte_df["commodity_name"].apply(
            lambda x: x[:3] + "\x00" + x[3:] if isinstance(x, str) else x
        )
        problems.append(
            self._write_csv(
                null_byte_df,
                challenge_dir / "problem" / "null_bytes.csv",
            )
        )
        corrections.append(
            self._write_csv(
                base, challenge_dir / "corrected" / "null_bytes_cleaned.csv"
            )
        )

        return {
            "challenge": "encoding_corruption",
            "description": (
                "Latin-1 data declared as UTF-8, BOM-prefixed files, "
                "null bytes embedded in string columns"
            ),
            "root_cause": (
                "Source system encoding mismatch; Windows-generated CSVs "
                "with BOM; binary data leaking into text fields; "
                "legacy mainframe EBCDIC conversion failures"
            ),
            "snowpipe_symptom": (
                "Parse errors on special characters; mojibake in loaded data; "
                "null byte causes VARCHAR truncation; first column name "
                "prefixed with BOM characters"
            ),
            "correction_strategy": (
                "1. FILE_FORMAT with ENCODING = 'UTF8' (explicit)\n"
                "2. Pre-process: strip BOM, convert encoding, remove null bytes\n"
                "3. Use REPLACE_INVALID_CHARACTERS = TRUE in COPY\n"
                "4. Validate encoding in producer before upload"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 8: Notification Queue Failures ─────────────────────────

    def challenge_notification_failures(self, out_dir: Path) -> dict:
        """
        Simulate files that land in stage but whose SQS/EventGrid/Pub-Sub
        notifications are lost, delayed, or duplicated.
        """
        challenge_dir = out_dir / "08_notification_failures"
        n = self.config.records_per_scenario

        problems = []
        corrections = []
        metadata = []

        # Simulate 10 batches: some notified, some missed, some duplicated
        for batch_id in range(10):
            batch_df = self._generate_base_trades(n // 10)
            batch_df["trade_id"] = [
                f"TRD-B{batch_id}-{i}" for i in range(len(batch_df))
            ]
            batch_path = challenge_dir / "problem" / f"batch_{batch_id:03d}.parquet"
            problems.append(self._write_parquet(batch_df, batch_path))

            # Simulate notification status
            if batch_id in [2, 5, 8]:
                status = "NOTIFICATION_LOST"
            elif batch_id in [3, 7]:
                status = "NOTIFICATION_DUPLICATED"
            else:
                status = "NOTIFICATION_OK"
            metadata.append(
                {
                    "batch_id": batch_id,
                    "file": str(batch_path),
                    "notification_status": status,
                    "records": len(batch_df),
                }
            )

        # Correction: Manual ALTER PIPE REFRESH to pick up missed files
        # (the corrected files are the same — the fix is operational)
        for batch_id in [2, 5, 8]:
            batch_df = self._generate_base_trades(n // 10)
            batch_df["trade_id"] = [
                f"TRD-B{batch_id}-{i}" for i in range(len(batch_df))
            ]
            corrections.append(
                self._write_parquet(
                    batch_df,
                    challenge_dir
                    / "corrected"
                    / f"missed_batch_{batch_id:03d}.parquet",
                )
            )

        # Write metadata manifest
        meta_path = challenge_dir / "notification_status.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return {
            "challenge": "notification_failures",
            "description": (
                "SQS/EventGrid notifications lost (3 of 10 batches), "
                "duplicated (2 of 10), causing missed and double loads"
            ),
            "root_cause": (
                "SQS message visibility timeout too short; "
                "EventGrid retry policy delivers duplicates; "
                "notification integration IAM policy changed; "
                "queue dead-letter threshold too low"
            ),
            "snowpipe_symptom": (
                "Files sit in stage unprocessed for hours/days; "
                "monitoring shows gap in load times; "
                "duplicate notifications cause double COPY attempts"
            ),
            "correction_strategy": (
                "1. ALTER PIPE <pipe> REFRESH — scan stage for missed files\n"
                "2. Scheduled reconciliation: compare stage listing vs "
                "COPY_HISTORY\n"
                "3. Increase SQS visibility timeout to 5× avg COPY duration\n"
                "4. Enable SQS dead-letter queue monitoring\n"
                "5. Idempotent COPY with dedup downstream"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
            "notification_metadata": str(meta_path),
        }

    # ─── Challenge 9: Pipe Stale After DDL ────────────────────────────────

    def challenge_pipe_stale(self, out_dir: Path) -> dict:
        """
        Simulate the scenario where ALTER TABLE on the target causes the
        pipe to go STALE, silently stopping ingestion.
        """
        challenge_dir = out_dir / "09_pipe_stale"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # Pre-DDL data (loads fine)
        pre_ddl = self._generate_base_trades(n)
        problems.append(
            self._write_parquet(
                pre_ddl, challenge_dir / "problem" / "pre_ddl_batch.parquet"
            )
        )

        # Post-DDL data (pipe is stale, these files pile up in stage)
        for i in range(3):
            batch = self._generate_base_trades(n // 3)
            batch["trade_id"] = [f"TRD-POST-{i}-{j}" for j in range(len(batch))]
            problems.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "problem" / f"post_ddl_batch_{i:02d}.parquet",
                )
            )

        # Correction: All post-DDL batches re-loaded after pipe recreation
        for i in range(3):
            batch = self._generate_base_trades(n // 3)
            batch["trade_id"] = [f"TRD-POST-{i}-{j}" for j in range(len(batch))]
            corrections.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "corrected" / f"reloaded_batch_{i:02d}.parquet",
                )
            )

        return {
            "challenge": "pipe_stale",
            "description": (
                "ALTER TABLE on RAW_TRADES (add column) causes PIPE_TRADES "
                "to go STALE; 3 subsequent batches pile up unloaded"
            ),
            "root_cause": (
                "DBA runs ALTER TABLE ADD COLUMN without recreating pipe; "
                "dbt run modifies table structure; migration script "
                "doesn't include pipe refresh step"
            ),
            "snowpipe_symptom": (
                "SYSTEM$PIPE_STATUS returns 'STALE_PIPE'; "
                "files accumulate in stage; no errors in pipe history "
                "(pipe simply stops processing)"
            ),
            "correction_strategy": (
                "1. CREATE OR REPLACE PIPE (recreate with same definition)\n"
                "2. ALTER PIPE REFRESH to reprocess accumulated files\n"
                "3. Add pipe status check to DDL migration runbooks\n"
                "4. Monitor: alert when SYSTEM$PIPE_STATUS != 'RUNNING'\n"
                "5. Automate: post-DDL hook that refreshes dependent pipes"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 10: ON_ERROR Silent Data Loss ──────────────────────────

    def challenge_on_error_data_loss(self, out_dir: Path) -> dict:
        """
        Simulate mixed good/bad records where ON_ERROR = SKIP_FILE
        causes entire files of mostly-good data to be silently dropped.
        """
        challenge_dir = out_dir / "10_on_error_data_loss"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Create a file where 2% of rows have a bad value,
        # but ON_ERROR=SKIP_FILE drops ALL rows
        mixed = base.copy()
        bad_indices = self.rng.choice(n, size=max(1, int(n * 0.02)), replace=False)
        for idx in bad_indices:
            # Inject a non-numeric value in total_value_usd
            mixed.at[idx, "total_value_usd"] = "NOT_A_NUMBER"
            # Inject an invalid date
            mixed.at[idx, "trade_date"] = "2025-13-45"

        problems.append(
            self._write_csv(mixed, challenge_dir / "problem" / "mixed_good_bad.csv")
        )

        # Correction A: Clean file (bad rows removed)
        clean = base.drop(index=bad_indices).reset_index(drop=True)
        corrections.append(
            self._write_csv(clean, challenge_dir / "corrected" / "bad_rows_removed.csv")
        )

        # Correction B: Bad rows quarantined separately
        quarantined = base.iloc[bad_indices].reset_index(drop=True)
        corrections.append(
            self._write_csv(
                quarantined,
                challenge_dir / "corrected" / "quarantined_rows.csv",
            )
        )

        # Variant: Multiple errors across different columns
        multi_err = base.copy()
        for idx in range(0, n, 50):  # Every 50th row
            col = random.choice(
                ["total_value_usd", "sanctions_risk_score", "trade_date"]
            )
            if col in ("total_value_usd", "sanctions_risk_score"):
                multi_err.at[idx, col] = "ERR"
            else:
                multi_err.at[idx, col] = "INVALID"
        problems.append(
            self._write_csv(
                multi_err,
                challenge_dir / "problem" / "scattered_errors.csv",
            )
        )
        # Correction: Fixed with COALESCE / TRY_CAST
        corrections.append(
            self._write_csv(
                base, challenge_dir / "corrected" / "scattered_errors_fixed.csv"
            )
        )

        return {
            "challenge": "on_error_data_loss",
            "description": (
                "ON_ERROR = SKIP_FILE drops entire files containing 98% good "
                "data because 2% of rows have parse errors"
            ),
            "root_cause": (
                "SKIP_FILE is the default for most pipes; when even one row "
                "is malformed, the entire file is skipped with only a log entry"
            ),
            "snowpipe_symptom": (
                "COPY_HISTORY shows status=SKIP_FILE with first_error_message; "
                "row_count shows expected count but row_parsed=0; "
                "downstream tables show data gaps"
            ),
            "correction_strategy": (
                "1. Use ON_ERROR = CONTINUE to load good rows\n"
                "2. Monitor error_count in COPY_HISTORY\n"
                "3. Route rejected rows to an error table via "
                "VALIDATION_MODE\n"
                "4. Pre-validate: TRY_CAST numeric/date columns in a "
                "staging COPY\n"
                "5. Implement dead-letter pattern: good rows → prod, "
                "bad rows → quarantine"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 11: File Ordering Dependencies ─────────────────────────

    def challenge_file_ordering(self, out_dir: Path) -> dict:
        """
        Simulate fact files arriving before their dimension dependencies.
        """
        challenge_dir = out_dir / "11_file_ordering"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # Trade file references counterparties that haven't been loaded yet
        trades = self._generate_base_trades(n)
        # Use counterparty IDs that won't exist yet
        trades["buyer_counterparty_id"] = [f"CP-FUTURE-{i}" for i in range(n)]
        trades["seller_counterparty_id"] = [f"CP-FUTURE-{i + 10000}" for i in range(n)]
        problems.append(
            self._write_parquet(
                trades,
                challenge_dir / "problem" / "trades_before_counterparties.parquet",
            )
        )

        # Trade file references vessels that haven't been loaded
        trades_v = self._generate_base_trades(n)
        trades_v["vessel_id"] = [f"VSL-FUTURE-{i}" for i in range(n)]
        problems.append(
            self._write_parquet(
                trades_v,
                challenge_dir / "problem" / "trades_before_vessels.parquet",
            )
        )

        # Correction: Dimension files that should load first
        # Counterparties
        cp_records = []
        for i in range(n):
            cp_records.append(
                {
                    "counterparty_id": f"CP-FUTURE-{i}",
                    "legal_name": f"Future Corp {i}",
                    "entity_type": "CORPORATE",
                    "country_of_incorporation": "US",
                    "risk_rating": "MEDIUM",
                    "source_system": "KYC_SYSTEM",
                }
            )
        for i in range(n):
            cp_records.append(
                {
                    "counterparty_id": f"CP-FUTURE-{i + 10000}",
                    "legal_name": f"Future Seller Corp {i}",
                    "entity_type": "CORPORATE",
                    "country_of_incorporation": "GB",
                    "risk_rating": "LOW",
                    "source_system": "KYC_SYSTEM",
                }
            )
        corrections.append(
            self._write_parquet(
                pd.DataFrame(cp_records),
                challenge_dir / "corrected" / "counterparties_first.parquet",
            )
        )

        # Vessels
        vsl_records = []
        for i in range(n):
            vsl_records.append(
                {
                    "vessel_id": f"VSL-FUTURE-{i}",
                    "imo_number": f"{9200000 + i}",
                    "vessel_name": f"M/V FUTURE {i}",
                    "vessel_type": "VLCC",
                    "flag_state": "PA",
                    "source_system": "VESSEL_REGISTRY",
                }
            )
        corrections.append(
            self._write_parquet(
                pd.DataFrame(vsl_records),
                challenge_dir / "corrected" / "vessels_first.parquet",
            )
        )

        # Then the trades (same data, now FK-valid)
        corrections.append(
            self._write_parquet(
                trades,
                challenge_dir / "corrected" / "trades_after_dimensions.parquet",
            )
        )

        return {
            "challenge": "file_ordering",
            "description": (
                "Fact files (trades) arrive and load before dimension files "
                "(counterparties, vessels), causing FK violations and "
                "broken JOIN results downstream"
            ),
            "root_cause": (
                "Multiple independent Snowpipes with no ordering guarantee; "
                "dimension pipe slow/stale while fact pipe is current; "
                "backfill loads facts before dimensions"
            ),
            "snowpipe_symptom": (
                "No Snowpipe error (pipes are independent); downstream dbt "
                "models show NULL JOINs; referential integrity tests fail; "
                "reporting dashboards show 'Unknown' counterparties"
            ),
            "correction_strategy": (
                "1. Load dimensions first via explicit COPY before enabling "
                "fact pipes\n"
                "2. Use dbt post-load deferred JOIN resolution\n"
                "3. Implement a staging→production promotion pattern\n"
                "4. Add data quality tests: assert all FK values exist in "
                "dimension tables\n"
                "5. Use dynamic tables to auto-resolve as dimensions arrive"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 12: Concurrent Writer Conflicts ────────────────────────

    def challenge_concurrent_writers(self, out_dir: Path) -> dict:
        """
        Simulate multiple producers writing to the same stage path,
        causing interleaved or corrupt data.
        """
        challenge_dir = out_dir / "12_concurrent_writers"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # Two writers produce files with overlapping trade IDs
        writer_a = self._generate_base_trades(n)
        writer_a["source_system"] = "ETRM_LONDON"
        writer_a["trade_id"] = [f"TRD-{100000 + i}" for i in range(n)]

        writer_b = self._generate_base_trades(n)
        writer_b["source_system"] = "ETRM_SINGAPORE"
        # Overlapping IDs with different data
        writer_b["trade_id"] = [f"TRD-{100000 + i}" for i in range(n)]
        writer_b["total_value_usd"] = writer_b["total_value_usd"] * 1.1

        problems.append(
            self._write_parquet(
                writer_a, challenge_dir / "problem" / "writer_london.parquet"
            )
        )
        problems.append(
            self._write_parquet(
                writer_b, challenge_dir / "problem" / "writer_singapore.parquet"
            )
        )

        # Correction: Merge with source_system precedence
        merged = pd.concat([writer_a, writer_b])
        # Keep Singapore as the latest (simulating last-write-wins)
        merged = merged.drop_duplicates(subset=["trade_id"], keep="last")
        corrections.append(
            self._write_parquet(
                merged, challenge_dir / "corrected" / "merged_deduplicated.parquet"
            )
        )

        return {
            "challenge": "concurrent_writers",
            "description": (
                "Two regional ETRM systems (London, Singapore) write to the "
                "same stage path with overlapping trade IDs but different values"
            ),
            "root_cause": (
                "Multiple source systems feeding one pipe; no namespace "
                "separation in stage paths; global trade book replicated "
                "across regions without conflict resolution"
            ),
            "snowpipe_symptom": (
                "Duplicate trade_ids in target table with different values; "
                "non-deterministic query results depending on load order; "
                "reconciliation breaks between regions"
            ),
            "correction_strategy": (
                "1. Namespace stage paths by source: @stage/london/, "
                "@stage/singapore/\n"
                "2. Separate pipes per source system\n"
                "3. MERGE-based load with source_system + trade_id as key\n"
                "4. Last-write-wins with _loaded_at tiebreaker\n"
                "5. Add METADATA$FILENAME to track provenance"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 13: Load Latency / SLA Breaches ────────────────────────

    def challenge_load_latency(self, out_dir: Path) -> dict:
        """
        Simulate batches that arrive with varying latency, some breaching
        the ingestion SLA.
        """
        challenge_dir = out_dir / "13_load_latency"
        n = self.config.records_per_scenario

        problems = []
        corrections = []
        latency_report = []

        base_time = datetime(2025, 6, 15, 0, 0, 0)
        for hour in range(24):
            batch = self._generate_base_trades(n // 24 or 10)
            event_time = base_time + timedelta(hours=hour)
            batch["trade_timestamp"] = event_time.isoformat()

            # Simulate variable latency
            if hour in [3, 4, 14, 15]:
                # SLA breach: 4+ hour delay
                latency_minutes = random.randint(240, 480)
            elif hour in [8, 9, 10]:
                # Peak hours: moderate delay
                latency_minutes = random.randint(30, 90)
            else:
                # Normal: < 15 min
                latency_minutes = random.randint(1, 15)

            load_time = event_time + timedelta(minutes=latency_minutes)
            batch["_loaded_at"] = load_time.isoformat()

            problems.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "problem" / f"hour_{hour:02d}.parquet",
                )
            )

            sla_breach = latency_minutes > 60
            latency_report.append(
                {
                    "hour": hour,
                    "event_time": event_time.isoformat(),
                    "load_time": load_time.isoformat(),
                    "latency_minutes": latency_minutes,
                    "sla_breach": sla_breach,
                    "sla_threshold_minutes": 60,
                }
            )

        # Correction: Re-prioritized loads with SLA-breach batches first
        for entry in latency_report:
            if entry["sla_breach"]:
                hour = entry["hour"]
                batch = self._generate_base_trades(n // 24 or 10)
                batch["trade_timestamp"] = entry["event_time"]
                batch["_loaded_at"] = (
                    datetime.fromisoformat(entry["event_time"]) + timedelta(minutes=10)
                ).isoformat()
                corrections.append(
                    self._write_parquet(
                        batch,
                        challenge_dir
                        / "corrected"
                        / f"priority_reload_hour_{hour:02d}.parquet",
                    )
                )

        # Write latency report
        report_path = challenge_dir / "latency_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(latency_report, f, indent=2)

        return {
            "challenge": "load_latency",
            "description": (
                "Variable ingestion latency across 24 hours: 4 batches "
                "breach 60-minute SLA with 4-8 hour delays"
            ),
            "root_cause": (
                "Snowpipe serverless compute throttled during peak; "
                "warehouse auto-suspend delays; large files ahead in queue; "
                "notification backlog during burst periods"
            ),
            "snowpipe_symptom": (
                "Gap between file stage time and first_load_time in "
                "COPY_HISTORY exceeds SLA; downstream models show stale data; "
                "real-time dashboards lag by hours"
            ),
            "correction_strategy": (
                "1. Monitor: DATEDIFF between stage_time and load_time\n"
                "2. Dedicated warehouse for critical pipes (not serverless)\n"
                "3. Priority lanes: separate high-priority data into its "
                "own pipe + warehouse\n"
                "4. Auto-scale warehouse on queue depth\n"
                "5. Alert when latency > SLA threshold"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
            "latency_report": str(report_path),
        }

    # ─── Challenge 14: Numeric Precision Overflow ─────────────────────────

    def challenge_numeric_overflow(self, out_dir: Path) -> dict:
        """
        Simulate numeric values that overflow Snowflake column precision.
        """
        challenge_dir = out_dir / "14_numeric_overflow"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: total_value_usd exceeds NUMBER(18,2) — max ~10^16
        overflow = base.copy()
        overflow_indices = self.rng.choice(n, size=max(1, n // 20), replace=False)
        for idx in overflow_indices:
            overflow.at[idx, "total_value_usd"] = 99999999999999999.99
        problems.append(
            self._write_parquet(
                overflow, challenge_dir / "problem" / "numeric_overflow.parquet"
            )
        )

        # Correction: Cap at column maximum
        fixed = overflow.copy()
        fixed["total_value_usd"] = fixed["total_value_usd"].clip(
            upper=9999999999999999.99
        )
        corrections.append(
            self._write_parquet(
                fixed, challenge_dir / "corrected" / "numeric_capped.parquet"
            )
        )

        # Variant B: Extreme decimal precision (30+ decimal places)
        precision = base.copy()
        precision["sanctions_risk_score"] = [
            3.141592653589793238462643383279 for _ in range(n)
        ]
        problems.append(
            self._write_parquet(
                precision, challenge_dir / "problem" / "excess_precision.parquet"
            )
        )
        # Correction: Round to column precision
        fixed_p = precision.copy()
        fixed_p["sanctions_risk_score"] = fixed_p["sanctions_risk_score"].round(2)
        corrections.append(
            self._write_parquet(
                fixed_p, challenge_dir / "corrected" / "precision_rounded.parquet"
            )
        )

        # Variant C: NaN and Infinity values
        nan_inf = base.copy()
        nan_inf.at[0, "total_value_usd"] = float("nan")
        nan_inf.at[1, "total_value_usd"] = float("inf")
        nan_inf.at[2, "total_value_usd"] = float("-inf")
        problems.append(
            self._write_parquet(
                nan_inf, challenge_dir / "problem" / "nan_infinity.parquet"
            )
        )
        fixed_ni = base.copy()
        fixed_ni.at[0, "total_value_usd"] = 0.0
        fixed_ni.at[1, "total_value_usd"] = 9999999999999999.99
        fixed_ni.at[2, "total_value_usd"] = -9999999999999999.99
        corrections.append(
            self._write_parquet(
                fixed_ni, challenge_dir / "corrected" / "nan_infinity_fixed.parquet"
            )
        )

        return {
            "challenge": "numeric_overflow",
            "description": (
                "Numeric values exceed column precision: NUMBER(18,2) overflow, "
                "30+ decimal places in NUMBER(5,2), NaN/Infinity in float columns"
            ),
            "root_cause": (
                "Source system uses unbounded decimals; currency conversion "
                "produces extreme precision; division by zero yields Infinity; "
                "missing value encoded as NaN instead of NULL"
            ),
            "snowpipe_symptom": (
                "COPY error: 'Numeric value out of range'; implicit truncation "
                "of decimal places; NaN loads as NULL (silent data loss); "
                "Infinity causes parse error"
            ),
            "correction_strategy": (
                "1. TRY_CAST in COPY transformation to catch overflow\n"
                "2. Pre-validate: reject values outside column range\n"
                "3. REPLACE NaN/Inf: CASE WHEN value = 'NaN' THEN NULL\n"
                "4. Use NUMBER(38,6) for high-precision financial data\n"
                "5. Round at source to target precision"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 15: Timestamp Format Inconsistency ─────────────────────

    def challenge_timestamp_formats(self, out_dir: Path) -> dict:
        """
        Simulate different timestamp formats from different source systems.
        """
        challenge_dir = out_dir / "15_timestamp_formats"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: UNIX epoch (seconds)
        epoch_df = base.copy()
        epoch_df["trade_timestamp"] = [
            int((datetime(2025, 6, 15) + timedelta(seconds=i * 5)).timestamp())
            for i in range(n)
        ]
        problems.append(
            self._write_csv(
                epoch_df, challenge_dir / "problem" / "unix_epoch_seconds.csv"
            )
        )

        # Variant B: UNIX epoch (milliseconds)
        epoch_ms_df = base.copy()
        epoch_ms_df["trade_timestamp"] = [
            int((datetime(2025, 6, 15) + timedelta(seconds=i * 5)).timestamp() * 1000)
            for i in range(n)
        ]
        problems.append(
            self._write_csv(
                epoch_ms_df,
                challenge_dir / "problem" / "unix_epoch_millis.csv",
            )
        )

        # Variant C: Mixed formats in same file
        mixed_df = base.copy()
        formats = [
            "2025-06-15T08:00:00",
            "2025/06/15 08:00:00",
            "15-Jun-2025 08:00:00",
            "06/15/2025 08:00:00",
            "Jun 15, 2025 8:00 AM",
        ]
        mixed_df["trade_timestamp"] = [formats[i % len(formats)] for i in range(n)]
        problems.append(
            self._write_csv(mixed_df, challenge_dir / "problem" / "mixed_formats.csv")
        )

        # Variant D: Timezone-aware vs naive
        tz_df = base.copy()
        tz_formats = [
            "2025-06-15T08:00:00Z",
            "2025-06-15T08:00:00+00:00",
            "2025-06-15T03:00:00-05:00",
            "2025-06-15T16:00:00+08:00",
            "2025-06-15T08:00:00",  # Naive — which TZ?
        ]
        tz_df["trade_timestamp"] = [tz_formats[i % len(tz_formats)] for i in range(n)]
        problems.append(
            self._write_csv(tz_df, challenge_dir / "problem" / "timezone_variants.csv")
        )

        # Correction: All normalised to ISO 8601 UTC
        corrected_df = base.copy()
        corrected_df["trade_timestamp"] = [
            (datetime(2025, 6, 15, 8, 0, 0) + timedelta(seconds=i * 5)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            for i in range(n)
        ]
        for name in [
            "unix_normalised.csv",
            "mixed_normalised.csv",
            "tz_normalised.csv",
        ]:
            corrections.append(
                self._write_csv(corrected_df, challenge_dir / "corrected" / name)
            )

        return {
            "challenge": "timestamp_formats",
            "description": (
                "Timestamp format inconsistencies: UNIX epoch (sec/ms), "
                "mixed date formats in same file, timezone-aware vs naive"
            ),
            "root_cause": (
                "Multiple source systems with different serialisation; "
                "API returns epoch, batch export returns ISO; regional "
                "offices use local date formats; TZ handling inconsistent"
            ),
            "snowpipe_symptom": (
                "COPY error: 'Timestamp does not match format'; "
                "epoch loads as integer (wrong column type); "
                "TZ-aware timestamps silently truncated to NTZ"
            ),
            "correction_strategy": (
                "1. Standardise at source: mandate ISO 8601 UTC\n"
                "2. COPY transformation: TO_TIMESTAMP_NTZ with explicit format\n"
                "3. Use VARIANT column + post-load parsing for mixed formats\n"
                "4. Convert epoch: TO_TIMESTAMP(col::NUMBER) in COPY SELECT\n"
                "5. Enforce TZ policy: all TIMESTAMP_NTZ assumed UTC"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 16: Stage Permission / IAM Failures ────────────────────

    def challenge_stage_permissions(self, out_dir: Path) -> dict:
        """
        Simulate files that are in stage but inaccessible due to
        IAM/permission changes.
        """
        challenge_dir = out_dir / "16_stage_permissions"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # Files that were staged successfully but became inaccessible
        for i in range(3):
            batch = self._generate_base_trades(n // 3)
            problems.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "problem" / f"inaccessible_batch_{i:02d}.parquet",
                )
            )

        # Correction: Same files re-staged after IAM fix
        for i in range(3):
            batch = self._generate_base_trades(n // 3)
            corrections.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "corrected" / f"restaged_batch_{i:02d}.parquet",
                )
            )

        return {
            "challenge": "stage_permissions",
            "description": (
                "Files in stage become inaccessible after IAM policy rotation, "
                "key expiry, or storage integration credential change"
            ),
            "root_cause": (
                "Cloud IAM key rotation without updating Snowflake storage "
                "integration; S3 bucket policy change; Azure SAS token expiry; "
                "cross-account trust policy removed"
            ),
            "snowpipe_symptom": (
                "COPY error: 'Access Denied' or 'AuthenticationFailed'; "
                "pipe shows pending files but cannot read them; "
                "all pipes using the same integration fail simultaneously"
            ),
            "correction_strategy": (
                "1. ALTER STORAGE INTEGRATION SET — update credentials\n"
                "2. DESCRIBE INTEGRATION to verify external_id/IAM_USER_ARN\n"
                "3. Re-grant USAGE on integration to pipe-owning role\n"
                "4. ALTER PIPE REFRESH after credential fix\n"
                "5. Automate: schedule credential rotation with integration "
                "update in same runbook"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 17: COPY History Metadata Expiry ───────────────────────

    def challenge_copy_history_expiry(self, out_dir: Path) -> dict:
        """
        Simulate the 14-day COPY metadata window causing duplicate loads.
        """
        challenge_dir = out_dir / "17_copy_history_expiry"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # Files originally loaded 15+ days ago (metadata expired)
        for day_offset in [15, 20, 25, 30]:
            batch = self._generate_base_trades(n // 4)
            load_date = datetime.now() - timedelta(days=day_offset)
            batch["_loaded_at"] = load_date.isoformat()
            batch["trade_id"] = [
                f"TRD-OLD-D{day_offset}-{i}" for i in range(len(batch))
            ]
            problems.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "problem" / f"old_file_day_{day_offset}.parquet",
                )
            )

        # These same files re-staged will be loaded AGAIN (metadata expired)
        # Correction: Deduplicate after reload
        all_batches = []
        for day_offset in [15, 20, 25, 30]:
            batch = self._generate_base_trades(n // 4)
            batch["trade_id"] = [
                f"TRD-OLD-D{day_offset}-{i}" for i in range(len(batch))
            ]
            all_batches.append(batch)

        deduped = pd.concat(all_batches).drop_duplicates(
            subset=["trade_id"], keep="first"
        )
        corrections.append(
            self._write_parquet(
                deduped,
                challenge_dir / "corrected" / "deduplicated_reload.parquet",
            )
        )

        return {
            "challenge": "copy_history_expiry",
            "description": (
                "Files older than 14 days are re-processed when re-staged "
                "because Snowpipe's COPY_HISTORY metadata has expired"
            ),
            "root_cause": (
                "Snowflake only tracks loaded files for 14 days; "
                "disaster recovery re-stages old files; bucket lifecycle "
                "rule moves files then moves them back; manual re-upload"
            ),
            "snowpipe_symptom": (
                "Duplicate rows appear for data originally loaded 15+ days "
                "ago; row counts suddenly double for old date partitions; "
                "no error — Snowpipe treats re-staged files as new"
            ),
            "correction_strategy": (
                "1. Never re-stage files to the same path within 14 days\n"
                "2. Maintain external load manifest (DynamoDB/Redis) for "
                "files > 14 days\n"
                "3. Downstream dedup: QUALIFY ROW_NUMBER() OVER "
                "(PARTITION BY PK ORDER BY _loaded_at DESC) = 1\n"
                "4. Use stage sub-paths with dates to avoid collisions\n"
                "5. Archive loaded files to a separate bucket"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 18: Retry Storm / Thundering Herd ──────────────────────

    def challenge_retry_storm(self, out_dir: Path) -> dict:
        """
        Simulate a burst of failed files all retried simultaneously,
        overwhelming the warehouse.
        """
        challenge_dir = out_dir / "18_retry_storm"
        n = self.config.records_per_scenario

        problems = []
        corrections = []

        # 20 files all retried at once after an outage recovery
        for i in range(20):
            batch = self._generate_base_trades(n)
            batch["trade_id"] = [f"TRD-RETRY-{i}-{j}" for j in range(n)]
            problems.append(
                self._write_parquet(
                    batch,
                    challenge_dir / "problem" / f"retry_batch_{i:03d}.parquet",
                )
            )

        # Correction: Staggered reload (5 files at a time)
        for wave in range(4):
            for i in range(5):
                batch_idx = wave * 5 + i
                batch = self._generate_base_trades(n)
                batch["trade_id"] = [f"TRD-RETRY-{batch_idx}-{j}" for j in range(n)]
                corrections.append(
                    self._write_parquet(
                        batch,
                        challenge_dir
                        / "corrected"
                        / f"wave_{wave:02d}_batch_{i:02d}.parquet",
                    )
                )

        return {
            "challenge": "retry_storm",
            "description": (
                "20 failed files retried simultaneously after outage recovery, "
                "overwhelming the warehouse and causing cascading timeouts"
            ),
            "root_cause": (
                "Outage causes SQS messages to pile up in dead-letter queue; "
                "all re-driven at once; auto-retry with no backoff; "
                "multiple pipes resume simultaneously after maintenance"
            ),
            "snowpipe_symptom": (
                "Warehouse credit spike; COPY timeouts; cascading failures "
                "across all pipes sharing the warehouse; query queuing; "
                "downstream task SLA breaches"
            ),
            "correction_strategy": (
                "1. Implement exponential backoff in retry logic\n"
                "2. Rate-limit re-drive from dead-letter queue (5 msg/sec)\n"
                "3. Separate warehouses for backfill vs real-time pipes\n"
                "4. Use COPY with MAX_FILE_SIZE and FORCE = TRUE for "
                "controlled reload\n"
                "5. Stagger pipe RESUME after maintenance window"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 19: Column Name Case Sensitivity ───────────────────────

    def challenge_column_case_sensitivity(self, out_dir: Path) -> dict:
        """
        Simulate column name case mismatches between source files
        and Snowflake table definitions.
        """
        challenge_dir = out_dir / "19_column_case"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: All uppercase column names
        upper = base.copy()
        upper.columns = [c.upper() for c in upper.columns]
        problems.append(
            self._write_parquet(
                upper, challenge_dir / "problem" / "uppercase_columns.parquet"
            )
        )

        # Variant B: CamelCase column names
        camel_map = {
            "trade_id": "TradeId",
            "trade_reference": "TradeReference",
            "trade_timestamp": "TradeTimestamp",
            "trade_date": "TradeDate",
            "trade_type": "TradeType",
            "trade_status": "TradeStatus",
            "buyer_counterparty_id": "BuyerCounterpartyId",
            "seller_counterparty_id": "SellerCounterpartyId",
            "commodity_code": "CommodityCode",
            "commodity_name": "CommodityName",
            "total_value_usd": "TotalValueUsd",
            "currency": "Currency",
            "origin_country": "OriginCountry",
            "destination_country": "DestinationCountry",
            "vessel_id": "VesselId",
            "sanctions_risk_score": "SanctionsRiskScore",
            "source_system": "SourceSystem",
            "_loaded_at": "LoadedAt",
        }
        camel = base.rename(columns=camel_map)
        problems.append(
            self._write_parquet(
                camel, challenge_dir / "problem" / "camelcase_columns.parquet"
            )
        )

        # Variant C: Mixed case within same file
        mixed = base.copy()
        cols = list(mixed.columns)
        mixed.columns = [
            c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(cols)
        ]
        problems.append(
            self._write_parquet(
                mixed, challenge_dir / "problem" / "mixed_case_columns.parquet"
            )
        )

        # Correction: All lowercase (matching Snowflake default)
        for name in [
            "uppercase_fixed.parquet",
            "camelcase_fixed.parquet",
            "mixed_case_fixed.parquet",
        ]:
            corrections.append(
                self._write_parquet(base, challenge_dir / "corrected" / name)
            )

        return {
            "challenge": "column_case_sensitivity",
            "description": (
                "Column name case mismatches: all-uppercase, CamelCase, "
                "and mixed case — when MATCH_BY_COLUMN_NAME expects "
                "case-insensitive or exact match"
            ),
            "root_cause": (
                "Different source systems use different naming conventions; "
                "Java/C# serialisation produces CamelCase; "
                "SQL exports produce UPPERCASE; Python produces snake_case"
            ),
            "snowpipe_symptom": (
                "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE works for most cases "
                "but CASE_SENSITIVE fails; without MATCH_BY_COLUMN_NAME, "
                "ordinal mapping loads wrong data into wrong columns"
            ),
            "correction_strategy": (
                "1. Always use MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
                "2. Standardise column names at source (prefer snake_case)\n"
                "3. Use COPY transformation to alias columns\n"
                "4. Pre-process files to normalise column names\n"
                "5. Document naming convention in data contracts"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Challenge 20: Multi-Byte Path / Filename ─────────────────────────

    def challenge_multibyte_paths(self, out_dir: Path) -> dict:
        """
        Simulate files with Unicode/special characters in paths or filenames
        that break stage listing or COPY commands.
        """
        challenge_dir = out_dir / "20_multibyte_paths"
        n = self.config.records_per_scenario
        base = self._generate_base_trades(n)

        problems = []
        corrections = []

        # Variant A: Spaces in filename
        problems.append(
            self._write_parquet(
                base,
                challenge_dir / "problem" / "trades batch 001.parquet",
            )
        )

        # Variant B: Special characters
        problems.append(
            self._write_parquet(
                base,
                challenge_dir / "problem" / "trades_[2025]_(final).parquet",
            )
        )

        # Variant C: Hash and ampersand
        problems.append(
            self._write_parquet(
                base,
                challenge_dir / "problem" / "trades#v2&latest.parquet",
            )
        )

        # Corrections: Clean filenames
        for i, name in enumerate(
            [
                "trades_batch_001.parquet",
                "trades_2025_final.parquet",
                "trades_v2_latest.parquet",
            ]
        ):
            corrections.append(
                self._write_parquet(base, challenge_dir / "corrected" / name)
            )

        return {
            "challenge": "multibyte_paths",
            "description": (
                "Filenames with spaces, brackets, hash signs, and ampersands "
                "that break stage listing or COPY path resolution"
            ),
            "root_cause": (
                "Manual file uploads with default naming; Windows-generated "
                "paths with spaces; copy-paste from Excel adds brackets; "
                "versioning adds special characters"
            ),
            "snowpipe_symptom": (
                "COPY error: 'File not found' or 'Invalid stage path'; "
                "LIST @stage shows file but COPY cannot resolve it; "
                "notification contains URL-encoded path that doesn't match"
            ),
            "correction_strategy": (
                "1. Enforce filename policy: [a-z0-9_-] only\n"
                "2. URL-encode paths in COPY commands\n"
                "3. Pre-process: rename files on upload via Lambda/Function\n"
                "4. Use stage sub-directories with date-based naming\n"
                "5. Reject files with special characters at upload time"
            ),
            "problem_files": [str(p) for p in problems],
            "correction_files": [str(c) for c in corrections],
        }

    # ─── Master orchestrator ──────────────────────────────────────────────

    def generate_all_challenges(self, output_dir: str | Path) -> dict:
        """
        Generate all enabled challenge scenarios and produce a master manifest.

        Returns:
            Master manifest dict with all challenge metadata.
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        challenges = []
        challenge_methods = [
            ("schema_evolution", self.challenge_schema_evolution),
            ("file_format_mismatch", self.challenge_file_format_mismatch),
            ("partial_files", self.challenge_partial_files),
            ("duplicate_files", self.challenge_duplicate_files),
            ("empty_files", self.challenge_empty_files),
            ("oversized_files", self.challenge_oversized_files),
            ("encoding_corruption", self.challenge_encoding_corruption),
            ("notification_failures", self.challenge_notification_failures),
            ("pipe_stale", self.challenge_pipe_stale),
            ("on_error_data_loss", self.challenge_on_error_data_loss),
            ("file_ordering", self.challenge_file_ordering),
            ("concurrent_writers", self.challenge_concurrent_writers),
            ("load_latency", self.challenge_load_latency),
            ("numeric_overflow", self.challenge_numeric_overflow),
            ("timestamp_formats", self.challenge_timestamp_formats),
            ("stage_permissions", self.challenge_stage_permissions),
            ("copy_history_expiry", self.challenge_copy_history_expiry),
            ("retry_storm", self.challenge_retry_storm),
            ("column_case_sensitivity", self.challenge_column_case_sensitivity),
            ("multibyte_paths", self.challenge_multibyte_paths),
        ]

        for name, method in challenge_methods:
            if getattr(self.config, name, True):
                logger.info(f"Generating challenge: {name}")
                try:
                    result = method(out)
                    challenges.append(result)
                except Exception as e:
                    logger.error(f"Failed to generate challenge {name}: {e}")
                    challenges.append(
                        {
                            "challenge": name,
                            "error": str(e),
                        }
                    )

        manifest = {
            "generated_at": datetime.now().isoformat(),
            "total_challenges": len(challenges),
            "challenges": challenges,
        }

        manifest_path = out / "snowpipe_challenge_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, default=str)

        logger.info(
            f"Generated {len(challenges)} Snowpipe challenge scenarios → "
            f"{manifest_path}"
        )
        return manifest
