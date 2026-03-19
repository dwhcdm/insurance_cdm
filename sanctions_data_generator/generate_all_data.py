"""
Master orchestrator for generating all synthetic data.

Supports DEV (millions), TEST (tens of millions), PROD (billions) scale.
Generates data in date-partitioned Parquet files optimized for
Snowflake COPY INTO ingestion.

Usage:
    python generate_all_data.py --env dev --output ./generated_data
    python generate_all_data.py --env test --output ./generated_data
    python generate_all_data.py --env prod --output ./generated_data --workers 8
"""

import argparse
import json
import sys
from datetime import date, timedelta, datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

from tqdm import tqdm

from config import volume_config, snowflake_config
from generators.counterparty_generator import CounterpartyGenerator
from generators.sanctions_list_generator import SanctionsListGenerator
from generators.trade_generator import TradeGenerator
from generators.vessel_generator import VesselGenerator
from generators.vessel_movement_generator import VesselMovementGenerator
from generators.screening_generator import ScreeningResultGenerator


class DataOrchestrator:
    """Orchestrates all data generation and loading."""

    def __init__(self, environment: str = "dev", output_dir: str = "./generated_data"):
        self.environment = environment
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Set volume multiplier based on environment
        if environment == "dev":
            self.multiplier = volume_config.DEV_MULTIPLIER     # 0.001
        elif environment == "test":
            self.multiplier = volume_config.TEST_MULTIPLIER    # 0.01
        else:
            self.multiplier = 1.0                               # Full production volume

        # Calculate actual volumes
        self.volumes = {
            "counterparties": max(int(volume_config.COUNTERPARTIES * self.multiplier), 1_000),
            "vessels": max(int(volume_config.VESSELS * self.multiplier), 500),
            "sanctioned_entities": max(int(volume_config.SANCTIONED_ENTITIES * self.multiplier), 500),
            "trades_per_day": max(int(volume_config.TRADES_PER_DAY * self.multiplier), 100),
            "trade_days": min(volume_config.TRADE_HISTORY_DAYS, 30 if environment == "dev" else 365),
            "vessel_movements_per_day": max(int(volume_config.VESSEL_MOVEMENTS_PER_DAY * self.multiplier), 1_000),
            "vessel_movement_days": min(volume_config.VESSEL_MOVEMENT_DAYS, 7 if environment == "dev" else 90),
        }

        self.volumes["total_trades"] = self.volumes["trades_per_day"] * self.volumes["trade_days"]
        self.volumes["total_vessel_movements"] = (
            self.volumes["vessel_movements_per_day"] * self.volumes["vessel_movement_days"]
        )
        self.volumes["total_screenings"] = int(
            self.volumes["total_trades"] * volume_config.SCREENING_RESULTS_RATIO
        )

        print(f"\n{'=' * 60}")
        print(f"DATA GENERATION PLAN - Environment: {environment.upper()}")
        print(f"{'=' * 60}")
        for k, v in self.volumes.items():
            print(f"  {k:35s}: {v:>15,}")
        print(f"{'=' * 60}\n")

    def generate_dimension_data(self) -> dict:
        """Generate all dimension/reference data first."""
        manifest = {}

        print("\n[1/6] Generating Counterparty Data...")
        cp_gen = CounterpartyGenerator(seed=42)
        cp_files = cp_gen.generate_to_parquet(
            output_dir=self.output_dir / "counterparties",
            total_records=self.volumes["counterparties"],
            batch_size=min(self.volumes["counterparties"], 500_000),
            file_prefix="counterparties",
        )
        manifest["counterparties"] = [str(f) for f in cp_files]

        print("\n[2/6] Generating Vessel Data...")
        vessel_gen = VesselGenerator(seed=43)
        vessel_files = vessel_gen.generate_to_parquet(
            output_dir=self.output_dir / "vessels",
            total_records=self.volumes["vessels"],
            batch_size=min(self.volumes["vessels"], 500_000),
            file_prefix="vessels",
        )
        manifest["vessels"] = [str(f) for f in vessel_files]

        print("\n[3/6] Generating Sanctions List Data...")
        sanctions_gen = SanctionsListGenerator(seed=44)
        sanctions_files = sanctions_gen.generate_to_parquet(
            output_dir=self.output_dir / "sanctions_lists",
            total_records=self.volumes["sanctioned_entities"],
            batch_size=min(self.volumes["sanctioned_entities"], 500_000),
            file_prefix="sanctions_lists",
        )
        manifest["sanctions_lists"] = [str(f) for f in sanctions_files]

        return manifest

    def generate_trade_data(self) -> list:
        """Generate trade transactions - potentially BILLIONS."""
        print(f"\n[4/6] Generating Trade Data ({self.volumes['total_trades']:,} records)...")

        trade_gen = TradeGenerator(seed=45)
        trade_dir = self.output_dir / "trades"
        trade_dir.mkdir(parents=True, exist_ok=True)

        all_files = []
        start_date = date.today() - timedelta(days=self.volumes["trade_days"])

        for day_offset in tqdm(range(self.volumes["trade_days"]), desc="Trade days"):
            current_date = start_date + timedelta(days=day_offset)
            date_dir = trade_dir / current_date.strftime("%Y/%m/%d")
            date_dir.mkdir(parents=True, exist_ok=True)

            day_files = trade_gen.generate_to_parquet(
                output_dir=date_dir,
                total_records=self.volumes["trades_per_day"],
                batch_size=min(self.volumes["trades_per_day"], 500_000),
                file_prefix="trades",
                trade_date=current_date,
            )
            all_files.extend(day_files)

        return [str(f) for f in all_files]

    def generate_vessel_movement_data(self) -> list:
        """Generate AIS vessel movement data - BILLIONS scale."""
        print(
            f"\n[5/6] Generating Vessel Movement Data "
            f"({self.volumes['total_vessel_movements']:,} records)..."
        )

        movement_gen = VesselMovementGenerator(seed=46)
        movement_dir = self.output_dir / "vessel_movements"
        movement_dir.mkdir(parents=True, exist_ok=True)

        all_files = []
        start_date = date.today() - timedelta(days=self.volumes["vessel_movement_days"])

        for day_offset in tqdm(range(self.volumes["vessel_movement_days"]), desc="Movement days"):
            current_date = start_date + timedelta(days=day_offset)
            date_dir = movement_dir / current_date.strftime("%Y/%m/%d")
            date_dir.mkdir(parents=True, exist_ok=True)

            day_files = movement_gen.generate_to_parquet(
                output_dir=date_dir,
                total_records=self.volumes["vessel_movements_per_day"],
                batch_size=min(self.volumes["vessel_movements_per_day"], 1_000_000),
                file_prefix="movements",
                movement_date=current_date,
            )
            all_files.extend(day_files)

        return [str(f) for f in all_files]

    def generate_screening_data(self) -> list:
        """Generate screening results linked to trades."""
        print(f"\n[6/6] Generating Screening Results ({self.volumes['total_screenings']:,} records)...")

        screening_gen = ScreeningResultGenerator(seed=47)
        screening_files = screening_gen.generate_to_parquet(
            output_dir=self.output_dir / "screening_results",
            total_records=self.volumes["total_screenings"],
            batch_size=min(self.volumes["total_screenings"], 500_000),
            file_prefix="screening",
        )
        return [str(f) for f in screening_files]

    def generate_all(self) -> Path:
        """Generate all data and write manifest."""
        start_time = datetime.now()

        # 1. Dimensions first
        manifest = self.generate_dimension_data()

        # 2. Facts
        manifest["trades"] = self.generate_trade_data()
        manifest["vessel_movements"] = self.generate_vessel_movement_data()
        manifest["screening_results"] = self.generate_screening_data()

        # Write manifest
        manifest_data = {
            "environment": self.environment,
            "generated_at": datetime.now().isoformat(),
            "volumes": self.volumes,
            "files": manifest,
        }

        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest_data, f, indent=2, default=str)

        elapsed = datetime.now() - start_time

        print(f"\n{'=' * 60}")
        print(f"GENERATION COMPLETE")
        print(f"{'=' * 60}")
        print(f"  Environment:  {self.environment.upper()}")
        print(f"  Output dir:   {self.output_dir}")
        print(f"  Manifest:     {manifest_path}")
        print(f"  Duration:     {elapsed}")
        print(f"  Total files:  {sum(len(v) for v in manifest.values())}")
        print(f"{'=' * 60}")

        return manifest_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for Sanctions Risk Analytics Platform"
    )
    parser.add_argument(
        "--env",
        choices=["dev", "test", "prod"],
        default="dev",
        help="Target environment (controls data volume)",
    )
    parser.add_argument(
        "--output",
        default="./generated_data",
        help="Output directory for generated files",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (for prod-scale generation)",
    )

    args = parser.parse_args()

    orchestrator = DataOrchestrator(
        environment=args.env,
        output_dir=args.output,
    )

    manifest_path = orchestrator.generate_all()

    print(f"\nNext step: Load data to Snowflake:")
    print(f"  python load_to_snowflake.py --manifest {manifest_path}")


if __name__ == "__main__":
    main()
