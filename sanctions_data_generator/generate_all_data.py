"""
Master data generation orchestrator.

Generates all synthetic data for the Sanctions Risk Analytics Platform
with environment-based volume scaling (dev/test/prod).

Usage:
    python generate_all_data.py --env dev --output ./generated_data
    python generate_all_data.py --env test --output ./generated_data
    python generate_all_data.py --env prod --output ./generated_data --workers 8
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from config import issue_config, volume_config
from generators.data_quality_issues import IssueInjectionRates
from generators.counterparty_generator import CounterpartyGenerator
from generators.sanctions_list_generator import SanctionsListGenerator
from generators.screening_generator import ScreeningResultGenerator
from generators.trade_generator import TradeGenerator
from generators.vessel_generator import VesselGenerator
from generators.vessel_movement_generator import VesselMovementGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class DataOrchestrator:
    """Orchestrate synthetic data generation across all domains."""

    ENV_MULTIPLIERS = {
        "dev":  volume_config.DEV_MULTIPLIER,    # 0.001x
        "test": volume_config.TEST_MULTIPLIER,   # 0.01x
        "prod": 1.0,                             # Full scale
    }

    def __init__(self, env: str, output_dir: str, seed: int = 42,
                 issue_profile: str | None = None):
        self.env = env
        self.multiplier = self.ENV_MULTIPLIERS[env]
        self.output_dir = Path(output_dir)
        self.seed = seed

        # Resolve issue injection profile
        profile = issue_profile or issue_config.profile
        self.issue_rates = self._resolve_issue_rates(profile)

        self.manifest = {
            "environment": env,
            "issue_profile": profile,
            "generated_at": datetime.now().isoformat(),
            "files": {},
        }

        logger.info(
            f"Initializing data orchestrator: env={env}, "
            f"multiplier={self.multiplier}, issue_profile={profile}"
        )

    @staticmethod
    def _resolve_issue_rates(profile: str) -> IssueInjectionRates:
        """Map a profile name to an IssueInjectionRates instance."""
        profiles = {
            "clean": IssueInjectionRates.clean,
            "dev": IssueInjectionRates.dev,
            "test": IssueInjectionRates.test,
            "prod_realistic": IssueInjectionRates.prod_realistic,
            "stress": IssueInjectionRates.stress,
        }
        factory = profiles.get(profile)
        if factory is None:
            raise ValueError(
                f"Unknown issue profile '{profile}'. "
                f"Choose from: {', '.join(profiles)}"
            )
        return factory()

    def _scale(self, count: int) -> int:
        """Apply environment multiplier and ensure at least 10 records."""
        return max(int(count * self.multiplier), 10)

    def generate_dimension_data(self) -> None:
        """Generate all dimension / reference data."""
        logger.info("=== Generating dimension data ===")

        # Counterparties
        gen = CounterpartyGenerator(seed=self.seed, issue_rates=self.issue_rates)
        count = self._scale(volume_config.COUNTERPARTIES)
        logger.info(f"Generating {count:,} counterparty records...")
        files = gen.generate_to_parquet(
            self.output_dir / "counterparties", count,
            file_prefix="counterparties",
        )
        self.manifest["files"]["counterparties"] = {
            "count": count, "files": [str(f) for f in files],
        }

        # Sanctions lists
        gen = SanctionsListGenerator(seed=self.seed + 1, issue_rates=self.issue_rates)
        count = self._scale(volume_config.SANCTIONED_ENTITIES)
        logger.info(f"Generating {count:,} sanctions list entries...")
        files = gen.generate_to_parquet(
            self.output_dir / "sanctions_lists", count,
            file_prefix="sanctions_lists",
        )
        self.manifest["files"]["sanctions_lists"] = {
            "count": count, "files": [str(f) for f in files],
        }

        # Vessels
        gen = VesselGenerator(seed=self.seed + 2, issue_rates=self.issue_rates)
        count = self._scale(volume_config.VESSELS)
        logger.info(f"Generating {count:,} vessel records...")
        files = gen.generate_to_parquet(
            self.output_dir / "vessels", count,
            file_prefix="vessels",
        )
        self.manifest["files"]["vessels"] = {
            "count": count, "files": [str(f) for f in files],
        }

    def generate_trade_data(self, days: int = None) -> None:
        """Generate trade transaction data for the specified number of days."""
        days = days or min(volume_config.TRADE_HISTORY_DAYS, 365)
        daily_count = self._scale(volume_config.TRADES_PER_DAY)

        logger.info(f"=== Generating trade data: {daily_count:,}/day x {days} days ===")

        gen = TradeGenerator(seed=self.seed + 3, issue_rates=self.issue_rates)
        all_files = []

        for day_offset in range(days):
            trade_date = (datetime.now() - timedelta(days=days - day_offset)).date()
            files = gen.generate_to_parquet(
                self.output_dir / "trades" / trade_date.strftime("%Y/%m/%d"),
                daily_count,
                file_prefix=f"trades_{trade_date.strftime('%Y%m%d')}",
                trade_date=trade_date,
            )
            all_files.extend(files)

            if (day_offset + 1) % 30 == 0:
                logger.info(f"  ... {day_offset + 1}/{days} days complete")

        self.manifest["files"]["trades"] = {
            "daily_count": daily_count,
            "days": days,
            "total_count": daily_count * days,
            "files": [str(f) for f in all_files],
        }

    def generate_vessel_movement_data(self, days: int = None) -> None:
        """Generate AIS vessel movement data (BILLIONS scale at prod)."""
        days = days or min(volume_config.VESSEL_MOVEMENT_DAYS, 365)
        daily_count = self._scale(volume_config.VESSEL_MOVEMENTS_PER_DAY)

        logger.info(f"=== Generating vessel movements: {daily_count:,}/day x {days} days ===")

        gen = VesselMovementGenerator(seed=self.seed + 4, issue_rates=self.issue_rates)
        all_files = []

        for day_offset in range(days):
            movement_date = (datetime.now() - timedelta(days=days - day_offset)).date()
            files = gen.generate_to_parquet(
                self.output_dir / "vessel_movements" / movement_date.strftime("%Y/%m/%d"),
                daily_count,
                file_prefix=f"movements_{movement_date.strftime('%Y%m%d')}",
                movement_date=movement_date,
            )
            all_files.extend(files)

            if (day_offset + 1) % 30 == 0:
                logger.info(f"  ... {day_offset + 1}/{days} days complete")

        self.manifest["files"]["vessel_movements"] = {
            "daily_count": daily_count,
            "days": days,
            "total_count": daily_count * days,
            "files": [str(f) for f in all_files],
        }

    def generate_screening_data(self, days: int = None) -> None:
        """Generate screening result data linked to trades."""
        days = days or min(volume_config.TRADE_HISTORY_DAYS, 365)
        daily_count = self._scale(
            int(volume_config.TRADES_PER_DAY * volume_config.SCREENING_RESULTS_RATIO)
        )

        logger.info(f"=== Generating screening results: {daily_count:,}/day x {days} days ===")

        gen = ScreeningResultGenerator(seed=self.seed + 5, issue_rates=self.issue_rates)
        all_files = []

        for day_offset in range(days):
            screening_date = (datetime.now() - timedelta(days=days - day_offset)).date()
            files = gen.generate_to_parquet(
                self.output_dir / "screening_results" / screening_date.strftime("%Y/%m/%d"),
                daily_count,
                file_prefix=f"screening_{screening_date.strftime('%Y%m%d')}",
                screening_date=screening_date,
            )
            all_files.extend(files)

            if (day_offset + 1) % 30 == 0:
                logger.info(f"  ... {day_offset + 1}/{days} days complete")

        self.manifest["files"]["screening_results"] = {
            "daily_count": daily_count,
            "days": days,
            "total_count": daily_count * days,
            "files": [str(f) for f in all_files],
        }

    def generate_all(self) -> Path:
        """Generate all data types and write manifest."""
        start = datetime.now()
        logger.info(f"Starting full data generation for env={self.env}")

        self.generate_dimension_data()
        self.generate_trade_data()
        self.generate_vessel_movement_data()
        self.generate_screening_data()

        elapsed = (datetime.now() - start).total_seconds()
        self.manifest["elapsed_seconds"] = elapsed

        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(self.manifest, f, indent=2, default=str)

        logger.info(f"Generation complete in {elapsed:.1f}s. Manifest: {manifest_path}")
        return manifest_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sanctions data")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default="dev")
    parser.add_argument("--output", default="./generated_data")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers (future use)")
    parser.add_argument(
        "--issue-profile",
        choices=["clean", "dev", "test", "prod_realistic", "stress"],
        default=None,
        help="Data quality issue injection profile (default: from config)",
    )
    args = parser.parse_args()

    orchestrator = DataOrchestrator(
        env=args.env, output_dir=args.output, seed=args.seed,
        issue_profile=args.issue_profile,
    )
    orchestrator.generate_all()


if __name__ == "__main__":
    main()
