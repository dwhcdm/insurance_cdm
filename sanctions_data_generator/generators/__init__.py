"""
Synthetic data generators for the Sanctions Risk Analytics Platform.

Each generator produces domain-specific data at configurable volumes,
outputting Parquet files optimized for Snowflake COPY INTO ingestion.

All generators support injection of production-grade data quality issues
via the DataQualityIssueInjector, controlled by IssueInjectionRates profiles.
"""

from generators.base_generator import BaseGenerator
from generators.counterparty_generator import CounterpartyGenerator
from generators.data_quality_issues import (
    DataQualityIssueInjector,
    IssueInjectionRates,
    NameVariationGenerator,
    SanctionsScenarioGenerator,
)
from generators.sanctions_list_generator import SanctionsListGenerator
from generators.vessel_generator import VesselGenerator
from generators.vessel_movement_generator import VesselMovementGenerator
from generators.trade_generator import TradeGenerator
from generators.screening_generator import ScreeningResultGenerator

__all__ = [
    "BaseGenerator",
    "CounterpartyGenerator",
    "SanctionsListGenerator",
    "VesselGenerator",
    "VesselMovementGenerator",
    "TradeGenerator",
    "ScreeningResultGenerator",
    "DataQualityIssueInjector",
    "IssueInjectionRates",
    "NameVariationGenerator",
    "SanctionsScenarioGenerator",
]
