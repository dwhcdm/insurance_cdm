"""
Configuration module for the Sanctions Risk Analytics Platform data generator.

Contains all configuration dataclasses for Snowflake connection,
data volumes, sanctions reference data, and commodity reference data.
"""

import os
from dataclasses import dataclass, field


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration sourced from environment variables."""

    account: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ACCOUNT", ""))
    user: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_PASSWORD", ""))
    role: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", "SANCTIONS_DATA_ENGINEER")
    )
    warehouse: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "SANCTIONS_LOADING_WH_M")
    )
    database: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", "SANCTIONS_DEV")
    )
    schema: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_SCHEMA", "RAW")
    )


@dataclass
class DataVolumeConfig:
    """
    Production-scale data volume targets.

    Use DEV_MULTIPLIER (0.001) or TEST_MULTIPLIER (0.01) for smaller environments.
    """

    # Dimension tables (total records)
    COUNTERPARTIES: int = 5_000_000
    VESSELS: int = 500_000
    SANCTIONED_ENTITIES: int = 250_000

    # Fact tables (records per day)
    TRADES_PER_DAY: int = 2_500_000
    VESSEL_MOVEMENTS_PER_DAY: int = 10_000_000
    SCREENING_RESULTS_RATIO: float = 0.15  # Per trade

    # History depth
    TRADE_HISTORY_DAYS: int = 365
    VESSEL_MOVEMENT_DAYS: int = 365

    # Environment multipliers
    DEV_MULTIPLIER: float = 0.001
    TEST_MULTIPLIER: float = 0.01


@dataclass
class SanctionsConfig:
    """Reference data for sanctions screening simulation."""

    SANCTIONS_LISTS: list = field(default_factory=lambda: [
        "OFAC_SDN", "OFAC_CONS", "EU_SANCTIONS", "UN_SANCTIONS",
        "UK_SANCTIONS", "OFAC_SSI", "BIS_ENTITY", "BIS_DENIED",
    ])

    SANCTIONS_PROGRAMS: list = field(default_factory=lambda: [
        "IRAN", "NORTH_KOREA", "RUSSIA", "SYRIA", "CUBA",
        "VENEZUELA", "BELARUS", "MYANMAR", "SUDAN", "SOMALIA",
        "YEMEN", "LIBYA", "LEBANON", "IRAQ", "AFGHANISTAN",
    ])

    HIGH_RISK_COUNTRIES: list = field(default_factory=lambda: [
        "IR", "KP", "SY", "CU", "VE", "BY", "MM", "SD", "RU",
    ])

    ELEVATED_RISK_COUNTRIES: list = field(default_factory=lambda: [
        "SO", "YE", "LB", "LY", "IQ", "AF", "PK", "NG", "TR",
    ])

    # Match score distribution parameters
    EXACT_MATCH_PERCENTAGE: float = 0.02
    HIGH_MATCH_PERCENTAGE: float = 0.08
    MEDIUM_MATCH_PERCENTAGE: float = 0.15


@dataclass
class CommodityConfig:
    """Reference data for commodity trading simulation."""

    COMMODITIES: dict = field(default_factory=lambda: {
        "CRUDE_OIL": [
            ("CRD_BRT", "Brent Crude"), ("CRD_WTI", "WTI Crude"),
            ("CRD_DUB", "Dubai Crude"), ("CRD_URAL", "Urals Crude"),
            ("CRD_MURB", "Murban Crude"),
        ],
        "REFINED_PRODUCTS": [
            ("PRD_ULSD", "Ultra Low Sulphur Diesel"), ("PRD_JET", "Jet Fuel A1"),
            ("PRD_MOGAS", "Motor Gasoline"), ("PRD_NAPH", "Naphtha"),
            ("PRD_FUEL", "Fuel Oil 380cst"), ("PRD_VLSFO", "VLSFO 0.5%"),
        ],
        "LNG": [
            ("LNG_SPOT", "LNG Spot"), ("LNG_JKM", "LNG JKM"),
            ("LNG_NBP", "LNG NBP"),
        ],
        "METALS": [
            ("MET_ALUM", "Aluminium"), ("MET_COPP", "Copper"),
            ("MET_NICK", "Nickel"), ("MET_ZINC", "Zinc"),
            ("MET_IRON", "Iron Ore"),
        ],
        "AGRICULTURE": [
            ("AGR_WHEAT", "Wheat"), ("AGR_CORN", "Corn/Maize"),
            ("AGR_SOYB", "Soybeans"), ("AGR_PALM", "Palm Oil"),
            ("AGR_SUGAR", "Raw Sugar"),
        ],
    })

    INCOTERMS: list = field(default_factory=lambda: [
        "FOB", "CIF", "CFR", "DES", "DAP", "FAS", "EXW",
    ])

    TRADE_TYPES: list = field(default_factory=lambda: [
        "PHYSICAL", "PAPER", "SWAP", "OPTION", "FUTURE",
    ])


# Module-level singleton instances
snowflake_config = SnowflakeConfig()
volume_config = DataVolumeConfig()
sanctions_config = SanctionsConfig()
commodity_config = CommodityConfig()
