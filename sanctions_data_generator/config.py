"""
Configuration module for Sanctions Risk Analytics Platform.

Contains all configuration dataclasses for Snowflake connection,
data volume scaling, sanctions domain parameters, and commodity
reference data.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration - reads from environment variables."""

    account: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_ACCOUNT", ""))
    user: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_USER", ""))
    password: str = field(default_factory=lambda: os.getenv("SNOWFLAKE_PASSWORD", ""))
    role: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", "SANCTIONS_DATA_ENGINEER")
    )
    warehouse: str = field(
        default_factory=lambda: os.getenv(
            "SNOWFLAKE_WAREHOUSE", "SANCTIONS_LOADING_WH_M"
        )
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
    Data volume configuration for synthetic data generation.

    Production-scale volumes:
      - Counterparties:       5,000,000
      - Vessels:              500,000
      - Sanctioned Entities:  250,000
      - Trades/day:           500,000  (x 1,825 days = 912.5M)
      - Vessel movements/day: 10,000,000  (x 365 days = 3.65B)

    Use DEV_MULTIPLIER (0.001) or TEST_MULTIPLIER (0.01) to scale down.
    """

    COUNTERPARTIES: int = 5_000_000
    VESSELS: int = 500_000
    SANCTIONED_ENTITIES: int = 250_000
    COMMODITIES: int = 5_000
    GEOGRAPHIES: int = 250

    TRADES_PER_DAY: int = 500_000
    TRADE_HISTORY_DAYS: int = 1_825        # ~5 years

    VESSEL_MOVEMENTS_PER_DAY: int = 10_000_000
    VESSEL_MOVEMENT_DAYS: int = 365        # 1 year

    SCREENING_RESULTS_RATIO: float = 0.15  # 15% of trades generate screening results

    BATCH_SIZE: int = 1_000_000
    PARQUET_ROW_GROUP_SIZE: int = 100_000

    DEV_MULTIPLIER: float = 0.001          # ~1/1000 of prod
    TEST_MULTIPLIER: float = 0.01          # ~1/100 of prod


@dataclass
class SanctionsConfig:
    """Domain-specific configuration for sanctions screening parameters."""

    SANCTIONS_LISTS: List[str] = field(default_factory=lambda: [
        "OFAC_SDN", "OFAC_CONS", "EU_SANCTIONS", "UN_SANCTIONS",
        "UK_SANCTIONS", "OFAC_SSI", "BIS_ENTITY", "BIS_DENIED",
    ])

    SANCTIONS_PROGRAMS: List[str] = field(default_factory=lambda: [
        "IRAN", "NORTH_KOREA", "SYRIA", "CUBA", "RUSSIA", "BELARUS",
        "VENEZUELA", "MYANMAR", "SUDAN", "ZIMBABWE", "CRIMEA",
        "DONETSK", "LUHANSK", "GLOBAL_TERRORISM", "NARCOTICS",
        "WMD_PROLIFERATION", "CYBER", "HUMAN_RIGHTS",
    ])

    HIGH_RISK_COUNTRIES: List[str] = field(default_factory=lambda: [
        "IR", "KP", "SY", "CU", "RU", "BY", "VE", "MM", "ZW", "SD",
    ])

    ELEVATED_RISK_COUNTRIES: List[str] = field(default_factory=lambda: [
        "CN", "AE", "TR", "PK", "LB", "IQ", "AF", "YE", "LY", "SO",
    ])

    # Match score distribution parameters
    EXACT_MATCH_PERCENTAGE: float = 0.02   # 2% exact matches
    HIGH_MATCH_PERCENTAGE: float = 0.08    # 8% high confidence
    MEDIUM_MATCH_PERCENTAGE: float = 0.20  # 20% medium confidence
    # Remaining ~70% are low-confidence / false positives


@dataclass
class CommodityConfig:
    """Commodity reference data for trade generation."""

    COMMODITY_GROUPS: List[str] = field(default_factory=lambda: [
        "CRUDE_OIL", "REFINED_PRODUCTS", "LNG", "LPG",
        "COAL", "IRON_ORE", "METALS", "GRAINS",
        "SOFT_COMMODITIES", "CHEMICALS", "FERTILIZERS",
    ])

    COMMODITIES: dict = field(default_factory=lambda: {
        "CRUDE_OIL": [
            ("CRU_BRENT_0001", "BRENT_CRUDE"),
            ("CRU_WTI___0002", "WTI_CRUDE"),
            ("CRU_URALS_0003", "URALS_CRUDE"),
            ("CRU_ESPO__0004", "ESPO_CRUDE"),
            ("CRU_ARAB__0005", "ARAB_LIGHT"),
            ("CRU_DUBAI_0006", "DUBAI_CRUDE"),
        ],
        "REFINED_PRODUCTS": [
            ("REF_GASOL_0001", "GASOLINE"),
            ("REF_DIESE_0002", "DIESEL"),
            ("REF_JETFU_0003", "JET_FUEL"),
            ("REF_NAPHT_0004", "NAPHTHA"),
            ("REF_FUEOI_0005", "FUEL_OIL"),
        ],
        "LNG": [
            ("LNG_SPOT__0001", "LNG_SPOT"),
            ("LNG_CONTR_0002", "LNG_CONTRACT"),
        ],
        "LPG": [
            ("LPG_PROPA_0001", "PROPANE"),
            ("LPG_BUTAN_0002", "BUTANE"),
        ],
        "METALS": [
            ("MET_GOLD__0001", "GOLD"),
            ("MET_SILVE_0002", "SILVER"),
            ("MET_PLATI_0003", "PLATINUM"),
            ("MET_PALLA_0004", "PALLADIUM"),
            ("MET_COPPE_0005", "COPPER"),
            ("MET_ALUMI_0006", "ALUMINIUM"),
            ("MET_NICKE_0007", "NICKEL"),
        ],
        "COAL": [
            ("COL_THERM_0001", "THERMAL_COAL"),
            ("COL_COKIN_0002", "COKING_COAL"),
        ],
        "GRAINS": [
            ("GRN_WHEAT_0001", "WHEAT"),
            ("GRN_CORN__0002", "CORN"),
            ("GRN_SOYBE_0003", "SOYBEANS"),
        ],
        "CHEMICALS": [
            ("CHM_METHA_0001", "METHANOL"),
            ("CHM_AMMON_0002", "AMMONIA"),
            ("CHM_UREA__0003", "UREA"),
        ],
    })

    INCOTERMS: List[str] = field(default_factory=lambda: [
        "FOB", "CIF", "CFR", "DES", "DAP", "FAS", "EXW",
    ])

    TRADE_TYPES: List[str] = field(default_factory=lambda: [
        "PHYSICAL", "PAPER", "SWAP", "OPTION", "FUTURE",
    ])


# Module-level singleton instances
snowflake_config = SnowflakeConfig()
volume_config = DataVolumeConfig()
sanctions_config = SanctionsConfig()
commodity_config = CommodityConfig()
