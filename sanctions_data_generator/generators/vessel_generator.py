"""
Vessel master data generator.

Generates realistic vessel records with IMO numbers, MMSI, vessel types,
flag states, tonnage, and ownership details.

Production-grade BAU issues injected:
    - Flag hopping history (frequent flag changes = evasion indicator)
    - IMO/MMSI spoofing (duplicate identifiers across vessels)
    - Beneficial ownership opacity (missing or circular ownership)
    - Name changes to evade detection (vessel renamed multiple times)
    - Stale registration data (vessel scrapped but still ACTIVE in system)
    - Conflicting data across source systems (Lloyd's vs Equasis vs MT)
    - Age-related classification issues (year_built inconsistencies)
    - Ghost vessels (registered but never observed via AIS)
"""

from datetime import datetime

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator
from generators.data_quality_issues import (
    DataQualityIssueInjector,
    IssueInjectionRates,
    NameVariationGenerator,
    SanctionsScenarioGenerator,
)


class VesselGenerator(BaseGenerator):
    """Generate vessel master data with realistic maritime attributes and BAU issues."""

    VESSEL_TYPES = {
        "VLCC":             {"dwt_min": 200_000, "dwt_max": 320_000},
        "SUEZMAX":          {"dwt_min": 120_000, "dwt_max": 200_000},
        "AFRAMAX":          {"dwt_min": 80_000,  "dwt_max": 120_000},
        "PANAMAX":          {"dwt_min": 60_000,  "dwt_max": 80_000},
        "MR_TANKER":        {"dwt_min": 25_000,  "dwt_max": 55_000},
        "LNG_CARRIER":      {"dwt_min": 50_000,  "dwt_max": 100_000},
        "BULK_CARRIER":     {"dwt_min": 30_000,  "dwt_max": 200_000},
        "CONTAINER":        {"dwt_min": 10_000,  "dwt_max": 200_000},
        "GENERAL_CARGO":    {"dwt_min": 5_000,   "dwt_max": 30_000},
        "CHEMICAL":         {"dwt_min": 5_000,   "dwt_max": 50_000},
    }

    # Open registry / flag of convenience states weighted higher
    FLAG_STATES = [
        "PA", "LR", "MH", "HK", "SG", "BS", "MT", "CY",
        "GB", "NO", "GR", "JP", "CN", "KR", "US", "DE",
        "DK", "IT", "IN", "TR", "SA", "AE", "MY", "ID",
        "RU", "IR",
    ]
    FLAG_WEIGHTS = [
        0.12, 0.10, 0.10, 0.08, 0.06, 0.05, 0.05, 0.04,
        0.04, 0.04, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02,
        0.02, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.01,
        0.03, 0.03,
    ]

    SANCTIONED_FLAGS = {"IR", "KP", "SY", "RU", "CU", "VE"}

    # Flag-hopping convenience flags
    FLAG_HOP_FLAGS = ["PA", "LR", "MH", "BS", "MT", "CY", "KM", "TZ", "TG", "CM"]

    CLASS_SOCIETIES = [
        "LLOYD'S_REGISTER", "DNV", "BUREAU_VERITAS", "ABS",
        "CLASS_NK", "RINA", "KR", "CCS", "IRS",
    ]

    BUILDERS = [
        "HYUNDAI_HEAVY", "SAMSUNG_HEAVY", "DAEWOO",
        "IMABARI", "OSHIMA", "TSUNEISHI",
        "CSSC", "COSCO_SHIPPING_HEAVY", "YANGZIJIANG",
    ]

    VESSEL_NAME_SUFFIXES = [
        "STAR", "GLORY", "SPIRIT", "PEARL", "FORTUNE",
        "PIONEER", "GUARDIAN", "VOYAGER", "HARMONY", "LIBERTY",
    ]

    def __init__(self, seed: int = 42, issue_rates: IssueInjectionRates | None = None):
        super().__init__(seed)
        self.injector = DataQualityIssueInjector(issue_rates, seed)
        self.name_gen = NameVariationGenerator(seed)
        self.scenario_gen = SanctionsScenarioGenerator(seed)

        # Track IMO/MMSI for spoofing injection
        self._used_imos: set[str] = set()
        self._used_mmsis: set[str] = set()
        self._spoofed_imo_pool: list[str] = []
        self._spoofed_mmsi_pool: list[str] = []

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of vessel master records with production-grade issues."""
        records = []

        vessel_type_names = list(self.VESSEL_TYPES.keys())

        for i in range(batch_size):
            idx = batch_offset + i
            record_id = f"VS{idx:010d}"
            vessel_type = np.random.choice(vessel_type_names)
            specs = self.VESSEL_TYPES[vessel_type]

            flag_state = np.random.choice(self.FLAG_STATES, p=self.FLAG_WEIGHTS)

            is_flagged = (
                flag_state in self.SANCTIONED_FLAGS
                or np.random.random() < 0.03
            )

            dwt = round(np.random.uniform(specs["dwt_min"], specs["dwt_max"]), 2)
            gt = round(dwt * np.random.uniform(0.4, 0.7), 2)
            year_built = int(np.random.choice(range(1990, 2025)))

            imo_number = f"IMO{9000000 + idx}"
            mmsi = f"{200000000 + idx}"

            # IMO/MMSI spoofing (duplicate identifiers)
            if np.random.random() < 0.005 and self._spoofed_imo_pool:
                imo_number = np.random.choice(self._spoofed_imo_pool)
            else:
                self._used_imos.add(imo_number)
                if np.random.random() < 0.01:
                    self._spoofed_imo_pool.append(imo_number)

            if np.random.random() < 0.005 and self._spoofed_mmsi_pool:
                mmsi = np.random.choice(self._spoofed_mmsi_pool)
            else:
                self._used_mmsis.add(mmsi)
                if np.random.random() < 0.01:
                    self._spoofed_mmsi_pool.append(mmsi)

            vessel_name = f"M/V {self.fake.last_name().upper()} {np.random.choice(self.VESSEL_NAME_SUFFIXES)}"

            # Vessel renaming (evasion indicator)
            previous_names = None
            if np.random.random() < 0.03:
                num_previous = np.random.randint(1, 4)
                previous_names = [
                    f"M/V {self.fake.last_name().upper()} {np.random.choice(self.VESSEL_NAME_SUFFIXES)}"
                    for _ in range(num_previous)
                ]

            # Flag hopping history
            flag_history = None
            if np.random.random() < 0.04:
                num_changes = np.random.randint(2, 6)
                flag_history = self.scenario_gen.generate_flag_hopping_history(num_changes)
                flag_state = np.random.choice(self.FLAG_HOP_FLAGS)

            # Stale status: vessel scrapped but still ACTIVE
            status = np.random.choice(
                ["ACTIVE", "LAID_UP", "SCRAPPED", "UNDER_CONSTRUCTION"],
                p=[0.85, 0.08, 0.05, 0.02],
            )
            if np.random.random() < 0.01 and status == "SCRAPPED":
                status = "ACTIVE"

            # Beneficial ownership opacity
            registered_owner = self.fake.company()
            beneficial_owner = None
            if np.random.random() < 0.60:
                beneficial_owner = self.fake.company()
            if np.random.random() < 0.02 and beneficial_owner:
                beneficial_owner = registered_owner

            record = {
                "vessel_id": record_id,
                "imo_number": imo_number,
                "mmsi": mmsi,
                "vessel_name": vessel_name,
                "call_sign": self.fake.bothify(text="????#").upper(),
                "vessel_type": vessel_type,
                "flag_state": flag_state,
                "class_society": np.random.choice(self.CLASS_SOCIETIES),
                "dwt": dwt,
                "gross_tonnage": gt,
                "year_built": year_built,
                "builder": np.random.choice(self.BUILDERS),
                "status": status,
                "is_flagged": is_flagged,
                "registered_owner": registered_owner,
                "beneficial_owner": beneficial_owner,
                "source_system": np.random.choice(
                    ["LLOYD_LIST", "EQUASIS", "MARINETRAFFIC", "MANUAL"]
                ),
                "_loaded_at": datetime.now(),
            }

            # Inject data quality issues
            record, near_dupe = self.injector.inject_all(
                record,
                record_id=record_id,
                nullable_fields=[
                    "call_sign", "class_society", "builder",
                    "beneficial_owner", "mmsi", "gross_tonnage",
                ],
                text_fields=[
                    "vessel_name", "registered_owner", "beneficial_owner",
                    "builder",
                ],
                positive_fields=["dwt", "gross_tonnage"],
                bounded_fields={
                    "year_built": (1950, 2026),
                },
                enum_fields={
                    "vessel_type": ["FSO", "FPSO", "BARGE", "TUG", "YACHT", "UNKNOWN"],
                    "status": ["DETAINED", "MISSING", "DARK", "DECOMMISSIONED"],
                    "flag_state": ["XX", "UNKNOWN", "STATELESS"],
                },
                mutable_fields=["vessel_name", "flag_state", "registered_owner"],
                contradictions=[
                    ("is_flagged", True, "flag_state", "NO"),
                    ("status", "UNDER_CONSTRUCTION", "year_built", 2018),
                ],
            )

            records.append(record)

            exact_dupe = self.injector.maybe_create_exact_duplicate(record)
            if exact_dupe:
                records.append(exact_dupe)

            if near_dupe:
                records.append(near_dupe)

        return pd.DataFrame(records)
