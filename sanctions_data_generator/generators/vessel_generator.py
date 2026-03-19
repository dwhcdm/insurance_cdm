"""
Vessel master data generator.

Generates realistic vessel records with IMO numbers, MMSI, vessel types,
flag states, tonnage, and ownership details.
"""

from datetime import datetime

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class VesselGenerator(BaseGenerator):
    """Generate vessel master data with realistic maritime attributes."""

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

    CLASS_SOCIETIES = [
        "LLOYD'S_REGISTER", "DNV", "BUREAU_VERITAS", "ABS",
        "CLASS_NK", "RINA", "KR", "CCS", "IRS",
    ]

    BUILDERS = [
        "HYUNDAI_HEAVY", "SAMSUNG_HEAVY", "DAEWOO",
        "IMABARI", "OSHIMA", "TSUNEISHI",
        "CSSC", "COSCO_SHIPPING_HEAVY", "YANGZIJIANG",
    ]

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of vessel master records."""
        records = []

        vessel_type_names = list(self.VESSEL_TYPES.keys())

        for i in range(batch_size):
            idx = batch_offset + i
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

            record = {
                "vessel_id": f"VS{idx:010d}",
                "imo_number": f"IMO{9000000 + idx}",
                "mmsi": f"{200000000 + idx}",
                "vessel_name": f"M/V {self.fake.last_name().upper()} {np.random.choice(['STAR', 'GLORY', 'SPIRIT', 'PEARL', 'FORTUNE', 'PIONEER', 'GUARDIAN', 'VOYAGER'])}",
                "call_sign": self.fake.bothify(text="????#").upper(),
                "vessel_type": vessel_type,
                "flag_state": flag_state,
                "class_society": np.random.choice(self.CLASS_SOCIETIES),
                "dwt": dwt,
                "gross_tonnage": gt,
                "year_built": year_built,
                "builder": np.random.choice(self.BUILDERS),
                "status": np.random.choice(
                    ["ACTIVE", "LAID_UP", "SCRAPPED", "UNDER_CONSTRUCTION"],
                    p=[0.85, 0.08, 0.05, 0.02],
                ),
                "is_flagged": is_flagged,
                "registered_owner": self.fake.company(),
                "beneficial_owner": (
                    self.fake.company() if np.random.random() < 0.60
                    else None
                ),
                "source_system": np.random.choice(
                    ["LLOYD_LIST", "EQUASIS", "MARINETRAFFIC", "MANUAL"]
                ),
                "_loaded_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
