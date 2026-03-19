"""
Vessel master data generator.

Generates realistic vessel records including tankers, bulk carriers,
container ships, and LNG carriers with appropriate flag states,
classifications, and risk indicators.
"""

from datetime import datetime

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class VesselGenerator(BaseGenerator):
    """Generate vessel master data mimicking Lloyd's List / Equasis."""

    VESSEL_TYPES = {
        "VLCC":        (200_000, 320_000),   # DWT range
        "SUEZMAX":     (120_000, 200_000),
        "AFRAMAX":     (80_000,  120_000),
        "PANAMAX":     (60_000,  80_000),
        "MR_TANKER":   (25_000,  55_000),
        "LNG_CARRIER": (60_000,  180_000),
        "BULK_CARRIER": (30_000, 400_000),
        "CONTAINER":   (10_000,  200_000),
        "GENERAL_CARGO": (5_000, 30_000),
        "CHEMICAL":    (5_000,   40_000),
    }

    FLAG_STATES = [
        "PA", "LR", "MH", "HK", "SG", "BS", "MT", "CY", "GB", "GR",
        "NO", "DK", "JP", "KR", "CN", "US", "IT", "DE", "FR", "NL",
        "BE", "PT", "IN", "AE", "SA", "BH", "MY", "TH", "ID", "PH",
    ] + ["IR", "KP", "SY", "RU", "BY", "VE", "CU"]  # Sanctioned flag states

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of vessel master records."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i

            vessel_type = np.random.choice(list(self.VESSEL_TYPES.keys()))
            dwt_range = self.VESSEL_TYPES[vessel_type]
            build_year = np.random.randint(1990, 2025)

            # Flag state distribution - most are open-registry
            flag_state = np.random.choice(
                self.FLAG_STATES,
                p=self._flag_state_probs(),
            )

            is_flagged = (
                flag_state in sanctions_config.HIGH_RISK_COUNTRIES
                or (np.random.random() < 0.03)  # 3% random flagging
            )

            record = {
                "vessel_id": f"VS{idx:010d}",
                "imo_number": f"IMO{9000000 + idx}",
                "mmsi_number": f"{200000000 + idx}",
                "vessel_name": (
                    f"{np.random.choice(['MV', 'MT', 'MS'])} "
                    f"{self.fake.last_name().upper()} "
                    f"{np.random.choice(['STAR', 'GLORY', 'FORTUNE', 'SPIRIT', 'PEARL', 'OCEAN', 'WAVE', 'WIND', 'SUN', 'MOON'])}"
                ),
                "vessel_type": vessel_type,
                "flag_state": flag_state,
                "dwt_tonnage": np.random.randint(dwt_range[0], dwt_range[1]),
                "gross_tonnage": np.random.randint(dwt_range[0] // 2, dwt_range[1] // 2),
                "build_year": build_year,
                "builder": np.random.choice([
                    "HYUNDAI", "SAMSUNG", "DAEWOO", "IMABARI", "CSSC",
                    "COSCO", "TSUNEISHI",
                ]),
                "owner_name": f"{self.fake.company()} Shipping",
                "operator_name": f"{self.fake.company()} Maritime",
                "class_society": np.random.choice([
                    "DNV", "LLOYD", "BV", "ABS", "NK", "CCS", "RINA",
                ]),
                "call_sign": self.fake.bothify(text="??##?").upper(),
                "is_flagged": is_flagged,
                "flag_reason": (
                    np.random.choice([
                        "SANCTIONED_FLAG_STATE", "OWNERSHIP_LINK", "DARK_ACTIVITY",
                        "STS_TRANSFER", "PREVIOUS_SANCTIONS_VIOLATION",
                    ]) if is_flagged else None
                ),
                "last_inspection_date": self.fake.date_between(start_date="-2y", end_date="today"),
                "is_active": np.random.random() > 0.08,
                "source_system": np.random.choice(["LLOYD_LIST", "EQUASIS", "AIS_PROVIDER", "MANUAL"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)

    def _flag_state_probs(self):
        """Generate probability distribution for flag states."""
        n = len(self.FLAG_STATES)
        # Give higher weight to open-registry flags (Panama, Liberia, Marshall Islands)
        probs = np.ones(n)
        probs[0] = 5.0   # PA
        probs[1] = 4.0   # LR
        probs[2] = 4.0   # MH
        probs[3] = 3.0   # HK
        probs[4] = 3.0   # SG
        # Sanctioned flags get lower probability
        for j in range(n - 7, n):
            probs[j] = 0.5
        return probs / probs.sum()
