"""
Screening result data generator.

Generates sanctions screening results linked to trades with match scores,
match types, dispositions, and SLA tracking.
"""

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class ScreeningResultGenerator(BaseGenerator):
    """Generate sanctions screening result records linked to trades."""

    MATCH_TYPES = [
        "EXACT_NAME", "FUZZY_NAME", "ALIAS_MATCH", "ADDRESS_MATCH",
        "ID_NUMBER_MATCH", "VESSEL_NAME", "VESSEL_IMO", "COUNTRY_MATCH",
    ]

    DISPOSITIONS = [
        "TRUE_POSITIVE", "FALSE_POSITIVE", "ESCALATED",
        "PENDING_REVIEW", "CLOSED_NO_ACTION", "SAR_FILED",
    ]

    def __init__(self, seed: int = 42, trade_ids: list = None, counterparty_ids: list = None):
        super().__init__(seed)
        self.trade_ids = trade_ids or [f"TR{i:012d}" for i in range(912_500_000)]
        self.counterparty_ids = counterparty_ids or [f"CP{i:010d}" for i in range(5_000_000)]
        self.sanctioned_entity_ids = [f"SE{i:010d}" for i in range(250_000)]

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of screening result records."""
        screening_date = kwargs.get("screening_date", datetime.now().date())

        records = []
        for i in range(batch_size):
            idx = batch_offset + i
            trade_id = np.random.choice(self.trade_ids[:min(len(self.trade_ids), 1_000_000)])

            screening_type = np.random.choice(
                ["PRE_TRADE", "POST_TRADE", "PERIODIC", "EVENT_DRIVEN"],
                p=[0.40, 0.30, 0.20, 0.10],
            )

            # Match score distribution
            match_category = np.random.choice(
                ["exact", "high", "medium", "low"],
                p=[
                    sanctions_config.EXACT_MATCH_PERCENTAGE,
                    sanctions_config.HIGH_MATCH_PERCENTAGE,
                    sanctions_config.MEDIUM_MATCH_PERCENTAGE,
                    1.0 - sanctions_config.EXACT_MATCH_PERCENTAGE
                    - sanctions_config.HIGH_MATCH_PERCENTAGE
                    - sanctions_config.MEDIUM_MATCH_PERCENTAGE,
                ],
            )

            if match_category == "exact":
                match_score = round(np.random.uniform(0.95, 1.00), 4)
            elif match_category == "high":
                match_score = round(np.random.uniform(0.85, 0.95), 4)
            elif match_category == "medium":
                match_score = round(np.random.uniform(0.65, 0.85), 4)
            else:
                match_score = round(np.random.uniform(0.30, 0.65), 4)

            # Risk level derived from match score
            if match_score >= 0.90:
                risk_level = "CRITICAL"
            elif match_score >= 0.80:
                risk_level = "HIGH"
            elif match_score >= 0.60:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            # Disposition correlates with match score
            if match_score >= 0.95:
                disposition = np.random.choice(
                    self.DISPOSITIONS,
                    p=[0.30, 0.15, 0.30, 0.10, 0.05, 0.10],
                )
            elif match_score >= 0.85:
                disposition = np.random.choice(
                    self.DISPOSITIONS,
                    p=[0.10, 0.25, 0.25, 0.20, 0.15, 0.05],
                )
            else:
                disposition = np.random.choice(
                    self.DISPOSITIONS,
                    p=[0.02, 0.50, 0.10, 0.18, 0.18, 0.02],
                )

            screening_ts = datetime.combine(
                screening_date, datetime.min.time()
            ) + timedelta(seconds=np.random.randint(0, 86400))

            # Resolution time varies by risk level
            if disposition in ("PENDING_REVIEW",):
                resolution_ts = None
                resolution_hours = None
            else:
                if risk_level == "CRITICAL":
                    resolution_hours = round(np.random.lognormal(mean=2, sigma=0.8), 2)
                elif risk_level == "HIGH":
                    resolution_hours = round(np.random.lognormal(mean=3, sigma=1.0), 2)
                else:
                    resolution_hours = round(np.random.lognormal(mean=1.5, sigma=0.5), 2)
                resolution_ts = screening_ts + timedelta(hours=resolution_hours)

            match_details = json.dumps({
                "matched_fields": np.random.choice(
                    [["name"], ["name", "country"], ["name", "address"],
                     ["name", "id_number"], ["vessel_name", "imo"]],
                ).tolist(),
                "confidence": match_score,
                "screening_engine": np.random.choice(
                    ["FIRCO", "ACTIMIZE", "REFINITIV_WC1", "CUSTOM"]
                ),
            })

            record = {
                "screening_id": f"SCR{idx:012d}",
                "trade_id": trade_id,
                "counterparty_id": np.random.choice(self.counterparty_ids[:min(len(self.counterparty_ids), 100_000)]),
                "sanctioned_entity_id": np.random.choice(self.sanctioned_entity_ids[:min(len(self.sanctioned_entity_ids), 50_000)]),
                "screening_timestamp": screening_ts,
                "screening_type": screening_type,
                "match_score": match_score,
                "match_type": np.random.choice(self.MATCH_TYPES),
                "match_details": match_details,
                "disposition": disposition,
                "risk_level": risk_level,
                "analyst_id": f"ANL{np.random.randint(1, 200):04d}" if disposition != "PENDING_REVIEW" else None,
                "analyst_notes": self.fake.sentence(nb_words=12) if disposition != "PENDING_REVIEW" else None,
                "resolution_timestamp": resolution_ts,
                "resolution_hours": resolution_hours,
                "sanctions_list_matched": np.random.choice(sanctions_config.SANCTIONS_LISTS),
                "is_pep_match": np.random.random() < 0.08,
                "is_adverse_media": np.random.random() < 0.05,
                "workflow_id": f"WF{np.random.randint(1, 10000):06d}",
                "source_system": np.random.choice(
                    ["FIRCO_CONTINUITY", "ACTIMIZE_SAM", "REFINITIV_WC1", "CUSTOM_ENGINE"]
                ),
                "_loaded_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
