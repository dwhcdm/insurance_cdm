"""
Screening result data generator.

Generates sanctions screening results linked to trades with realistic
match score distributions, disposition outcomes, and SLA tracking.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class ScreeningResultGenerator(BaseGenerator):
    """Generate sanctions screening results tied to trade transactions."""

    MATCH_TYPES = [
        "NAME_MATCH", "FUZZY_NAME", "ID_MATCH", "ADDRESS_MATCH",
        "VESSEL_MATCH", "COUNTRY_MATCH", "PEP_MATCH", "ADVERSE_MEDIA",
    ]

    DISPOSITIONS = [
        "TRUE_POSITIVE", "FALSE_POSITIVE", "ESCALATED",
        "PENDING_REVIEW", "CLOSED_NO_ACTION", "SAR_FILED",
    ]

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate screening results linked to trades."""
        trade_ids = kwargs.get(
            "trade_ids",
            [f"TR{i:012d}" for i in range(batch_size)],
        )
        counterparty_ids = kwargs.get(
            "counterparty_ids",
            [f"CP{np.random.randint(0, 5_000_000):010d}" for _ in range(batch_size)],
        )
        sanctioned_entity_ids = kwargs.get(
            "sanctioned_entity_ids",
            [f"SE{np.random.randint(0, 250_000):010d}" for _ in range(batch_size)],
        )

        records = []

        for i in range(batch_size):
            idx = batch_offset + i

            # Match score distribution (realistic - most are low/false positives)
            match_score_type = np.random.choice(
                ["EXACT", "HIGH", "MEDIUM", "LOW"],
                p=[
                    sanctions_config.EXACT_MATCH_PERCENTAGE,
                    sanctions_config.HIGH_MATCH_PERCENTAGE,
                    sanctions_config.MEDIUM_MATCH_PERCENTAGE,
                    1 - sanctions_config.EXACT_MATCH_PERCENTAGE
                    - sanctions_config.HIGH_MATCH_PERCENTAGE
                    - sanctions_config.MEDIUM_MATCH_PERCENTAGE,
                ],
            )

            if match_score_type == "EXACT":
                match_score = np.random.uniform(0.95, 1.0)
                disposition = np.random.choice(
                    ["TRUE_POSITIVE", "ESCALATED", "SAR_FILED"],
                    p=[0.4, 0.4, 0.2],
                )
            elif match_score_type == "HIGH":
                match_score = np.random.uniform(0.85, 0.95)
                disposition = np.random.choice(
                    ["TRUE_POSITIVE", "FALSE_POSITIVE", "ESCALATED", "PENDING_REVIEW"],
                    p=[0.2, 0.3, 0.3, 0.2],
                )
            elif match_score_type == "MEDIUM":
                match_score = np.random.uniform(0.65, 0.85)
                disposition = np.random.choice(
                    ["FALSE_POSITIVE", "PENDING_REVIEW", "CLOSED_NO_ACTION"],
                    p=[0.5, 0.3, 0.2],
                )
            else:
                match_score = np.random.uniform(0.30, 0.65)
                disposition = np.random.choice(
                    ["FALSE_POSITIVE", "CLOSED_NO_ACTION"],
                    p=[0.7, 0.3],
                )

            screening_timestamp = datetime.now() - timedelta(
                days=np.random.randint(0, 365),
                hours=np.random.randint(0, 24),
            )

            # Resolution time depends on disposition
            if disposition in ("TRUE_POSITIVE", "SAR_FILED"):
                resolution_hours = np.random.randint(24, 168)
            elif disposition in ("ESCALATED", "PENDING_REVIEW"):
                resolution_hours = np.random.randint(4, 72)
            else:
                resolution_hours = np.random.randint(1, 24)

            resolution_timestamp = (
                screening_timestamp + timedelta(hours=resolution_hours)
                if disposition != "PENDING_REVIEW" else None
            )

            record = {
                "screening_id": f"SCR{idx:012d}",
                "trade_id": trade_ids[i % len(trade_ids)],
                "counterparty_id": counterparty_ids[i % len(counterparty_ids)],
                "sanctioned_entity_id": sanctioned_entity_ids[i % len(sanctioned_entity_ids)],
                "screening_timestamp": screening_timestamp,
                "screening_type": np.random.choice(
                    ["PRE_TRADE", "POST_TRADE", "PERIODIC", "EVENT_DRIVEN", "RETROSPECTIVE"],
                    p=[0.35, 0.30, 0.15, 0.10, 0.10],
                ),
                "match_score": round(match_score, 4),
                "match_type": np.random.choice(self.MATCH_TYPES),
                "match_details": str({
                    "matched_field": np.random.choice(["name", "alias", "id_number", "address", "vessel_name"]),
                    "source_value": f"source_val_{idx}",
                    "matched_value": f"matched_val_{idx}",
                    "algorithm": np.random.choice(["JARO_WINKLER", "LEVENSHTEIN", "SOUNDEX", "EXACT", "COSINE"]),
                }),
                "disposition": disposition,
                "risk_level": "HIGH" if match_score >= 0.85 else ("MEDIUM" if match_score >= 0.65 else "LOW"),
                "analyst_id": (
                    f"ANALYST_{np.random.randint(1, 50):03d}"
                    if disposition != "CLOSED_NO_ACTION" else None
                ),
                "analyst_notes": (
                    self.fake.sentence(nb_words=20)
                    if disposition in ("TRUE_POSITIVE", "ESCALATED", "SAR_FILED") else None
                ),
                "resolution_timestamp": resolution_timestamp,
                "resolution_hours": resolution_hours if resolution_timestamp else None,
                "sanctions_list_matched": np.random.choice(sanctions_config.SANCTIONS_LISTS),
                "is_pep_match": np.random.random() < 0.15,
                "is_adverse_media": np.random.random() < 0.10,
                "workflow_id": f"WF{np.random.randint(100000, 999999)}",
                "source_system": np.random.choice(["FIRCOSOFT", "ACTIMIZE", "NORKOM", "CUSTOM_ENGINE"]),
                "created_at": screening_timestamp,
                "updated_at": resolution_timestamp or screening_timestamp,
            }
            records.append(record)

        return pd.DataFrame(records)
