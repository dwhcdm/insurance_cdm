"""
Screening result data generator.

Generates sanctions screening results linked to trades with match scores,
match types, dispositions, and SLA tracking.

Production-grade BAU issues injected:
    - False positive storms (sudden spike in matches for one entity)
    - SLA breaches (resolution_hours > SLA threshold for risk level)
    - Screening backlogs (PENDING_REVIEW piling up without assignment)
    - Orphan trade_id / counterparty_id references
    - Contradictory disposition (TRUE_POSITIVE with LOW risk level)
    - Duplicate screening results for the same trade
    - Stale match details (screening engine version mismatch)
    - Missing analyst assignment on resolved cases
"""

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator
from generators.data_quality_issues import (
    DataQualityIssueInjector,
    IssueInjectionRates,
    SanctionsScenarioGenerator,
)


class ScreeningResultGenerator(BaseGenerator):
    """Generate sanctions screening result records linked to trades."""

    MATCH_TYPES = [
        "EXACT_NAME",
        "FUZZY_NAME",
        "ALIAS_MATCH",
        "ADDRESS_MATCH",
        "ID_NUMBER_MATCH",
        "VESSEL_NAME",
        "VESSEL_IMO",
        "COUNTRY_MATCH",
    ]

    DISPOSITIONS = [
        "TRUE_POSITIVE",
        "FALSE_POSITIVE",
        "ESCALATED",
        "PENDING_REVIEW",
        "CLOSED_NO_ACTION",
        "SAR_FILED",
    ]

    # SLA thresholds by risk level (hours)
    SLA_THRESHOLDS = {
        "CRITICAL": 4,
        "HIGH": 24,
        "MEDIUM": 72,
        "LOW": 168,
    }

    def __init__(
        self,
        seed: int = 42,
        trade_ids: list = None,
        counterparty_ids: list = None,
        issue_rates: IssueInjectionRates | None = None,
    ):
        super().__init__(seed)
        self.trade_ids = trade_ids or [f"TR{i:012d}" for i in range(912_500_000)]
        self.counterparty_ids = counterparty_ids or [
            f"CP{i:010d}" for i in range(5_000_000)
        ]
        self.sanctioned_entity_ids = [f"SE{i:010d}" for i in range(250_000)]
        self.injector = DataQualityIssueInjector(issue_rates, seed)
        self.scenario_gen = SanctionsScenarioGenerator(seed)

    def generate_batch(
        self, batch_size: int, batch_offset: int = 0, **kwargs
    ) -> pd.DataFrame:
        """Generate a batch of screening result records with production-grade issues."""
        screening_date = kwargs.get("screening_date", datetime.now().date())

        records = []

        # False positive storm: one entity triggers 50-200 matches - 1% of batches
        fp_storm_entity = None
        fp_storm_trade = None
        if np.random.random() < 0.01:
            fp_storm_entity = np.random.choice(
                self.sanctioned_entity_ids[
                    : min(len(self.sanctioned_entity_ids), 50_000)
                ]
            )
            fp_storm_trade = np.random.choice(
                self.trade_ids[: min(len(self.trade_ids), 1_000_000)]
            )

        for i in range(batch_size):
            idx = batch_offset + i
            record_id = f"SCR{idx:012d}"
            trade_id = np.random.choice(
                self.trade_ids[: min(len(self.trade_ids), 1_000_000)]
            )

            # Orphan trade FK - 0.8%
            if np.random.random() < 0.008:
                trade_id = f"TR_ORPHAN_{np.random.randint(1, 99999):08d}"

            # Inject false positive storm records
            sanctioned_entity_id = np.random.choice(
                self.sanctioned_entity_ids[
                    : min(len(self.sanctioned_entity_ids), 50_000)
                ]
            )
            if fp_storm_entity and np.random.random() < 0.15:
                sanctioned_entity_id = fp_storm_entity
                trade_id = fp_storm_trade

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
                    1.0
                    - sanctions_config.EXACT_MATCH_PERCENTAGE
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

            # Contradictory: TRUE_POSITIVE with LOW risk - 0.3%
            if np.random.random() < 0.003 and risk_level == "LOW":
                pass  # will be set to TRUE_POSITIVE below via contradiction injection

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
                    resolution_hours = round(
                        np.random.lognormal(mean=1.5, sigma=0.5), 2
                    )
                resolution_ts = screening_ts + timedelta(hours=resolution_hours)

            # SLA breach injection: resolution exceeds SLA threshold - 3%
            sla_threshold = self.SLA_THRESHOLDS.get(risk_level, 168)
            is_sla_breach = False
            if resolution_hours and np.random.random() < 0.03:
                resolution_hours = round(sla_threshold * np.random.uniform(1.5, 5.0), 2)
                resolution_ts = screening_ts + timedelta(hours=resolution_hours)
                is_sla_breach = True

            # Backlog: PENDING_REVIEW without analyst for extended period - 2%
            backlog_hours = None
            if disposition == "PENDING_REVIEW" and np.random.random() < 0.02:
                backlog_hours = round(np.random.uniform(48, 336), 2)  # 2-14 days

            # Missing analyst on resolved case - 0.5%
            analyst_id = (
                f"ANL{np.random.randint(1, 200):04d}"
                if disposition != "PENDING_REVIEW"
                else None
            )
            if analyst_id and np.random.random() < 0.005:
                analyst_id = None

            match_details = json.dumps(
                {
                    "matched_fields": np.random.choice(
                        [
                            ["name"],
                            ["name", "country"],
                            ["name", "address"],
                            ["name", "id_number"],
                            ["vessel_name", "imo"],
                        ],
                    ).tolist(),
                    "confidence": match_score,
                    "screening_engine": np.random.choice(
                        ["FIRCO", "ACTIMIZE", "REFINITIV_WC1", "CUSTOM"]
                    ),
                }
            )

            record = {
                "screening_id": record_id,
                "trade_id": trade_id,
                "counterparty_id": np.random.choice(
                    self.counterparty_ids[: min(len(self.counterparty_ids), 100_000)]
                ),
                "sanctioned_entity_id": sanctioned_entity_id,
                "screening_timestamp": screening_ts,
                "screening_type": screening_type,
                "match_score": match_score,
                "match_type": np.random.choice(self.MATCH_TYPES),
                "match_details": match_details,
                "disposition": disposition,
                "risk_level": risk_level,
                "analyst_id": analyst_id,
                "analyst_notes": (
                    self.fake.sentence(nb_words=12)
                    if disposition != "PENDING_REVIEW"
                    else None
                ),
                "resolution_timestamp": resolution_ts,
                "resolution_hours": resolution_hours,
                "is_sla_breach": is_sla_breach,
                "backlog_hours": backlog_hours,
                "sanctions_list_matched": np.random.choice(
                    sanctions_config.SANCTIONS_LISTS
                ),
                "is_pep_match": np.random.random() < 0.08,
                "is_adverse_media": np.random.random() < 0.05,
                "workflow_id": f"WF{np.random.randint(1, 10000):06d}",
                "source_system": np.random.choice(
                    [
                        "FIRCO_CONTINUITY",
                        "ACTIMIZE_SAM",
                        "REFINITIV_WC1",
                        "CUSTOM_ENGINE",
                    ]
                ),
                "_loaded_at": datetime.now(),
            }

            # ---- Inject production-grade data quality issues ----
            record, near_dupe = self.injector.inject_all(
                record,
                record_id=record_id,
                nullable_fields=[
                    "analyst_id",
                    "analyst_notes",
                    "resolution_timestamp",
                    "resolution_hours",
                    "backlog_hours",
                ],
                text_fields=["analyst_notes", "match_details"],
                timestamp_field="screening_timestamp",
                loaded_at_field="_loaded_at",
                json_fields=["match_details"],
                positive_fields=["match_score", "resolution_hours"],
                bounded_fields={
                    "match_score": (0.0, 1.0),
                },
                enum_fields={
                    "disposition": ["AUTO_CLOSED", "SYSTEM_ERROR", "EXPIRED"],
                    "risk_level": ["UNKNOWN", "NOT_RATED"],
                    "screening_type": ["MANUAL_OVERRIDE", "BATCH_RESCREEN"],
                    "source_system": ["LEGACY_V1", "UNKNOWN_ENGINE", "MANUAL"],
                },
                mutable_fields=[
                    "trade_id",
                    "counterparty_id",
                    "sanctioned_entity_id",
                    "analyst_id",
                ],
                contradictions=[
                    # TRUE_POSITIVE but LOW risk
                    ("disposition", "TRUE_POSITIVE", "risk_level", "LOW"),
                    # SAR filed but match score is very low
                    ("disposition", "SAR_FILED", "match_score", 0.35),
                ],
            )

            records.append(record)

            exact_dupe = self.injector.maybe_create_exact_duplicate(record)
            if exact_dupe:
                records.append(exact_dupe)

            if near_dupe:
                records.append(near_dupe)

        return pd.DataFrame(records[:batch_size])
