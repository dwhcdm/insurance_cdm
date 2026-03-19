"""
Counterparty / KYC data generator.

Generates realistic counterparty records with entity types, risk ratings,
PEP flags, sanctions flags, and KYC review dates.

Production-grade BAU issues injected:
    - Expired KYC records (review date > 12 months old)
    - Shell company patterns (same address, rapid creation bursts)
    - Incomplete onboarding records (partial data from KYC portal)
    - Name variations across source systems (transliteration, abbreviation)
    - Orphan beneficial ownership chains
    - Contradictory risk flags (sanctioned entity with LOW risk)
    - Duplicate registrations from multiple source feeds
    - Unicode / encoding issues in entity names from international sources
    - Stale records that never completed KYC refresh
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
    NameVariationGenerator,
    SanctionsScenarioGenerator,
)


class CounterpartyGenerator(BaseGenerator):
    """Generate KYC counterparty records at scale with production-grade issues."""

    ENTITY_TYPES = ["CORPORATE", "INDIVIDUAL", "GOVERNMENT", "BANK", "TRADER"]
    ENTITY_TYPE_WEIGHTS = [0.45, 0.25, 0.05, 0.15, 0.10]

    INDUSTRY_SECTORS = [
        "OIL_AND_GAS", "SHIPPING", "MINING", "AGRICULTURE",
        "FINANCIAL_SERVICES", "PETROCHEMICALS", "METALS_TRADING",
        "POWER_UTILITIES", "GOVERNMENT", "MANUFACTURING",
    ]

    ALL_COUNTRIES = [
        "US", "GB", "DE", "FR", "JP", "CN", "SG", "AE", "CH", "NL",
        "NO", "BR", "IN", "KR", "AU", "CA", "SA", "QA", "KW", "OM",
        "MY", "TH", "ID", "RU", "TR", "ZA", "NG", "EG", "HK", "MT",
    ] + sanctions_config.HIGH_RISK_COUNTRIES

    # Shell company jurisdiction hotspots
    SHELL_JURISDICTIONS = ["CY", "PA", "VG", "BS", "BM", "KY", "JE", "GG", "MT"]

    # Shared registered agent addresses (shell company indicator)
    SHELL_ADDRESSES = [
        "Suite 401, Harbour Tower, Road Town",
        "Office 12B, Millennium Building, George Town",
        "3rd Floor, Citadel House, Victoria",
        "PO Box 309, Ugland House, George Town",
        "Trust Company Complex, Ajeltake Road",
    ]

    def __init__(self, seed: int = 42, issue_rates: IssueInjectionRates | None = None):
        super().__init__(seed)
        self.injector = DataQualityIssueInjector(issue_rates, seed)
        self.name_gen = NameVariationGenerator(seed)
        self.scenario_gen = SanctionsScenarioGenerator(seed)

        # Track recent creations for burst detection
        self._recent_burst_jurisdiction: str | None = None
        self._burst_remaining: int = 0
        self._burst_address: str | None = None

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of counterparty records with production-grade issues."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i
            record_id = f"CP{idx:010d}"
            country = np.random.choice(self.ALL_COUNTRIES)

            # -- Shell company burst pattern --------------------------
            if self._burst_remaining > 0:
                country = self._recent_burst_jurisdiction
                self._burst_remaining -= 1
            elif np.random.random() < 0.003:
                burst = self.scenario_gen.generate_rapid_entity_creation_burst()
                self._recent_burst_jurisdiction = burst["jurisdiction"]
                self._burst_remaining = burst["num_entities"] - 1
                self._burst_address = (
                    np.random.choice(self.SHELL_ADDRESSES)
                    if burst["same_address"] else None
                )
                country = self._recent_burst_jurisdiction

            # Higher risk for sanctioned countries
            if country in sanctions_config.HIGH_RISK_COUNTRIES:
                risk_rating = np.random.choice(
                    ["HIGH", "MEDIUM", "LOW"], p=[0.60, 0.30, 0.10]
                )
            elif country in sanctions_config.ELEVATED_RISK_COUNTRIES:
                risk_rating = np.random.choice(
                    ["HIGH", "MEDIUM", "LOW"], p=[0.20, 0.50, 0.30]
                )
            else:
                risk_rating = np.random.choice(
                    ["HIGH", "MEDIUM", "LOW"], p=[0.05, 0.25, 0.70]
                )

            entity_type = np.random.choice(
                self.ENTITY_TYPES, p=self.ENTITY_TYPE_WEIGHTS
            )

            num_aliases = np.random.choice([0, 1, 2, 3], p=[0.40, 0.30, 0.20, 0.10])
            aliases = [self.fake.company() for _ in range(num_aliases)] if num_aliases > 0 else []

            # -- Name variation injection -----------------------------
            if aliases and np.random.random() < 0.15:
                for a_idx in range(len(aliases)):
                    aliases[a_idx] = self.name_gen.generate_name_variant(aliases[a_idx])

            is_sanctioned = (
                country in sanctions_config.HIGH_RISK_COUNTRIES
                and np.random.random() < 0.03
            )

            reg_date = self.fake.date_between(start_date="-20y", end_date="-1y")
            kyc_date = self.fake.date_between(
                start_date=reg_date,
                end_date="today",
            )

            # -- Expired KYC: review date > 12 months old ------------
            if np.random.random() < 0.08:
                kyc_date = self.fake.date_between(
                    start_date="-5y",
                    end_date="-13m",
                )

            legal_name = (
                self.fake.company() if entity_type != "INDIVIDUAL"
                else self.fake.name()
            )

            # -- Name variation in legal_name for some records --------
            if np.random.random() < 0.05:
                legal_name = self.name_gen.generate_name_variant(legal_name)

            address = (
                self._burst_address
                if self._burst_address and self._burst_remaining > 0
                else self.fake.street_address()
            )

            record = {
                "counterparty_id": record_id,
                "legal_name": legal_name,
                "entity_type": entity_type,
                "country_of_incorporation": country,
                "country_of_domicile": (
                    country if np.random.random() < 0.80
                    else np.random.choice(self.ALL_COUNTRIES)
                ),
                "registration_number": (
                    self.fake.bothify(text="REG-####-????")
                    if entity_type != "INDIVIDUAL" else None
                ),
                "lei_code": (
                    self.fake.bothify(text="####00??????????##")
                    if entity_type in ("CORPORATE", "BANK") and np.random.random() < 0.70
                    else None
                ),
                "swift_bic": (
                    self.fake.bothify(text="????GB2L###")
                    if entity_type == "BANK" else None
                ),
                "tax_id": self.fake.bothify(text="??#########") if np.random.random() < 0.80 else None,
                "address_line_1": address,
                "address_line_2": self.fake.secondary_address() if np.random.random() < 0.30 else None,
                "city": self.fake.city(),
                "state_province": self.fake.state() if np.random.random() < 0.60 else None,
                "postal_code": self.fake.postcode(),
                "industry_sector": np.random.choice(self.INDUSTRY_SECTORS),
                "risk_rating": risk_rating,
                "is_pep": np.random.random() < 0.02,
                "is_sanctioned": is_sanctioned,
                "alias_names": json.dumps(aliases) if aliases else None,
                "registration_date": reg_date,
                "last_kyc_review_date": kyc_date,
                "source_system": np.random.choice(["KYC_PORTAL", "REFINITIV", "MANUAL", "API_FEED"]),
                "_loaded_at": datetime.now(),
            }

            # -- Inject data quality issues ---------------------------
            record, near_dupe = self.injector.inject_all(
                record,
                record_id=record_id,
                nullable_fields=[
                    "registration_number", "lei_code", "swift_bic", "tax_id",
                    "address_line_2", "state_province", "alias_names",
                    "city", "postal_code", "industry_sector",
                ],
                text_fields=[
                    "legal_name", "address_line_1", "city",
                    "state_province", "postal_code",
                ],
                json_fields=["alias_names"],
                enum_fields={
                    "entity_type": ["TRUST", "FOUNDATION", "SPV", "PARTNERSHIP", "NGO"],
                    "risk_rating": ["VERY_HIGH", "PROHIBITED", "UNRATED", "PENDING"],
                    "source_system": ["LEGACY_MAINFRAME", "UNKNOWN", "EXCEL_UPLOAD"],
                },
                mutable_fields=["legal_name", "address_line_1", "city", "country_of_domicile"],
                contradictions=[
                    ("is_sanctioned", True, "risk_rating", "LOW"),
                    ("is_pep", True, "last_kyc_review_date",
                     self.fake.date_between(start_date="-6y", end_date="-3y")),
                ],
            )

            records.append(record)

            exact_dupe = self.injector.maybe_create_exact_duplicate(record)
            if exact_dupe:
                records.append(exact_dupe)

            if near_dupe:
                records.append(near_dupe)

        return pd.DataFrame(records)
