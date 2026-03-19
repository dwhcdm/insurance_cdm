"""
Counterparty / KYC data generator.

Generates realistic counterparty records with entity types, risk ratings,
PEP flags, sanctions flags, and KYC review dates.
"""

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class CounterpartyGenerator(BaseGenerator):
    """Generate KYC counterparty records at scale."""

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

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of counterparty records."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i
            country = np.random.choice(self.ALL_COUNTRIES)

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

            is_sanctioned = (
                country in sanctions_config.HIGH_RISK_COUNTRIES
                and np.random.random() < 0.03
            )

            reg_date = self.fake.date_between(start_date="-20y", end_date="-1y")
            kyc_date = self.fake.date_between(
                start_date=reg_date,
                end_date="today",
            )

            record = {
                "counterparty_id": f"CP{idx:010d}",
                "legal_name": (
                    self.fake.company() if entity_type != "INDIVIDUAL"
                    else self.fake.name()
                ),
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
                "address_line_1": self.fake.street_address(),
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
            records.append(record)

        return pd.DataFrame(records)
