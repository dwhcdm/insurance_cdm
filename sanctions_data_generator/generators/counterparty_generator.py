"""
Counterparty/entity data generator.

Generates realistic KYC counterparty records including corporate entities,
individuals, banks, traders, and government entities with appropriate
risk ratings and PEP flags.
"""

import json
import random
from datetime import datetime

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class CounterpartyGenerator(BaseGenerator):
    """Generate counterparty/entity data for KYC systems."""

    ENTITY_TYPES = ["CORPORATE", "INDIVIDUAL", "GOVERNMENT", "BANK", "TRADER"]
    SECTORS = [
        "OIL_GAS", "SHIPPING", "MINING", "AGRICULTURE", "CHEMICALS",
        "BANKING", "INSURANCE", "TRADING", "MANUFACTURING", "GOVERNMENT",
        "DEFENCE", "TECHNOLOGY", "PHARMACEUTICALS", "CONSTRUCTION",
    ]
    KYC_STATUSES = ["ACTIVE", "EXPIRED", "PENDING", "SUSPENDED"]

    ALL_COUNTRIES = [
        "US", "GB", "DE", "FR", "JP", "CN", "SG", "AE", "CH", "NL",
        "NO", "BR", "IN", "KR", "AU", "CA", "SA", "QA", "KW", "OM",
        "MY", "TH", "ID", "PH", "VN", "TR", "ZA", "NG", "EG", "KE",
        "MX", "CO", "AR", "CL", "PE",
    ] + sanctions_config.HIGH_RISK_COUNTRIES + sanctions_config.ELEVATED_RISK_COUNTRIES

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of counterparty records."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i

            entity_type = np.random.choice(
                self.ENTITY_TYPES,
                p=[0.50, 0.15, 0.05, 0.15, 0.15],
            )

            country = np.random.choice(self.ALL_COUNTRIES)
            is_high_risk = country in sanctions_config.HIGH_RISK_COUNTRIES

            # Risk rating distribution - higher for sanctioned countries
            if is_high_risk:
                risk_rating = np.random.choice(
                    ["HIGH", "MEDIUM", "LOW"], p=[0.60, 0.30, 0.10]
                )
            else:
                risk_rating = np.random.choice(
                    ["HIGH", "MEDIUM", "LOW"], p=[0.10, 0.30, 0.60]
                )

            is_pep = np.random.random() < (0.15 if is_high_risk else 0.03)

            # Alias names (JSON array)
            num_aliases = np.random.choice([0, 1, 2, 3], p=[0.40, 0.30, 0.20, 0.10])
            alias_names = json.dumps(
                [self.fake.company()[:50] for _ in range(num_aliases)]
            ) if num_aliases > 0 else None

            registration_date = self.fake.date_between(start_date="-20y", end_date="-1y")
            last_kyc_date = self.fake.date_between(start_date="-3y", end_date="today")

            record = {
                "counterparty_id": f"CP{idx:010d}",
                "legal_name": (
                    f"{self.fake.company()} {np.random.choice(['Ltd', 'LLC', 'Inc', 'Corp', 'GmbH', 'SA', 'Pte Ltd', 'BV'])}"
                    if entity_type != "INDIVIDUAL"
                    else f"{self.fake.last_name().upper()}, {self.fake.first_name()}"
                ),
                "short_name": self.fake.company_suffix() + " " + self.fake.lexify(text="???").upper(),
                "entity_type": entity_type,
                "country_of_incorporation": country,
                "country_of_domicile": country if np.random.random() > 0.15 else np.random.choice(self.ALL_COUNTRIES),
                "sector": np.random.choice(self.SECTORS),
                "is_pep": is_pep,
                "pep_level": np.random.choice(["DIRECT", "FAMILY", "ASSOCIATE"]) if is_pep else None,
                "risk_rating": risk_rating,
                "registration_number": self.fake.bothify(text="REG-########"),
                "lei_code": self.fake.bothify(text="####00??????????##") if entity_type in ("CORPORATE", "BANK") else None,
                "swift_bic": self.fake.bothify(text="????GB2L").upper() if entity_type == "BANK" else None,
                "alias_names": alias_names,
                "registration_date": registration_date,
                "last_kyc_date": last_kyc_date,
                "next_kyc_due_date": self.fake.date_between(start_date="today", end_date="+2y"),
                "kyc_status": np.random.choice(self.KYC_STATUSES, p=[0.70, 0.10, 0.10, 0.10]),
                "is_active": np.random.random() > 0.05,
                "source_system": np.random.choice(["KYC_SYSTEM", "CRM", "ONBOARDING", "MANUAL"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
