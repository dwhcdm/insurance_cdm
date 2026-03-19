"""
Sanctions list data generator.

Generates realistic sanctions list entries from OFAC SDN, EU, UN, and UK
sanctions lists with appropriate entity types, programs, aliases, and
identification numbers.
"""

import json
import random
from datetime import datetime

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class SanctionsListGenerator(BaseGenerator):
    """Generate sanctions list entries mimicking OFAC/EU/UN/UK lists."""

    ENTITY_TYPES = ["INDIVIDUAL", "ENTITY", "VESSEL", "AIRCRAFT"]

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of sanctions list records."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i

            sanctions_list = np.random.choice(
                sanctions_config.SANCTIONS_LISTS,
                p=[0.25, 0.10, 0.20, 0.15, 0.10, 0.08, 0.07, 0.05],
            )
            sanctions_program = np.random.choice(sanctions_config.SANCTIONS_PROGRAMS)
            entity_type = np.random.choice(
                self.ENTITY_TYPES, p=[0.45, 0.40, 0.10, 0.05]
            )

            # Country codes (JSON array)
            num_countries = np.random.choice([1, 2, 3], p=[0.60, 0.30, 0.10])
            country_codes = json.dumps(
                list(np.random.choice(
                    sanctions_config.HIGH_RISK_COUNTRIES,
                    size=min(num_countries, len(sanctions_config.HIGH_RISK_COUNTRIES)),
                    replace=False,
                ))
            )

            # Alias names (JSON array)
            num_aliases = np.random.choice([0, 1, 2, 3, 4], p=[0.20, 0.25, 0.25, 0.20, 0.10])
            alias_names = json.dumps(
                [self.fake.name() if entity_type == "INDIVIDUAL" else self.fake.company()
                 for _ in range(num_aliases)]
            ) if num_aliases > 0 else None

            # Identification numbers (JSON array)
            num_ids = np.random.choice([0, 1, 2], p=[0.30, 0.50, 0.20])
            identification_numbers = json.dumps(
                [{"type": np.random.choice(["PASSPORT", "NATIONAL_ID", "TAX_ID", "REGISTRATION"]),
                  "number": self.fake.bothify(text="??########"),
                  "country": np.random.choice(sanctions_config.HIGH_RISK_COUNTRIES)}
                 for _ in range(num_ids)]
            ) if num_ids > 0 else None

            listed_date = self.fake.date_between(start_date="-15y", end_date="today")
            is_active = np.random.random() > 0.12  # 12% delisted
            delisted_date = (
                self.fake.date_between(start_date=listed_date, end_date="today")
                if not is_active else None
            )

            record = {
                "entity_id": f"SE{idx:010d}",
                "entity_name": (
                    f"{self.fake.last_name().upper()}, {self.fake.first_name()}"
                    if entity_type == "INDIVIDUAL"
                    else f"{self.fake.company()} {np.random.choice(['Trading', 'Shipping', 'Holdings', 'Group', 'Industries', 'Corp'])}"
                ),
                "entity_type": entity_type,
                "sanctions_list": sanctions_list,
                "sanctions_program": sanctions_program,
                "listed_date": listed_date,
                "delisted_date": delisted_date,
                "country_codes": country_codes,
                "alias_names": alias_names,
                "identification_numbers": identification_numbers,
                "date_of_birth": (
                    str(self.fake.date_of_birth(minimum_age=25, maximum_age=80))
                    if entity_type == "INDIVIDUAL" else None
                ),
                "nationality": np.random.choice(sanctions_config.HIGH_RISK_COUNTRIES),
                "remarks": self.fake.sentence(nb_words=15) if np.random.random() < 0.60 else None,
                "is_active": is_active,
                "source_url": f"https://sanctions.example.com/{sanctions_list.lower()}/{idx}",
                "last_updated": self.fake.date_time_between(start_date="-30d", end_date="now"),
                "created_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
