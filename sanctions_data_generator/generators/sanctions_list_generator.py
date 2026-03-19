"""
Sanctions list data generator.

Generates realistic sanctions list entries across OFAC, EU, UN, and UK
sanctions programs with entity types, aliases, and identification numbers.
"""

import json
from datetime import datetime

import numpy as np
import pandas as pd

from config import sanctions_config
from generators.base_generator import BaseGenerator


class SanctionsListGenerator(BaseGenerator):
    """Generate sanctions list entries mirroring OFAC/EU/UN/UK lists."""

    ENTITY_TYPES = ["INDIVIDUAL", "ENTITY", "VESSEL", "AIRCRAFT"]
    ENTITY_TYPE_WEIGHTS = [0.40, 0.40, 0.15, 0.05]

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of sanctions list entries."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i
            entity_type = np.random.choice(
                self.ENTITY_TYPES, p=self.ENTITY_TYPE_WEIGHTS
            )

            sanctions_list = np.random.choice(sanctions_config.SANCTIONS_LISTS)
            sanctions_program = np.random.choice(sanctions_config.SANCTIONS_PROGRAMS)

            # Generate appropriate entity name based on type
            if entity_type == "INDIVIDUAL":
                entity_name = self.fake.name()
            elif entity_type == "VESSEL":
                entity_name = f"M/V {self.fake.last_name().upper()} {np.random.choice(['STAR', 'GLORY', 'SPIRIT', 'PEARL', 'FORTUNE'])}"
            elif entity_type == "AIRCRAFT":
                entity_name = f"{self.fake.bothify(text='??-####').upper()}"
            else:
                entity_name = self.fake.company()

            nationality = np.random.choice(
                sanctions_config.HIGH_RISK_COUNTRIES
                + sanctions_config.ELEVATED_RISK_COUNTRIES
            )

            num_countries = np.random.choice([1, 2, 3], p=[0.60, 0.30, 0.10])
            country_codes = [nationality] + [
                np.random.choice(sanctions_config.HIGH_RISK_COUNTRIES)
                for _ in range(num_countries - 1)
            ]

            num_aliases = np.random.choice([0, 1, 2, 3, 4], p=[0.20, 0.30, 0.25, 0.15, 0.10])
            aliases = [
                self.fake.name() if entity_type == "INDIVIDUAL" else self.fake.company()
                for _ in range(num_aliases)
            ]

            num_ids = np.random.choice([0, 1, 2], p=[0.30, 0.50, 0.20])
            id_numbers = [
                {
                    "type": np.random.choice(["PASSPORT", "NATIONAL_ID", "TAX_ID", "REGISTRATION"]),
                    "number": self.fake.bothify(text="??########"),
                    "country": nationality,
                }
                for _ in range(num_ids)
            ]

            listed_date = self.fake.date_between(start_date="-15y", end_date="today")
            is_delisted = np.random.random() < 0.12
            delisted_date = (
                self.fake.date_between(start_date=listed_date, end_date="today")
                if is_delisted else None
            )

            record = {
                "entity_id": f"SE{idx:010d}",
                "entity_name": entity_name,
                "entity_type": entity_type,
                "sanctions_list": sanctions_list,
                "sanctions_program": sanctions_program,
                "nationality": nationality,
                "country_codes": json.dumps(list(set(country_codes))),
                "alias_names": json.dumps(aliases) if aliases else None,
                "identification_numbers": json.dumps(id_numbers) if id_numbers else None,
                "listed_date": listed_date,
                "delisted_date": delisted_date,
                "last_updated": self.fake.date_time_between(
                    start_date=listed_date, end_date="now"
                ),
                "remarks": self.fake.sentence(nb_words=15) if np.random.random() < 0.60 else None,
                "source_url": f"https://sanctions.example.com/{sanctions_list.lower()}/{idx}",
                "source_system": sanctions_list.split("_")[0],
                "_loaded_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
