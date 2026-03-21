"""
Sanctions list data generator.

Generates realistic sanctions list entries across OFAC, EU, UN, and UK
sanctions programs with entity types, aliases, and identification numbers.

Production-grade BAU issues injected:
    - Transliteration variants that cause false-positive/negative screening hits
    - Recently listed entities with propagation delay (listed but not yet in feed)
    - Recently delisted entities still appearing (stale list data)
    - Cross-list conflicts (entity on OFAC but not EU, or vice versa)
    - Alias explosion (20+ aliases for a single entity)
    - Missing identification numbers on high-priority entities
    - Duplicate entity listings across multiple programs
    - Name encoding issues from Arabic/Cyrillic/Chinese source systems
    - Partial list updates (only some fields changed)
    - Backdated listing corrections (listed_date changed retroactively)
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
    PROBLEMATIC_UNICODE,
)


class SanctionsListGenerator(BaseGenerator):
    """Generate sanctions list entries mirroring OFAC/EU/UN/UK lists with production issues."""

    ENTITY_TYPES = ["INDIVIDUAL", "ENTITY", "VESSEL", "AIRCRAFT"]
    ENTITY_TYPE_WEIGHTS = [0.40, 0.40, 0.15, 0.05]

    def __init__(self, seed: int = 42, issue_rates: IssueInjectionRates | None = None):
        super().__init__(seed)
        self.injector = DataQualityIssueInjector(issue_rates, seed)
        self.name_gen = NameVariationGenerator(seed)

    def generate_batch(
        self, batch_size: int, batch_offset: int = 0, **kwargs
    ) -> pd.DataFrame:
        """Generate a batch of sanctions list entries with production-grade issues."""
        records = []

        for i in range(batch_size):
            idx = batch_offset + i
            record_id = f"SE{idx:010d}"
            entity_type = np.random.choice(
                self.ENTITY_TYPES, p=self.ENTITY_TYPE_WEIGHTS
            )

            sanctions_list = np.random.choice(sanctions_config.SANCTIONS_LISTS)
            sanctions_program = np.random.choice(sanctions_config.SANCTIONS_PROGRAMS)

            # Generate appropriate entity name based on type
            if entity_type == "INDIVIDUAL":
                entity_name = self.fake.name()
                # Inject transliteration variants for ~20% of names
                if np.random.random() < 0.20:
                    entity_name = self.name_gen.generate_name_variant(entity_name)
            elif entity_type == "VESSEL":
                entity_name = f"M/V {self.fake.last_name().upper()} {np.random.choice(['STAR', 'GLORY', 'SPIRIT', 'PEARL', 'FORTUNE'])}"
            elif entity_type == "AIRCRAFT":
                entity_name = f"{self.fake.bothify(text='??-####').upper()}"
            else:
                entity_name = self.fake.company()

            # Native script names (Arabic/Cyrillic/Chinese)
            if np.random.random() < 0.08:
                script_type = np.random.choice(["arabic", "cyrillic", "chinese"])
                native_name = np.random.choice(PROBLEMATIC_UNICODE[script_type])
                entity_name = f"{entity_name} ({native_name})"

            nationality = np.random.choice(
                sanctions_config.HIGH_RISK_COUNTRIES
                + sanctions_config.ELEVATED_RISK_COUNTRIES
            )

            num_countries = np.random.choice([1, 2, 3], p=[0.60, 0.30, 0.10])
            country_codes = [nationality] + [
                np.random.choice(sanctions_config.HIGH_RISK_COUNTRIES)
                for _ in range(num_countries - 1)
            ]

            # Alias explosion: some entities have 10-30 aliases
            if np.random.random() < 0.03:
                num_aliases = np.random.randint(10, 30)
            else:
                num_aliases = np.random.choice(
                    [0, 1, 2, 3, 4], p=[0.20, 0.30, 0.25, 0.15, 0.10]
                )

            aliases = []
            for _ in range(num_aliases):
                if entity_type == "INDIVIDUAL":
                    alias = self.fake.name()
                    if np.random.random() < 0.30:
                        alias = self.name_gen.generate_name_variant(alias)
                else:
                    alias = self.fake.company()
                aliases.append(alias)

            num_ids = np.random.choice([0, 1, 2], p=[0.30, 0.50, 0.20])
            id_numbers = [
                {
                    "type": np.random.choice(
                        ["PASSPORT", "NATIONAL_ID", "TAX_ID", "REGISTRATION"]
                    ),
                    "number": self.fake.bothify(text="??########"),
                    "country": nationality,
                }
                for _ in range(num_ids)
            ]

            listed_date = self.fake.date_between(start_date="-15y", end_date="today")
            is_delisted = np.random.random() < 0.12

            # Delisting timing issues
            if is_delisted:
                delisted_date = self.fake.date_between(
                    start_date=listed_date, end_date="today"
                )
                if np.random.random() < 0.05:
                    delisted_date = self.fake.date_between(
                        start_date="today",
                        end_date="+90d",
                    )
            else:
                delisted_date = None

            # Recently listed with propagation delay
            last_updated_ts = self.fake.date_time_between(
                start_date=listed_date, end_date="now"
            )
            if np.random.random() < 0.04:
                last_updated_ts = datetime.now() - timedelta(
                    hours=np.random.randint(1, 72)
                )

            # Cross-list duplicate
            cross_list_duplicate = np.random.random() < 0.06

            record = {
                "entity_id": record_id,
                "entity_name": entity_name,
                "entity_type": entity_type,
                "sanctions_list": sanctions_list,
                "sanctions_program": sanctions_program,
                "nationality": nationality,
                "country_codes": json.dumps(list(set(country_codes))),
                "alias_names": json.dumps(aliases) if aliases else None,
                "identification_numbers": json.dumps(id_numbers)
                if id_numbers
                else None,
                "listed_date": listed_date,
                "delisted_date": delisted_date,
                "last_updated": last_updated_ts,
                "remarks": self.fake.sentence(nb_words=15)
                if np.random.random() < 0.60
                else None,
                "source_url": f"https://sanctions.example.com/{sanctions_list.lower()}/{idx}",
                "source_system": sanctions_list.split("_")[0],
                "_loaded_at": datetime.now(),
            }

            # Inject data quality issues
            record, near_dupe = self.injector.inject_all(
                record,
                record_id=record_id,
                nullable_fields=[
                    "nationality",
                    "alias_names",
                    "identification_numbers",
                    "remarks",
                    "source_url",
                    "delisted_date",
                    "country_codes",
                ],
                text_fields=["entity_name", "remarks", "nationality"],
                json_fields=["alias_names", "identification_numbers", "country_codes"],
                enum_fields={
                    "entity_type": [
                        "ORGANIZATION",
                        "SHIPPING_COMPANY",
                        "FRONT_COMPANY",
                    ],
                    "sanctions_list": ["OFAC_CAPTA", "EU_EMBARGO", "CUSTOM_INTERNAL"],
                },
                mutable_fields=["entity_name", "nationality", "sanctions_program"],
                contradictions=[
                    ("delisted_date", delisted_date, "source_system", "STALE_FEED"),
                ],
            )

            records.append(record)

            # Cross-list duplicate (slightly different entry)
            if cross_list_duplicate:
                dupe = record.copy()
                other_list = np.random.choice(
                    [
                        sl
                        for sl in sanctions_config.SANCTIONS_LISTS
                        if sl != sanctions_list
                    ]
                )
                dupe["entity_id"] = f"SE{idx:010d}_DUP"
                dupe["sanctions_list"] = other_list
                dupe["source_system"] = other_list.split("_")[0]
                dupe["entity_name"] = self.name_gen.generate_name_variant(
                    record["entity_name"]
                )
                records.append(dupe)

            exact_dupe = self.injector.maybe_create_exact_duplicate(record)
            if exact_dupe:
                records.append(exact_dupe)

            if near_dupe:
                records.append(near_dupe)

        return pd.DataFrame(records)
