"""
Production-grade data quality issue injector.

Simulates the full spectrum of real-world BAU data problems encountered in
enterprise commodity trading and sanctions compliance environments:

    1. DATA QUALITY DEFECTS — nulls, truncation, encoding, type mismatches
    2. DUPLICATE & NEAR-DUPLICATE RECORDS — exact copies, partial dupes
    3. TEMPORAL ANOMALIES — late arrivals, out-of-order, future-dated, timezone drift
    4. REFERENTIAL INTEGRITY BREAKS — orphan FKs, dangling references
    5. FORMAT & ENCODING ISSUES — Unicode, mixed case, extra whitespace, special chars
    6. VOLUME ANOMALIES — spikes, gaps, seasonal patterns
    7. SCHEMA DRIFT — unexpected columns, type changes, new enum values
    8. BUSINESS LOGIC VIOLATIONS — impossible values, contradictory flags

Each issue category is controlled by configurable injection rates so environments
can be tuned from "clean dev" to "realistic prod chaos".
"""

import copy
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np


# ---------------------------------------------------------------------------
# Issue rate configuration
# ---------------------------------------------------------------------------

@dataclass
class IssueInjectionRates:
    """
    Configurable injection rates for every category of data issue.

    Rates are expressed as probabilities (0.0 – 1.0).  Typical production
    environments see 2-5 % of records affected by at least one issue;
    the defaults below target ~3 % aggregate issue rate.
    """

    # ── Data quality defects ──────────────────────────────────────────────
    null_injection_rate: float = 0.015          # Random NULL in non-PK field
    partial_record_rate: float = 0.008          # Multiple fields blanked out
    truncated_field_rate: float = 0.005         # Field value chopped mid-string
    wrong_type_rate: float = 0.003              # e.g. string in numeric column

    # ── Duplicates ────────────────────────────────────────────────────────
    exact_duplicate_rate: float = 0.005         # Identical row (idempotency fail)
    near_duplicate_rate: float = 0.008          # Same entity, minor differences

    # ── Temporal anomalies ────────────────────────────────────────────────
    late_arrival_rate: float = 0.020            # Record arrives hours/days late
    out_of_order_rate: float = 0.015            # Timestamp earlier than prior row
    future_dated_rate: float = 0.002            # Timestamp in the future
    timezone_drift_rate: float = 0.010          # ±N hours from expected TZ
    stale_record_rate: float = 0.005            # _loaded_at far older than data

    # ── Referential integrity ─────────────────────────────────────────────
    orphan_fk_rate: float = 0.010               # FK points to non-existent parent
    self_reference_rate: float = 0.003          # buyer_id == seller_id etc.

    # ── Format / encoding ─────────────────────────────────────────────────
    unicode_injection_rate: float = 0.012       # Non-Latin chars, diacritics
    extra_whitespace_rate: float = 0.020        # Leading/trailing/double spaces
    mixed_case_rate: float = 0.015              # Case inconsistency
    special_char_rate: float = 0.008            # HTML entities, control chars
    encoding_corruption_rate: float = 0.004     # Mojibake / replacement chars

    # ── Volume anomalies (applied at batch level, not per-record) ────────
    volume_spike_probability: float = 0.03      # Batch is 3-10× normal size
    volume_gap_probability: float = 0.02        # Batch is 0-20 % of normal
    empty_batch_probability: float = 0.005      # Zero records delivered

    # ── Schema / structural ───────────────────────────────────────────────
    unexpected_column_rate: float = 0.003       # Extra column appears
    enum_drift_rate: float = 0.005              # Unknown enum value injected
    json_malformed_rate: float = 0.006          # Broken JSON in JSON columns

    # ── Business logic violations ─────────────────────────────────────────
    negative_value_rate: float = 0.004          # Negative where only +ve expected
    impossible_value_rate: float = 0.003        # e.g. lat > 90, score > 1.0
    contradictory_flag_rate: float = 0.005      # is_sanctioned but risk_rating=LOW

    @classmethod
    def clean(cls) -> "IssueInjectionRates":
        """All rates set to zero — pristine data."""
        fields = cls.__dataclass_fields__
        return cls(**{k: 0.0 for k in fields})

    @classmethod
    def dev(cls) -> "IssueInjectionRates":
        """Light issue injection for development (≈1 % affected)."""
        fields = cls.__dataclass_fields__
        defaults = cls()
        return cls(**{k: getattr(defaults, k) * 0.3 for k in fields})

    @classmethod
    def test(cls) -> "IssueInjectionRates":
        """Moderate injection for integration/QA testing (≈3 %)."""
        return cls()  # defaults

    @classmethod
    def prod_realistic(cls) -> "IssueInjectionRates":
        """Full production noise (≈5 % of records affected)."""
        fields = cls.__dataclass_fields__
        defaults = cls()
        return cls(**{k: min(getattr(defaults, k) * 1.8, 1.0) for k in fields})

    @classmethod
    def stress(cls) -> "IssueInjectionRates":
        """Extreme injection for resilience/chaos testing (≈15 %)."""
        fields = cls.__dataclass_fields__
        defaults = cls()
        return cls(**{k: min(getattr(defaults, k) * 5.0, 1.0) for k in fields})


# ---------------------------------------------------------------------------
# Transliteration & name variation tables (real-world sanctions screening)
# ---------------------------------------------------------------------------

# Arabic/Persian/Russian name transliteration variants that cause real
# screening false-positives/false-negatives in production
TRANSLITERATION_VARIANTS = {
    "Mohammed": ["Mohammad", "Muhammad", "Muhammed", "Mohamed", "Mohamad",
                 "Mahomed", "Muhamad", "Mohamud", "Muḥammad"],
    "Ahmed": ["Ahmad", "Ahmet", "Achmed", "Akhmed", "Achmad"],
    "Hussein": ["Husain", "Hussain", "Husein", "Hossein", "Hüseyin"],
    "Ali": ["Aly", "Alee", "Aalee", "'Alī"],
    "Abdul": ["Abdel", "Abdal", "Abdoul", "Abdool", "'Abd al-"],
    "Omar": ["Umar", "'Umar", "Omer", "Ömer"],
    "Hassan": ["Hasan", "Hasen", "Hacen", "Ḥasan"],
    "Ibrahim": ["Ibraheem", "Ebrahim", "Abrahim", "İbrahim"],
    "Nikolai": ["Nikolay", "Nicolai", "Nicolay", "Mykola"],
    "Dmitri": ["Dmitry", "Dmitrii", "Dmytro", "Dimitri"],
    "Sergei": ["Sergey", "Serhiy", "Siarhei", "Serguei"],
    "Vladimir": ["Volodymyr", "Uladzimir", "Wladimir"],
    "Aleksandr": ["Alexander", "Oleksandr", "Aliaksandr", "Iskander"],
    "Yevgeny": ["Yevgeniy", "Evgeny", "Evgeniy", "Yevhen"],
    "Andrei": ["Andrey", "Andriy", "Andrii", "Andrzej"],
    "Kim": ["Gim", "Ghim"],
    "Park": ["Pak", "Bak"],
    "Lee": ["Li", "Yi", "Rhee"],
    "Wang": ["Wong", "Huang", "Ong"],
    "Zhang": ["Chang", "Cheung", "Teo"],
}

# Common company name variations that cause screening noise
COMPANY_NAME_VARIATIONS = {
    "Trading": ["Trading Co", "Trading Corp", "Trading LLC", "Trading Ltd",
                "Trading Company", "Trdg", "Trading Establishment"],
    "Shipping": ["Shipping Co", "Shipping LLC", "Shipping Lines",
                 "Maritime", "Marine", "Shipmanagement"],
    "Oil": ["Oil & Gas", "Petroleum", "Petrol", "Energy", "Fuel"],
    "International": ["Int'l", "Intl", "Int.", "Internat."],
    "General": ["Gen.", "Gen", "Genl"],
    "Corporation": ["Corp", "Corp.", "Corpn"],
    "Limited": ["Ltd", "Ltd.", "Limitada", "Ltda"],
    "Company": ["Co", "Co.", "Cie", "Cia"],
    "Brothers": ["Bros", "Bros.", "Brethren"],
}

# Unicode characters that cause real screening headaches
PROBLEMATIC_UNICODE = {
    "arabic": ["محمد", "أحمد", "شركة", "تجارة"],
    "cyrillic": ["Владимир", "Газпром", "Роснефть", "Совкомфлот"],
    "chinese": ["中国石油", "中国石化", "华为", "中远海运"],
    "mixed_script": ["Société Générale", "Ørsted", "Škoda", "Ünlü",
                     "São Paulo", "José García", "François Müller"],
    "diacritics": ["café", "naïve", "résumé", "über", "ñoño",
                   "Zürich", "Ångström", "Dvořák", "Łódź"],
    "zero_width": ["\u200b", "\u200c", "\u200d", "\ufeff"],  # ZWS, ZWNJ, ZWJ, BOM
    "homoglyphs": [  # Characters that LOOK like Latin but aren't
        ("а", "a"),  # Cyrillic а vs Latin a
        ("е", "e"),  # Cyrillic е vs Latin e
        ("о", "o"),  # Cyrillic о vs Latin o
        ("р", "p"),  # Cyrillic р vs Latin p
        ("с", "c"),  # Cyrillic с vs Latin c
    ],
}


# ---------------------------------------------------------------------------
# Core issue injector class
# ---------------------------------------------------------------------------

class DataQualityIssueInjector:
    """
    Injects realistic production-grade data quality issues into generated records.

    Usage:
        injector = DataQualityIssueInjector(IssueInjectionRates.prod_realistic())
        record = injector.inject_issues(record, domain="counterparty")
    """

    def __init__(self, rates: IssueInjectionRates | None = None, seed: int = 42):
        self.rates = rates or IssueInjectionRates()
        self.rng = np.random.RandomState(seed)
        random.seed(seed)
        self._issue_log: list[dict] = []

    @property
    def issue_log(self) -> list[dict]:
        """Returns a log of all injected issues for audit / validation."""
        return self._issue_log

    def _log_issue(self, record_id: str, issue_type: str, field: str,
                   original_value: Any, corrupted_value: Any) -> None:
        self._issue_log.append({
            "record_id": record_id,
            "issue_type": issue_type,
            "field": field,
            "original_value": str(original_value)[:200],
            "corrupted_value": str(corrupted_value)[:200],
            "injected_at": datetime.now().isoformat(),
        })

    def _should_inject(self, rate: float) -> bool:
        return self.rng.random() < rate

    # ─── NULL / missing field injection ───────────────────────────────────

    def inject_null(self, record: dict, nullable_fields: list[str],
                    record_id: str = "") -> dict:
        """Randomly NULL out one or more fields."""
        if self._should_inject(self.rates.null_injection_rate):
            field = random.choice(nullable_fields)
            original = record.get(field)
            record[field] = None
            self._log_issue(record_id, "NULL_INJECTION", field, original, None)

        if self._should_inject(self.rates.partial_record_rate):
            # Blank out 3-6 fields at once (partial record from source)
            num_fields = min(random.randint(3, 6), len(nullable_fields))
            fields_to_blank = random.sample(nullable_fields, num_fields)
            for field in fields_to_blank:
                original = record.get(field)
                record[field] = None
                self._log_issue(record_id, "PARTIAL_RECORD", field, original, None)

        return record

    # ─── Duplicate injection ──────────────────────────────────────────────

    def maybe_create_exact_duplicate(self, record: dict) -> dict | None:
        """Returns a copy of the record if exact duplicate should be injected."""
        if self._should_inject(self.rates.exact_duplicate_rate):
            return copy.deepcopy(record)
        return None

    def maybe_create_near_duplicate(self, record: dict,
                                     mutable_fields: list[str],
                                     record_id: str = "") -> dict | None:
        """
        Returns a near-duplicate: same entity, minor differences.
        Simulates re-keying, source system sync issues, CDC replays.
        """
        if not self._should_inject(self.rates.near_duplicate_rate):
            return None

        dupe = copy.deepcopy(record)
        # Modify 1-2 fields slightly
        num_changes = random.randint(1, 2)
        for field in random.sample(mutable_fields, min(num_changes, len(mutable_fields))):
            original = dupe.get(field)
            if isinstance(original, str) and len(original) > 2:
                # Introduce a typo or case change
                if random.random() < 0.5:
                    dupe[field] = original.upper() if original.islower() else original.lower()
                else:
                    pos = random.randint(0, len(original) - 1)
                    dupe[field] = original[:pos] + original[pos + 1:]  # Delete a char
            self._log_issue(record_id, "NEAR_DUPLICATE", field, original, dupe.get(field))

        return dupe

    # ─── Temporal anomalies ───────────────────────────────────────────────

    def inject_temporal_issues(self, record: dict,
                                timestamp_field: str,
                                loaded_at_field: str = "_loaded_at",
                                record_id: str = "") -> dict:
        """Inject late arrivals, future dates, timezone drift."""
        ts = record.get(timestamp_field)
        if ts is None:
            return record

        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except (ValueError, TypeError):
                return record

        if self._should_inject(self.rates.late_arrival_rate):
            # Record arrives 2-72 hours after its actual timestamp
            delay = timedelta(hours=random.uniform(2, 72))
            record[loaded_at_field] = ts + delay
            self._log_issue(record_id, "LATE_ARRIVAL", loaded_at_field,
                            ts, record[loaded_at_field])

        if self._should_inject(self.rates.out_of_order_rate):
            # Timestamp shifted backward (out-of-sequence)
            shift = timedelta(minutes=random.uniform(1, 120))
            record[timestamp_field] = ts - shift
            self._log_issue(record_id, "OUT_OF_ORDER", timestamp_field,
                            ts, record[timestamp_field])

        if self._should_inject(self.rates.future_dated_rate):
            # Timestamp in the future (clock skew, bad TZ conversion)
            future = datetime.now() + timedelta(hours=random.uniform(1, 48))
            record[timestamp_field] = future
            self._log_issue(record_id, "FUTURE_DATED", timestamp_field,
                            ts, future)

        if self._should_inject(self.rates.timezone_drift_rate):
            # Off by N hours (common with global source systems)
            drift_hours = random.choice([-8, -5, -1, 1, 3, 5, 5.5, 8, 9])
            record[timestamp_field] = ts + timedelta(hours=drift_hours)
            self._log_issue(record_id, "TIMEZONE_DRIFT", timestamp_field,
                            ts, record[timestamp_field])

        return record

    # ─── Format / encoding issues ─────────────────────────────────────────

    def inject_text_issues(self, record: dict, text_fields: list[str],
                            record_id: str = "") -> dict:
        """Inject Unicode, whitespace, case, and encoding problems into text fields."""
        for field in text_fields:
            value = record.get(field)
            if not isinstance(value, str) or not value:
                continue

            if self._should_inject(self.rates.extra_whitespace_rate):
                variant = random.choice([
                    f"  {value}",           # Leading spaces
                    f"{value}  ",           # Trailing spaces
                    value.replace(" ", "  ", 1),  # Double space
                    f"\t{value}",           # Tab prefix
                    f"{value}\n",           # Trailing newline
                ])
                record[field] = variant
                self._log_issue(record_id, "EXTRA_WHITESPACE", field, value, variant)
                continue

            if self._should_inject(self.rates.mixed_case_rate):
                variant = random.choice([
                    value.upper(),
                    value.lower(),
                    value.title(),
                    value.swapcase(),
                ])
                record[field] = variant
                self._log_issue(record_id, "MIXED_CASE", field, value, variant)
                continue

            if self._should_inject(self.rates.unicode_injection_rate):
                variant = self._inject_unicode(value)
                record[field] = variant
                self._log_issue(record_id, "UNICODE_INJECTION", field, value, variant)
                continue

            if self._should_inject(self.rates.special_char_rate):
                variant = self._inject_special_chars(value)
                record[field] = variant
                self._log_issue(record_id, "SPECIAL_CHAR", field, value, variant)
                continue

            if self._should_inject(self.rates.encoding_corruption_rate):
                variant = self._inject_encoding_corruption(value)
                record[field] = variant
                self._log_issue(record_id, "ENCODING_CORRUPTION", field, value, variant)
                continue

        return record

    def _inject_unicode(self, value: str) -> str:
        """Insert Unicode complications into a string."""
        strategy = random.choice(["diacritics", "mixed_script", "zero_width", "homoglyph"])

        if strategy == "diacritics":
            replacements = {"a": "à", "e": "é", "o": "ö", "u": "ü", "n": "ñ", "c": "ç"}
            for orig, repl in replacements.items():
                if orig in value.lower():
                    return value.replace(orig, repl, 1)

        elif strategy == "mixed_script":
            sample = random.choice(PROBLEMATIC_UNICODE["mixed_script"])
            return f"{value} ({sample})"

        elif strategy == "zero_width":
            zwc = random.choice(PROBLEMATIC_UNICODE["zero_width"])
            pos = random.randint(0, max(len(value) - 1, 0))
            return value[:pos] + zwc + value[pos:]

        elif strategy == "homoglyph":
            pair = random.choice(PROBLEMATIC_UNICODE["homoglyphs"])
            return value.replace(pair[1], pair[0], 1)

        return value

    def _inject_special_chars(self, value: str) -> str:
        """Inject HTML entities, control chars, or SQL-injection-like strings."""
        strategy = random.choice(["html_entity", "control_char", "sql_like", "pipe_delim"])

        if strategy == "html_entity":
            return value.replace("&", "&amp;", 1) if "&" in value else f"{value}&nbsp;"
        elif strategy == "control_char":
            ctrl = random.choice(["\x00", "\x01", "\x0b", "\x0c", "\x1b"])
            pos = random.randint(0, max(len(value) - 1, 0))
            return value[:pos] + ctrl + value[pos:]
        elif strategy == "sql_like":
            return f"{value}'; DROP TABLE--"
        elif strategy == "pipe_delim":
            return value.replace(" ", "|", 1)
        return value

    def _inject_encoding_corruption(self, value: str) -> str:
        """Simulate mojibake / encoding mismatch."""
        strategy = random.choice(["replacement_char", "latin1_artifact", "double_encode"])

        if strategy == "replacement_char":
            pos = random.randint(0, max(len(value) - 1, 0))
            return value[:pos] + "�" + value[pos + 1:]
        elif strategy == "latin1_artifact":
            artifacts = ["\xc3\xa4", "\xc3\xb6", "\xc3\xbc", "\xc3\xa9", "\xc3\xb1", "\xe2\x80\x99", "\xe2\x80\x93", "\xc2\xa3", "\xc2\xa9"]
            return value + random.choice(artifacts)
        elif strategy == "double_encode":
            try:
                return value.encode("utf-8").decode("latin-1")
            except (UnicodeDecodeError, UnicodeEncodeError):
                return value
        return value

    # ─── Referential integrity breaks ─────────────────────────────────────

    def inject_orphan_fk(self, record: dict, fk_field: str,
                          prefix: str = "ORPHAN_",
                          record_id: str = "") -> dict:
        """Replace FK with a value that points to nothing."""
        if self._should_inject(self.rates.orphan_fk_rate):
            original = record.get(fk_field)
            orphan_id = f"{prefix}{random.randint(900_000_000, 999_999_999)}"
            record[fk_field] = orphan_id
            self._log_issue(record_id, "ORPHAN_FK", fk_field, original, orphan_id)
        return record

    def inject_self_reference(self, record: dict,
                               field_a: str, field_b: str,
                               record_id: str = "") -> dict:
        """Make two FK fields point to the same entity (buyer == seller)."""
        if self._should_inject(self.rates.self_reference_rate):
            original_b = record.get(field_b)
            record[field_b] = record[field_a]
            self._log_issue(record_id, "SELF_REFERENCE", field_b,
                            original_b, record[field_a])
        return record

    # ─── Numeric / business logic violations ──────────────────────────────

    def inject_numeric_issues(self, record: dict,
                               positive_fields: list[str],
                               bounded_fields: dict[str, tuple[float, float]] | None = None,
                               record_id: str = "") -> dict:
        """Inject negative values and out-of-range values."""
        for field in positive_fields:
            value = record.get(field)
            if value is not None and isinstance(value, (int, float)):
                if self._should_inject(self.rates.negative_value_rate):
                    record[field] = -abs(value)
                    self._log_issue(record_id, "NEGATIVE_VALUE", field, value, record[field])

        if bounded_fields:
            for field, (low, high) in bounded_fields.items():
                if self._should_inject(self.rates.impossible_value_rate):
                    original = record.get(field)
                    # Go beyond the valid range
                    if random.random() < 0.5:
                        record[field] = high + random.uniform(0.1, high * 0.5)
                    else:
                        record[field] = low - random.uniform(0.1, abs(low) + 1)
                    self._log_issue(record_id, "IMPOSSIBLE_VALUE", field,
                                    original, record[field])

        return record

    def inject_contradictory_flags(self, record: dict,
                                    contradictions: list[tuple[str, Any, str, Any]],
                                    record_id: str = "") -> dict:
        """
        Inject contradictory business rule violations.

        contradictions: list of (field1, value1, field2, value2) tuples.
        If field1 == value1, then field2 is set to value2 (the contradiction).
        """
        if not self._should_inject(self.rates.contradictory_flag_rate):
            return record

        for f1, v1, f2, v2 in contradictions:
            if record.get(f1) == v1:
                original = record.get(f2)
                record[f2] = v2
                self._log_issue(record_id, "CONTRADICTORY_FLAG", f2, original, v2)
                break

        return record

    # ─── JSON malformation ────────────────────────────────────────────────

    def inject_json_issues(self, record: dict, json_fields: list[str],
                            record_id: str = "") -> dict:
        """Corrupt JSON fields: trailing commas, unclosed braces, wrong types."""
        for field in json_fields:
            value = record.get(field)
            if value is None:
                continue

            if self._should_inject(self.rates.json_malformed_rate):
                strategy = random.choice([
                    "trailing_comma", "unclosed_brace", "single_quotes",
                    "raw_string", "nested_escape", "truncated",
                ])
                original = value

                if strategy == "trailing_comma":
                    if isinstance(value, str):
                        record[field] = value.rstrip("}]") + ",}"
                    else:
                        record[field] = json.dumps(value).rstrip("}]") + ",}"
                elif strategy == "unclosed_brace":
                    s = value if isinstance(value, str) else json.dumps(value)
                    record[field] = s[:-1]  # Remove closing brace/bracket
                elif strategy == "single_quotes":
                    s = value if isinstance(value, str) else json.dumps(value)
                    record[field] = s.replace('"', "'")
                elif strategy == "raw_string":
                    record[field] = "NOT_JSON_DATA"
                elif strategy == "nested_escape":
                    s = value if isinstance(value, str) else json.dumps(value)
                    record[field] = s.replace('"', '\\"')
                elif strategy == "truncated":
                    s = value if isinstance(value, str) else json.dumps(value)
                    record[field] = s[:max(len(s) // 2, 5)]

                self._log_issue(record_id, "JSON_MALFORMED", field,
                                str(original)[:100], str(record[field])[:100])

        return record

    # ─── Enum drift ───────────────────────────────────────────────────────

    def inject_enum_drift(self, record: dict, enum_field: str,
                           unknown_values: list[str],
                           record_id: str = "") -> dict:
        """Inject an unknown/unexpected enum value (schema drift from source)."""
        if self._should_inject(self.rates.enum_drift_rate):
            original = record.get(enum_field)
            new_value = random.choice(unknown_values)
            record[enum_field] = new_value
            self._log_issue(record_id, "ENUM_DRIFT", enum_field, original, new_value)
        return record

    # ─── Truncation ───────────────────────────────────────────────────────

    def inject_truncation(self, record: dict, text_fields: list[str],
                           record_id: str = "") -> dict:
        """Truncate text fields mid-string (buffer overflow, ETL truncation)."""
        if not self._should_inject(self.rates.truncated_field_rate):
            return record

        field = random.choice(text_fields)
        value = record.get(field)
        if isinstance(value, str) and len(value) > 5:
            cut_point = random.randint(3, len(value) // 2)
            record[field] = value[:cut_point]
            self._log_issue(record_id, "TRUNCATED_FIELD", field, value, record[field])

        return record

    # ─── Volume anomalies (batch-level) ───────────────────────────────────

    def adjust_batch_volume(self, target_size: int) -> int:
        """
        Adjust batch size to simulate volume spikes and gaps.

        Returns the adjusted batch size.
        """
        if self._should_inject(self.rates.empty_batch_probability):
            return 0

        if self._should_inject(self.rates.volume_spike_probability):
            multiplier = random.uniform(3.0, 10.0)
            return int(target_size * multiplier)

        if self._should_inject(self.rates.volume_gap_probability):
            multiplier = random.uniform(0.0, 0.2)
            return max(int(target_size * multiplier), 1)

        return target_size

    # ─── Convenience: apply all relevant issues to a record ───────────────

    def inject_all(
        self,
        record: dict,
        record_id: str = "",
        nullable_fields: list[str] | None = None,
        text_fields: list[str] | None = None,
        timestamp_field: str | None = None,
        loaded_at_field: str = "_loaded_at",
        positive_fields: list[str] | None = None,
        bounded_fields: dict[str, tuple[float, float]] | None = None,
        json_fields: list[str] | None = None,
        fk_fields: dict[str, str] | None = None,
        enum_fields: dict[str, list[str]] | None = None,
        mutable_fields: list[str] | None = None,
        contradictions: list[tuple[str, Any, str, Any]] | None = None,
    ) -> tuple[dict, dict | None]:
        """
        Apply all configured issue injections to a single record.

        Returns:
            (possibly-corrupted record, optional near-duplicate record or None)
        """
        if nullable_fields:
            record = self.inject_null(record, nullable_fields, record_id)
            record = self.inject_truncation(record, [f for f in nullable_fields
                                                      if isinstance(record.get(f), str)],
                                             record_id)

        if text_fields:
            record = self.inject_text_issues(record, text_fields, record_id)

        if timestamp_field:
            record = self.inject_temporal_issues(record, timestamp_field,
                                                  loaded_at_field, record_id)

        if positive_fields:
            record = self.inject_numeric_issues(record, positive_fields,
                                                 bounded_fields, record_id)

        if json_fields:
            record = self.inject_json_issues(record, json_fields, record_id)

        if fk_fields:
            for fk_field, prefix in fk_fields.items():
                record = self.inject_orphan_fk(record, fk_field, prefix, record_id)

        if enum_fields:
            for enum_field, unknown_values in enum_fields.items():
                record = self.inject_enum_drift(record, enum_field,
                                                 unknown_values, record_id)

        if contradictions:
            record = self.inject_contradictory_flags(record, contradictions, record_id)

        # Near-duplicate check
        near_dupe = None
        if mutable_fields:
            near_dupe = self.maybe_create_near_duplicate(record, mutable_fields,
                                                          record_id)

        return record, near_dupe

    def get_issue_summary(self) -> dict[str, int]:
        """Return counts of each issue type injected."""
        summary: dict[str, int] = {}
        for entry in self._issue_log:
            issue_type = entry["issue_type"]
            summary[issue_type] = summary.get(issue_type, 0) + 1
        return summary

    def save_issue_log(self, path: str) -> None:
        """Persist the issue log to a JSON file for downstream validation."""
        import json as json_mod
        with open(path, "w") as f:
            json_mod.dump(self._issue_log, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Name variation generator (for sanctions screening realism)
# ---------------------------------------------------------------------------

class NameVariationGenerator:
    """
    Generate realistic name variations that cause sanctions screening
    false positives and false negatives in production.
    """

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)

    def generate_name_variant(self, name: str) -> str:
        """
        Create a realistic variant of a name using common transliteration,
        abbreviation, and formatting differences.
        """
        strategy = self.rng.choice([
            "transliteration", "abbreviation", "honorific",
            "ordering", "spacing", "punctuation", "partial",
        ])

        if strategy == "transliteration":
            return self._transliterate(name)
        elif strategy == "abbreviation":
            return self._abbreviate(name)
        elif strategy == "honorific":
            return self._add_honorific(name)
        elif strategy == "ordering":
            return self._reorder_name(name)
        elif strategy == "spacing":
            return self._alter_spacing(name)
        elif strategy == "punctuation":
            return self._alter_punctuation(name)
        elif strategy == "partial":
            return self._partial_name(name)
        return name

    def _transliterate(self, name: str) -> str:
        for original, variants in TRANSLITERATION_VARIANTS.items():
            if original.lower() in name.lower():
                variant = self.rng.choice(variants)
                return name.replace(original, variant, 1)
        return name

    def _abbreviate(self, name: str) -> str:
        for full, abbrevs in COMPANY_NAME_VARIATIONS.items():
            if full in name:
                return name.replace(full, self.rng.choice(abbrevs), 1)
        parts = name.split()
        if len(parts) > 2:
            # Abbreviate middle name
            parts[1] = parts[1][0] + "."
            return " ".join(parts)
        return name

    def _add_honorific(self, name: str) -> str:
        honorifics = ["Mr.", "Mrs.", "Dr.", "Prof.", "Haj", "Sheikh",
                       "Eng.", "Gen.", "Col.", "Capt."]
        return f"{self.rng.choice(honorifics)} {name}"

    def _reorder_name(self, name: str) -> str:
        parts = name.split()
        if len(parts) >= 2:
            # Swap first/last (Western vs Eastern order)
            return f"{parts[-1]}, {' '.join(parts[:-1])}"
        return name

    def _alter_spacing(self, name: str) -> str:
        strategies = [
            name.replace(" ", "-"),          # Hyphenate
            name.replace("-", " "),          # Remove hyphens
            name.replace(" ", ""),           # No spaces
            name.replace(" ", "  "),         # Double spaces
        ]
        return self.rng.choice(strategies)

    def _alter_punctuation(self, name: str) -> str:
        strategies = [
            name.replace(".", ""),           # Remove periods
            name.replace(",", ""),           # Remove commas
            name.replace("'", ""),           # Remove apostrophes
            name.replace("'", "'"),          # Smart quote → plain
        ]
        return self.rng.choice(strategies)

    def _partial_name(self, name: str) -> str:
        parts = name.split()
        if len(parts) > 1:
            # Drop one part of the name
            drop_idx = self.rng.randint(0, len(parts) - 1)
            return " ".join(p for j, p in enumerate(parts) if j != drop_idx)
        return name


# ---------------------------------------------------------------------------
# Scenario generators for domain-specific BAU problems
# ---------------------------------------------------------------------------

class SanctionsScenarioGenerator:
    """
    Generate specific sanctions compliance scenarios that represent
    real-world BAU challenges.
    """

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.name_gen = NameVariationGenerator(seed)

    def generate_shell_company_chain(self, depth: int = 3) -> list[dict]:
        """
        Generate a chain of shell companies used to obscure beneficial ownership.
        Common pattern: sanctioned entity → shell in CY → shell in PA → clean entity.
        """
        jurisdictions = ["CY", "BS", "VG", "PA", "BM", "KY", "JE", "GG", "IM"]
        chain = []
        for i in range(depth):
            chain.append({
                "level": i,
                "jurisdiction": self.rng.choice(jurisdictions),
                "ownership_pct": round(self.rng.uniform(25, 100), 1),
                "is_nominee": self.rng.random() < 0.4,
                "registration_date_lag_days": self.rng.randint(0, 90),
            })
        return chain

    def generate_flag_hopping_history(self, num_changes: int = 4) -> list[dict]:
        """
        Generate vessel flag change history (sanctions evasion indicator).
        Vessels change flags to avoid detection — a major BAU screening trigger.
        """
        flags = ["PA", "LR", "MH", "BS", "MT", "CY", "KM", "TZ", "TG", "CM"]
        history = []
        current_date = datetime.now() - timedelta(days=365 * 3)
        for i in range(num_changes):
            history.append({
                "change_number": i + 1,
                "from_flag": self.rng.choice(flags),
                "to_flag": self.rng.choice(flags),
                "change_date": current_date.date(),
                "days_under_previous_flag": self.rng.randint(30, 365),
            })
            current_date += timedelta(days=self.rng.randint(60, 365))
        return history

    def generate_sts_transfer_event(self) -> dict:
        """
        Generate a ship-to-ship transfer event.
        STS transfers in open waters are a major sanctions evasion method,
        especially for Iranian and Russian crude oil.
        """
        sts_hotspots = [
            ("LACONIAN_GULF", 36.5, 22.8),
            ("CEUTA", 35.9, -5.3),
            ("KALAMATA", 36.95, 22.1),
            ("SOUTH_EAST_ASIA", 1.5, 104.5),
            ("WEST_AFRICA", 5.5, 2.0),
            ("PERSIAN_GULF", 26.5, 52.0),
        ]
        hotspot = self.rng.choice(sts_hotspots)
        return {
            "location_name": hotspot[0],
            "latitude": round(hotspot[1] + self.rng.uniform(-0.5, 0.5), 6),
            "longitude": round(hotspot[2] + self.rng.uniform(-0.5, 0.5), 6),
            "duration_hours": round(self.rng.uniform(4, 48), 1),
            "both_vessels_dark": self.rng.random() < 0.3,
            "cargo_type": self.rng.choice(["CRUDE_OIL", "FUEL_OIL", "NAPHTHA", "LPG"]),
            "estimated_volume_mt": round(self.rng.uniform(50_000, 300_000), 0),
        }

    def generate_circular_trading_pattern(self, num_legs: int = 4) -> list[dict]:
        """
        Generate a circular trading pattern.
        Entity A sells to B, B sells to C, C sells back to A — often used
        for price manipulation or money laundering.
        """
        entities = [f"CP{self.rng.randint(0, 5_000_000):010d}" for _ in range(num_legs)]
        legs = []
        for i in range(num_legs):
            buyer = entities[i]
            seller = entities[(i + 1) % num_legs]
            # Price escalation through the chain
            base_price = self.rng.uniform(50, 500)
            markup = 1 + (i * self.rng.uniform(0.02, 0.08))
            legs.append({
                "leg": i + 1,
                "buyer": buyer,
                "seller": seller,
                "price_per_mt": round(base_price * markup, 4),
                "time_between_legs_hours": self.rng.randint(1, 72),
            })
        return legs

    def generate_rapid_entity_creation_burst(self) -> dict:
        """
        Generate metadata for a rapid counterparty creation burst.
        Many entities created in short succession from same jurisdiction
        is a common money-laundering/sanctions-evasion indicator.
        """
        return {
            "num_entities": self.rng.randint(5, 25),
            "jurisdiction": self.rng.choice(["CY", "PA", "VG", "BS", "BM", "MT"]),
            "creation_window_hours": self.rng.randint(1, 24),
            "same_registered_agent": self.rng.random() < 0.7,
            "similar_names": self.rng.random() < 0.5,
            "same_address": self.rng.random() < 0.4,
        }

    def generate_screening_backlog_event(self) -> dict:
        """
        Generate a screening backlog / system outage event.
        Backlogs happen in production: system goes down, comes back,
        and thousands of screenings need processing at once.
        """
        return {
            "outage_duration_hours": round(self.rng.uniform(0.5, 12), 1),
            "backlog_size": self.rng.randint(500, 50_000),
            "priority_requeue_pct": round(self.rng.uniform(5, 30), 1),
            "cause": self.rng.choice([
                "SYSTEM_OUTAGE", "LIST_UPDATE_PROCESSING",
                "SCREENING_ENGINE_TIMEOUT", "DATABASE_FAILOVER",
                "NETWORK_PARTITION", "VENDOR_API_RATE_LIMIT",
            ]),
        }
