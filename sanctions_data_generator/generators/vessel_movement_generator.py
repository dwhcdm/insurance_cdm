"""
Vessel movement (AIS) data generator.

Generates realistic AIS vessel position data at MASSIVE scale
(10M records/day target). Simulates voyages between major ports
with dark activity periods, sanctioned zone proximity, and
ship-to-ship transfer indicators.

Production-grade BAU issues injected:
    - AIS spoofing / position manipulation (GPS offset, impossible speed)
    - Extended dark activity (transponder off for days, not hours)
    - Ship-to-ship (STS) transfers in known hotspots
    - Temporal drift across AIS source systems (satellite vs terrestrial lag)
    - Duplicate AIS messages from multiple receivers
    - Impossible navigation (speed > 25kts for tanker, lat/lon out of range)
    - Zone risk score anomalies (vessel near sanctioned zone but score = 0)
    - Stale position data replayed from cache
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from generators.base_generator import BaseGenerator
from generators.data_quality_issues import (
    DataQualityIssueInjector,
    IssueInjectionRates,
    SanctionsScenarioGenerator,
)


class VesselMovementGenerator(BaseGenerator):
    """
    Generate AIS vessel movement data at MASSIVE scale.
    This is the BILLIONS-scale table: 10M records per day.
    """

    # Major port coordinates (lat, lon)
    PORTS = {
        "SINGAPORE": (1.29, 103.85),
        "ROTTERDAM": (51.92, 4.48),
        "HOUSTON": (29.76, -95.36),
        "FUJAIRAH": (25.12, 56.33),
        "SHANGHAI": (31.23, 121.47),
        "BUSAN": (35.10, 129.04),
        "JEBEL_ALI": (25.01, 55.06),
        "MUMBAI": (19.08, 72.88),
        "SANTOS": (-23.96, -46.33),
        "DURBAN": (-29.87, 31.05),
        "BANDAR_ABBAS": (27.19, 56.27),
        "NOVOROSSIYSK": (44.72, 37.77),
        "PRIMORSK": (60.35, 28.68),
        "RIGA": (56.95, 24.11),
        "TARTUS": (34.89, 35.89),
        "NAMPO": (38.73, 125.41),
        "VLADIVOSTOK": (43.12, 131.87),
        "KOZMINO": (42.73, 133.02),
    }

    SANCTIONED_PORTS = [
        "BANDAR_ABBAS",
        "NOVOROSSIYSK",
        "PRIMORSK",
        "TARTUS",
        "NAMPO",
        "KOZMINO",
    ]

    # Known STS transfer hotspot zones (lat, lon, radius_deg)
    STS_HOTSPOTS = [
        (36.0, 15.0, 2.0),  # Mediterranean off Malta
        (25.0, 56.0, 1.5),  # Fujairah anchorage
        (1.0, 104.0, 1.0),  # Singapore Strait
        (-6.0, 106.0, 1.5),  # Java Sea
        (10.0, -61.0, 1.0),  # Trinidad and Tobago
        (37.0, 26.0, 1.5),  # Aegean Sea / Kalamata
    ]

    def __init__(
        self,
        seed: int = 42,
        vessel_ids: list = None,
        issue_rates: IssueInjectionRates | None = None,
    ):
        super().__init__(seed)
        self.vessel_ids = vessel_ids or [f"VS{i:010d}" for i in range(500_000)]
        self.port_names = list(self.PORTS.keys())
        self.port_coords = list(self.PORTS.values())
        self.injector = DataQualityIssueInjector(issue_rates, seed)
        self.scenario_gen = SanctionsScenarioGenerator(seed)

    def _simulate_voyage(self, origin: str, destination: str, num_points: int) -> list:
        """Simulate a voyage path between two ports with realistic movement."""
        origin_coords = self.PORTS[origin]
        dest_coords = self.PORTS[destination]

        points = []
        for j in range(num_points):
            progress = j / max(num_points - 1, 1)

            # Linear interpolation with random deviation
            lat = origin_coords[0] + (dest_coords[0] - origin_coords[0]) * progress
            lon = origin_coords[1] + (dest_coords[1] - origin_coords[1]) * progress

            # Add realistic deviation (weather, currents)
            lat += np.random.normal(0, 0.1)
            lon += np.random.normal(0, 0.1)

            # Speed varies during voyage
            if progress < 0.1 or progress > 0.9:
                speed = np.random.uniform(2, 8)  # Slow near port
            else:
                speed = np.random.uniform(10, 16)  # Cruising speed

            heading = (
                np.degrees(
                    np.arctan2(
                        dest_coords[1] - origin_coords[1],
                        dest_coords[0] - origin_coords[0],
                    )
                )
                % 360
            )
            heading += np.random.normal(0, 5)

            points.append(
                {
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "speed_knots": round(speed, 1),
                    "heading": round(heading % 360, 1),
                    "port_of_call": (
                        origin
                        if progress < 0.05
                        else (destination if progress > 0.95 else None)
                    ),
                }
            )

        return points

    def generate_batch(
        self, batch_size: int, batch_offset: int = 0, **kwargs
    ) -> pd.DataFrame:
        """
        Generate vessel movement AIS data with production-grade issues.
        Optimized for high-volume generation.
        """
        movement_date = kwargs.get("movement_date", datetime.now().date())

        records = []
        remaining = batch_size
        sts_injected = False

        while remaining > 0:
            vessel_id = np.random.choice(self.vessel_ids)

            # Each vessel generates multiple AIS pings per day (every 3-30 min)
            num_pings = min(np.random.randint(48, 480), remaining)

            origin = np.random.choice(self.port_names)
            destination = np.random.choice(self.port_names)
            while destination == origin:
                destination = np.random.choice(self.port_names)

            voyage_points = self._simulate_voyage(origin, destination, num_pings)

            # Dark activity period (AIS turned off) - 2% chance
            has_dark_period = np.random.random() < 0.02
            dark_start = (
                np.random.randint(0, max(num_pings - 20, 1)) if has_dark_period else -1
            )
            dark_end = dark_start + np.random.randint(5, 20) if has_dark_period else -1

            # Extended dark activity (days, not hours) - 0.3% chance
            if np.random.random() < 0.003 and has_dark_period:
                dark_end = min(dark_start + np.random.randint(50, 200), num_pings - 1)

            is_near_sanctioned = (
                origin in self.SANCTIONED_PORTS or destination in self.SANCTIONED_PORTS
            )

            # STS transfer event injection - 0.5% of voyages
            sts_event = None
            if np.random.random() < 0.005 and not sts_injected:
                sts_event = self.scenario_gen.generate_sts_transfer_event()
                sts_injected = True

            base_time = datetime.combine(movement_date, datetime.min.time())
            time_increment = timedelta(hours=24) / num_pings

            for j, point in enumerate(voyage_points):
                timestamp = (
                    base_time
                    + (time_increment * j)
                    + timedelta(seconds=np.random.randint(-30, 30))
                )

                is_dark = has_dark_period and dark_start <= j <= dark_end

                # Zone risk score based on proximity to sanctioned ports
                zone_risk = 0.0
                for sanctioned_port in self.SANCTIONED_PORTS:
                    port_coords = self.PORTS[sanctioned_port]
                    distance = np.sqrt(
                        (point["latitude"] - port_coords[0]) ** 2
                        + (point["longitude"] - port_coords[1]) ** 2
                    )
                    if distance < 2.0:
                        zone_risk = max(zone_risk, round(100 - distance * 50, 2))

                # STS indicator: vessel loitering near STS hotspot
                is_sts = False
                if sts_event and 0.3 < (j / num_pings) < 0.7:
                    for hlat, hlon, hradius in self.STS_HOTSPOTS:
                        dist = np.sqrt(
                            (point["latitude"] - hlat) ** 2
                            + (point["longitude"] - hlon) ** 2
                        )
                        if dist < hradius:
                            is_sts = True
                            point["speed_knots"] = round(np.random.uniform(0.5, 3.0), 1)
                            break

                record_id = f"MV{batch_offset + len(records):015d}"

                record = {
                    "movement_id": record_id,
                    "vessel_id": vessel_id,
                    "timestamp": timestamp,
                    "latitude": point["latitude"],
                    "longitude": point["longitude"],
                    "speed_knots": point["speed_knots"] if not is_dark else None,
                    "heading": point["heading"] if not is_dark else None,
                    "port_of_call": point["port_of_call"],
                    "origin_port": origin,
                    "destination_port": destination,
                    "voyage_id": f"VOY_{vessel_id}_{movement_date.strftime('%Y%m%d')}",
                    "is_dark_activity": is_dark,
                    "dark_duration_hours": (
                        round((dark_end - dark_start) * (24 / num_pings), 1)
                        if is_dark and j == dark_start
                        else None
                    ),
                    "zone_risk_score": max(zone_risk, 0),
                    "is_near_sanctioned_zone": is_near_sanctioned or zone_risk > 50,
                    "is_sts_indicator": is_sts,
                    "ais_message_type": int(
                        np.random.choice(
                            [1, 2, 3, 5, 18, 19],
                            p=[0.30, 0.30, 0.15, 0.10, 0.10, 0.05],
                        )
                    ),
                    "navigation_status": np.random.choice(
                        [
                            "UNDER_WAY",
                            "AT_ANCHOR",
                            "NOT_UNDER_COMMAND",
                            "MOORED",
                            "RESTRICTED_MANOEUVRABILITY",
                            "FISHING",
                        ],
                        p=[0.60, 0.15, 0.02, 0.15, 0.05, 0.03],
                    ),
                    "source_system": np.random.choice(
                        ["AIS_TERRESTRIAL", "AIS_SATELLITE", "LRIT"]
                    ),
                    "created_at": datetime.now(),
                }

                # ---- Inject production-grade data quality issues ----
                record, near_dupe = self.injector.inject_all(
                    record,
                    record_id=record_id,
                    nullable_fields=[
                        "speed_knots",
                        "heading",
                        "port_of_call",
                        "dark_duration_hours",
                    ],
                    text_fields=["origin_port", "destination_port", "port_of_call"],
                    timestamp_field="timestamp",
                    loaded_at_field="created_at",
                    positive_fields=["speed_knots", "zone_risk_score"],
                    bounded_fields={
                        "latitude": (-90.0, 90.0),
                        "longitude": (-180.0, 180.0),
                        "heading": (0.0, 360.0),
                        "speed_knots": (0.0, 50.0),
                    },
                    enum_fields={
                        "navigation_status": [
                            "AIS_OFF",
                            "UNKNOWN",
                            "DRIFTING",
                            "TOWING",
                        ],
                        "source_system": ["MANUAL_REPORT", "RADAR", "UNKNOWN_SOURCE"],
                    },
                    mutable_fields=["vessel_id", "origin_port", "destination_port"],
                    contradictions=[
                        (
                            "is_dark_activity",
                            True,
                            "speed_knots",
                            round(np.random.uniform(8, 14), 1),
                        ),
                        ("is_near_sanctioned_zone", True, "zone_risk_score", 0.0),
                    ],
                )

                records.append(record)

                # Duplicate AIS message from multiple receivers
                exact_dupe = self.injector.maybe_create_exact_duplicate(record)
                if exact_dupe:
                    records.append(exact_dupe)

                if near_dupe:
                    records.append(near_dupe)

                if len(records) >= batch_size:
                    break

            remaining = batch_size - len(records)

        return pd.DataFrame(records[:batch_size])
