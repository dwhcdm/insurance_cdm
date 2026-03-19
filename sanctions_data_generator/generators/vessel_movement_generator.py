"""
Vessel movement (AIS) data generator.

Generates realistic AIS vessel position data at MASSIVE scale
(10M records/day target). Simulates voyages between major ports
with dark activity periods, sanctioned zone proximity, and
ship-to-ship transfer indicators.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from generators.base_generator import BaseGenerator


class VesselMovementGenerator(BaseGenerator):
    """
    Generate AIS vessel movement data at MASSIVE scale.
    This is the BILLIONS-scale table: 10M records per day.
    """

    # Major port coordinates (lat, lon)
    PORTS = {
        "SINGAPORE":     (1.29,   103.85),
        "ROTTERDAM":     (51.92,  4.48),
        "HOUSTON":       (29.76,  -95.36),
        "FUJAIRAH":      (25.12,  56.33),
        "SHANGHAI":      (31.23,  121.47),
        "BUSAN":         (35.10,  129.04),
        "JEBEL_ALI":     (25.01,  55.06),
        "MUMBAI":        (19.08,  72.88),
        "SANTOS":        (-23.96, -46.33),
        "DURBAN":        (-29.87, 31.05),
        "BANDAR_ABBAS":  (27.19,  56.27),
        "NOVOROSSIYSK":  (44.72,  37.77),
        "PRIMORSK":      (60.35,  28.68),
        "RIGA":          (56.95,  24.11),
        "TARTUS":        (34.89,  35.89),
        "NAMPO":         (38.73,  125.41),
        "VLADIVOSTOK":   (43.12,  131.87),
        "KOZMINO":       (42.73,  133.02),
    }

    SANCTIONED_PORTS = [
        "BANDAR_ABBAS", "NOVOROSSIYSK", "PRIMORSK",
        "TARTUS", "NAMPO", "KOZMINO",
    ]

    def __init__(self, seed: int = 42, vessel_ids: list = None):
        super().__init__(seed)
        self.vessel_ids = vessel_ids or [f"VS{i:010d}" for i in range(500_000)]
        self.port_names = list(self.PORTS.keys())
        self.port_coords = list(self.PORTS.values())

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
                speed = np.random.uniform(2, 8)       # Slow near port
            else:
                speed = np.random.uniform(10, 16)     # Cruising speed

            heading = np.degrees(np.arctan2(
                dest_coords[1] - origin_coords[1],
                dest_coords[0] - origin_coords[0],
            )) % 360
            heading += np.random.normal(0, 5)

            points.append({
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "speed_knots": round(speed, 1),
                "heading": round(heading % 360, 1),
                "port_of_call": (
                    origin if progress < 0.05
                    else (destination if progress > 0.95 else None)
                ),
            })

        return points

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """
        Generate vessel movement AIS data.
        Optimized for high-volume generation.
        """
        movement_date = kwargs.get("movement_date", datetime.now().date())

        records = []
        remaining = batch_size

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
            dark_start = np.random.randint(0, max(num_pings - 20, 1)) if has_dark_period else -1
            dark_end = dark_start + np.random.randint(5, 20) if has_dark_period else -1

            is_near_sanctioned = (
                origin in self.SANCTIONED_PORTS
                or destination in self.SANCTIONED_PORTS
            )

            base_time = datetime.combine(movement_date, datetime.min.time())
            time_increment = timedelta(hours=24) / num_pings

            for j, point in enumerate(voyage_points):
                timestamp = base_time + (time_increment * j) + timedelta(
                    seconds=np.random.randint(-30, 30)
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
                    if distance < 2.0:  # Within ~200km
                        zone_risk = max(zone_risk, round(100 - distance * 50, 2))

                record = {
                    "movement_id": f"MV{batch_offset + len(records):015d}",
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
                        if is_dark and j == dark_start else None
                    ),
                    "zone_risk_score": max(zone_risk, 0),
                    "is_near_sanctioned_zone": is_near_sanctioned or zone_risk > 50,
                    "ais_message_type": int(np.random.choice(
                        [1, 2, 3, 5, 18, 19],
                        p=[0.30, 0.30, 0.15, 0.10, 0.10, 0.05],
                    )),
                    "navigation_status": np.random.choice(
                        ["UNDER_WAY", "AT_ANCHOR", "NOT_UNDER_COMMAND",
                         "MOORED", "RESTRICTED_MANOEUVRABILITY", "FISHING"],
                        p=[0.60, 0.15, 0.02, 0.15, 0.05, 0.03],
                    ),
                    "source_system": np.random.choice(
                        ["AIS_TERRESTRIAL", "AIS_SATELLITE", "LRIT"]
                    ),
                    "created_at": datetime.now(),
                }
                records.append(record)

                if len(records) >= batch_size:
                    break

            remaining = batch_size - len(records)

        return pd.DataFrame(records[:batch_size])
