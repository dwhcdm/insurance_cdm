"""
Trade transaction data generator.

Generates commodity trade transactions with counterparty and vessel linkage,
realistic pricing, risk scoring, and screening status.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import commodity_config, sanctions_config
from generators.base_generator import BaseGenerator


class TradeGenerator(BaseGenerator):
    """Generate commodity trade transactions at scale (2.5M/day target)."""

    BOOKING_ENTITIES = [
        "PRISM_TRADING_AG", "PRISM_COMMODITIES_PTE",
        "PRISM_ENERGY_LLC", "PRISM_METALS_BV",
        "PRISM_AGRI_SA", "PRISM_LNG_LTD",
    ]

    DESKS = [
        "CRUDE_DESK", "PRODUCTS_DESK", "LNG_DESK",
        "METALS_DESK", "AGRI_DESK", "STRUCTURED_DESK",
    ]

    COUNTRIES = [
        "US", "GB", "DE", "FR", "SG", "AE", "NL", "CH",
        "NO", "JP", "CN", "KR", "IN", "BR", "AU", "SA",
        "QA", "KW", "OM", "MY", "ZA", "NG", "EG", "TR",
        "RU", "IR", "KP", "VE",
    ]

    HIGH_RISK = set(sanctions_config.HIGH_RISK_COUNTRIES)

    PORTS = [
        "SINGAPORE", "ROTTERDAM", "HOUSTON", "FUJAIRAH",
        "SHANGHAI", "BUSAN", "JEBEL_ALI", "MUMBAI",
        "SANTOS", "DURBAN", "BANDAR_ABBAS", "NOVOROSSIYSK",
        "PRIMORSK", "RICHARDS_BAY", "DAMPIER", "PORT_HEDLAND",
        "NEW_ORLEANS",
    ]

    def __init__(self, seed: int = 42, counterparty_ids: list = None, vessel_ids: list = None):
        super().__init__(seed)
        self.counterparty_ids = counterparty_ids or [f"CP{i:010d}" for i in range(5_000_000)]
        self.vessel_ids = vessel_ids or [f"VS{i:010d}" for i in range(500_000)]

        # Flatten commodities list
        self.all_commodities = []
        for group, items in commodity_config.COMMODITIES.items():
            for code, name in items:
                self.all_commodities.append((code, name, group))

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of trade transaction records."""
        trade_date = kwargs.get("trade_date", datetime.now().date())

        records = []
        for i in range(batch_size):
            idx = batch_offset + i
            commodity = self.all_commodities[np.random.randint(len(self.all_commodities))]

            origin = np.random.choice(self.COUNTRIES)
            destination = np.random.choice(self.COUNTRIES)

            buyer_id = np.random.choice(self.counterparty_ids)
            seller_id = np.random.choice(self.counterparty_ids)

            trade_type = np.random.choice(
                commodity_config.TRADE_TYPES,
                p=[0.40, 0.25, 0.15, 0.10, 0.10],
            )

            quantity = round(np.random.lognormal(mean=8, sigma=1.5), 4)
            price = round(np.random.lognormal(mean=5, sigma=1.0), 4)
            total_value = round(quantity * price, 2)

            vessel_id = (
                np.random.choice(self.vessel_ids)
                if trade_type == "PHYSICAL" else None
            )

            # Risk scoring
            risk_score = 10.0
            if origin in self.HIGH_RISK or destination in self.HIGH_RISK:
                risk_score += np.random.uniform(20, 50)
            if np.random.random() < 0.05:
                risk_score += np.random.uniform(10, 30)

            risk_score = min(round(risk_score + np.random.uniform(-5, 15), 2), 100)

            screening_status = np.random.choice(
                ["CLEARED", "REVIEW_REQUIRED", "ESCALATED", "PENDING", "BLOCKED"],
                p=[0.70, 0.15, 0.08, 0.05, 0.02],
            )

            trade_ts = datetime.combine(trade_date, datetime.min.time()) + timedelta(
                seconds=np.random.randint(0, 86400)
            )

            record = {
                "trade_id": f"TR{idx:012d}",
                "trade_reference": f"REF-{trade_date.strftime('%Y%m%d')}-{idx:08d}",
                "trade_timestamp": trade_ts,
                "trade_date": trade_date,
                "settlement_date": trade_date + timedelta(days=np.random.choice([2, 5, 10, 30])),
                "trade_type": trade_type,
                "trade_status": np.random.choice(
                    ["CONFIRMED", "PENDING", "SETTLED", "CANCELLED"],
                    p=[0.50, 0.20, 0.25, 0.05],
                ),
                "buyer_counterparty_id": buyer_id,
                "seller_counterparty_id": seller_id,
                "commodity_code": commodity[0],
                "commodity_name": commodity[1],
                "commodity_group": commodity[2],
                "quantity_mt": quantity,
                "price_per_mt_usd": price,
                "total_value_usd": total_value,
                "currency": np.random.choice(["USD", "EUR", "GBP", "SGD"], p=[0.70, 0.15, 0.10, 0.05]),
                "fx_rate": round(np.random.uniform(0.75, 1.35), 8) if np.random.random() < 0.30 else 1.0,
                "incoterm": np.random.choice(commodity_config.INCOTERMS),
                "origin_country": origin,
                "destination_country": destination,
                "loading_port": np.random.choice(self.PORTS),
                "discharge_port": np.random.choice(self.PORTS),
                "vessel_id": vessel_id,
                "vessel_flagged": np.random.random() < 0.04 if vessel_id else False,
                "sanctions_risk_score": risk_score,
                "screening_status": screening_status,
                "booking_entity": np.random.choice(self.BOOKING_ENTITIES),
                "trader_id": f"TRD{np.random.randint(1, 500):04d}",
                "desk": np.random.choice(self.DESKS),
                "source_system": np.random.choice(["ETRM_PRIMARY", "ETRM_LEGACY", "MANUAL_ENTRY"]),
                "_loaded_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
