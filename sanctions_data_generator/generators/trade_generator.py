"""
Trade transaction data generator.

Generates realistic commodity trade transactions including physical,
paper, swap, option, and future trades with appropriate counterparties,
vessels, commodities, and risk scoring.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import commodity_config, sanctions_config
from generators.base_generator import BaseGenerator


class TradeGenerator(BaseGenerator):
    """Generate commodity trade transaction data at high volume."""

    BOOKING_ENTITIES = [
        "TRADING_DESK_LONDON", "TRADING_DESK_SINGAPORE",
        "TRADING_DESK_HOUSTON", "TRADING_DESK_GENEVA",
        "TRADING_DESK_DUBAI", "TRADING_DESK_TOKYO",
    ]

    DESKS = [
        "CRUDE_DESK", "PRODUCTS_DESK", "LNG_DESK",
        "METALS_DESK", "AGRI_DESK", "COAL_DESK",
    ]

    ALL_COUNTRIES = [
        "US", "GB", "DE", "FR", "JP", "CN", "SG", "AE", "CH", "NL",
        "NO", "BR", "IN", "KR", "AU", "CA", "SA", "QA", "KW", "OM",
        "MY", "TH", "ID", "RU", "TR", "ZA", "NG", "EG",
    ] + sanctions_config.HIGH_RISK_COUNTRIES

    PORTS = [
        "SINGAPORE", "ROTTERDAM", "HOUSTON", "FUJAIRAH", "SHANGHAI",
        "BUSAN", "JEBEL_ALI", "MUMBAI", "SANTOS", "DURBAN",
        "BANDAR_ABBAS", "NOVOROSSIYSK", "RAS_TANURA", "YANBU",
        "BONNY", "ANGOLA_TERMINAL", "RICHARDS_BAY",
    ]

    SCREENING_STATUSES = ["AUTO_CLEARED", "REVIEW_REQUIRED", "ESCALATED"]

    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """Generate a batch of trade transaction records."""
        trade_date = kwargs.get("trade_date", datetime.now().date())

        records = []

        for i in range(batch_size):
            idx = batch_offset + i

            # Select commodity
            commodity_group = np.random.choice(list(commodity_config.COMMODITIES.keys()))
            commodity = commodity_config.COMMODITIES[commodity_group]
            commodity_code, commodity_name = commodity[
                np.random.randint(0, len(commodity))
            ]

            trade_type = np.random.choice(
                commodity_config.TRADE_TYPES,
                p=[0.45, 0.20, 0.15, 0.10, 0.10],
            )

            origin_country = np.random.choice(self.ALL_COUNTRIES)
            destination_country = np.random.choice(self.ALL_COUNTRIES)

            # Risk scoring
            base_risk = np.random.uniform(0, 30)
            if origin_country in sanctions_config.HIGH_RISK_COUNTRIES:
                base_risk += 30
            elif origin_country in sanctions_config.ELEVATED_RISK_COUNTRIES:
                base_risk += 15
            if destination_country in sanctions_config.HIGH_RISK_COUNTRIES:
                base_risk += 30
            elif destination_country in sanctions_config.ELEVATED_RISK_COUNTRIES:
                base_risk += 15
            sanctions_risk_score = round(min(base_risk, 100), 2)

            if sanctions_risk_score >= 70:
                screening_status = np.random.choice(
                    self.SCREENING_STATUSES, p=[0.10, 0.30, 0.60]
                )
            elif sanctions_risk_score >= 40:
                screening_status = np.random.choice(
                    self.SCREENING_STATUSES, p=[0.40, 0.40, 0.20]
                )
            else:
                screening_status = np.random.choice(
                    self.SCREENING_STATUSES, p=[0.85, 0.12, 0.03]
                )

            quantity_mt = round(np.random.lognormal(mean=8, sigma=1.5), 4)
            price_per_mt = round(np.random.uniform(50, 2000), 4)
            total_value = round(quantity_mt * price_per_mt, 2)

            trade_timestamp = datetime.combine(trade_date, datetime.min.time()) + timedelta(
                hours=np.random.randint(0, 24),
                minutes=np.random.randint(0, 60),
                seconds=np.random.randint(0, 60),
            )

            vessel_id = f"VS{np.random.randint(0, 500_000):010d}" if trade_type == "PHYSICAL" else None
            vessel_flagged = np.random.random() < 0.05 if vessel_id else False

            record = {
                "trade_id": f"TR{idx:012d}",
                "trade_reference": f"REF-{trade_date.strftime('%Y%m%d')}-{idx:08d}",
                "trade_timestamp": trade_timestamp,
                "trade_date": trade_date,
                "settlement_date": trade_date + timedelta(days=np.random.choice([2, 3, 5, 10, 30])),
                "trade_type": trade_type,
                "trade_status": np.random.choice(
                    ["CONFIRMED", "PENDING", "SETTLED", "CANCELLED"],
                    p=[0.50, 0.20, 0.25, 0.05],
                ),
                "buyer_counterparty_id": f"CP{np.random.randint(0, 5_000_000):010d}",
                "seller_counterparty_id": f"CP{np.random.randint(0, 5_000_000):010d}",
                "commodity_code": commodity_code,
                "commodity_name": commodity_name,
                "commodity_group": commodity_group,
                "quantity_mt": quantity_mt,
                "price_per_mt_usd": price_per_mt,
                "total_value_usd": total_value,
                "currency": np.random.choice(["USD", "EUR", "GBP", "JPY", "SGD"], p=[0.60, 0.15, 0.10, 0.05, 0.10]),
                "fx_rate": round(np.random.uniform(0.8, 1.4), 8) if np.random.random() > 0.60 else 1.0,
                "incoterm": np.random.choice(commodity_config.INCOTERMS),
                "origin_country": origin_country,
                "destination_country": destination_country,
                "loading_port": np.random.choice(self.PORTS),
                "discharge_port": np.random.choice(self.PORTS),
                "vessel_id": vessel_id,
                "vessel_flagged": vessel_flagged,
                "sanctions_risk_score": sanctions_risk_score,
                "screening_status": screening_status,
                "booking_entity": np.random.choice(self.BOOKING_ENTITIES),
                "trader_id": f"TRADER_{np.random.randint(1, 200):03d}",
                "desk": np.random.choice(self.DESKS),
                "source_system": np.random.choice(["ETRM", "CTRM", "MANUAL", "API_FEED"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            records.append(record)

        return pd.DataFrame(records)
