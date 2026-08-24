"""
Mock market data connector.

Simulates a live provider so the rest of the pipeline (queue, processor,
gateway, Symphony delivery) can be built and tested before a real
provider is chosen. When a provider is picked, write a new class here
(e.g. RealMarketConnector) that implements BaseMarketConnector the same
way -- nothing else in the app needs to change.

Beyond the live price tick, this also invents the fundamentals a quote
page needs (previous close, 52-week range, market cap, bid/ask, dividend
info, ...). None of that comes from a real GSE entitlement yet -- it is
generated once per symbol at connect() time so it stays internally
consistent (e.g. bid/ask track the live price) without pretending to be
real market data.
"""

import asyncio
import random
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from app.connectors.base_connector import BaseMarketConnector
from app.models.market_data import MarketData

COMPANY_NAMES = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corporation",
    "TSLA": "Tesla, Inc.",
    "NVDA": "NVIDIA Corporation",
    "SNAP": "Snap Inc.",
    "META": "Meta Platforms, Inc.",
}

EXCHANGE_LABEL = "GSE - Simulated Quote - GHS"


class MockMarketConnector(BaseMarketConnector):

    def __init__(self, symbols: list[str], interval_seconds: float = 0.5):
        super().__init__(symbols)
        self.interval_seconds = interval_seconds
        self._prices: dict[str, float] = {}
        self._profiles: dict[str, dict] = {}
        self._session: dict[str, dict] = {}

    async def connect(self) -> None:
        print("Connecting to (mock) market data provider...")
        await asyncio.sleep(1)

        today = datetime.now(timezone.utc).date()

        for symbol in self.symbols:
            price = round(random.uniform(100, 500), 2)
            self._prices[symbol] = price

            forward_dividend = round(random.uniform(0, 2.5), 2)
            ex_dividend_date = today + timedelta(days=random.randint(5, 60))

            profile = {
                "name": COMPANY_NAMES.get(symbol, symbol),
                "previous_close": round(price + random.uniform(-8, 8), 2),
                "week52_low": round(price * random.uniform(0.55, 0.85), 2),
                "week52_high": round(price * random.uniform(1.15, 1.6), 2),
                "market_cap": round(price * random.uniform(2_000_000, 9_000_000), 0),
                "beta": round(random.uniform(0.7, 1.8), 2),
                "pe_ratio": round(random.uniform(10, 40), 2),
                "eps": round(price / random.uniform(12, 30), 2),
                "avg_volume": random.randint(8_000_000, 30_000_000),
                "forward_dividend": forward_dividend,
                "forward_dividend_yield": round((forward_dividend / price) * 100, 2),
                "ex_dividend_date": ex_dividend_date.isoformat(),
                "earnings_date": (today + timedelta(days=random.randint(10, 90))).isoformat(),
                "target_est": round(price * random.uniform(1.05, 1.35), 2),
                "dividend_announcement": None,
            }

            if forward_dividend > 0 and random.random() < 0.5:
                profile["dividend_announcement"] = (
                    f"{symbol} announced a cash dividend of "
                    f"GH₵{forward_dividend:.2f} with an ex-date of {ex_dividend_date.isoformat()}"
                )

            self._profiles[symbol] = profile
            self._session[symbol] = {"open": price, "day_high": price, "day_low": price, "volume": 0}

        self.running = True
        print("Connected to (mock) market data provider.")

    async def stream(self) -> AsyncIterator[MarketData]:
        if not self.running:
            raise RuntimeError("Connector is not connected. Call connect() first.")

        while self.running:
            for symbol in self.symbols:
                self._prices[symbol] += random.uniform(-1, 1)
                self._prices[symbol] = max(self._prices[symbol], 0.01)
                price = round(self._prices[symbol], 2)

                session = self._session[symbol]
                session["day_high"] = max(session["day_high"], price)
                session["day_low"] = min(session["day_low"], price)
                session["volume"] += random.randint(100, 5000)

                profile = self._profiles[symbol]
                spread = max(round(price * 0.0006, 2), 0.01)

                raw = {
                    "symbol": symbol,
                    "name": profile["name"],
                    "exchange_label": EXCHANGE_LABEL,
                    "price": price,
                    "previous_close": profile["previous_close"],
                    "open": round(session["open"], 2),
                    "day_high": round(session["day_high"], 2),
                    "day_low": round(session["day_low"], 2),
                    "week52_high": profile["week52_high"],
                    "week52_low": profile["week52_low"],
                    "market_cap": profile["market_cap"],
                    "beta": profile["beta"],
                    "pe_ratio": profile["pe_ratio"],
                    "eps": profile["eps"],
                    "bid": round(price - spread, 2),
                    "bid_size": random.randint(1, 40) * 100,
                    "ask": round(price + spread, 2),
                    "ask_size": random.randint(1, 40) * 100,
                    "volume": session["volume"],
                    "avg_volume": profile["avg_volume"],
                    "forward_dividend": profile["forward_dividend"],
                    "forward_dividend_yield": profile["forward_dividend_yield"],
                    "ex_dividend_date": profile["ex_dividend_date"],
                    "earnings_date": profile["earnings_date"],
                    "target_est": profile["target_est"],
                    "dividend_announcement": profile["dividend_announcement"],
                    "timestamp": datetime.now(timezone.utc),
                }

                yield self.normalize(raw)

            await asyncio.sleep(self.interval_seconds)

    async def disconnect(self) -> None:
        self.running = False
        print("Disconnected from (mock) market data provider.")

    def normalize(self, raw_data: dict) -> MarketData:
        # The mock already produces canonical field names, so this is a
        # pass-through. A real connector's normalize() would map the
        # provider's actual field names (e.g. sym/px/qty/ts) here.
        return MarketData(**raw_data)
