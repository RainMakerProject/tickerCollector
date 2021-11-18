from typing import Dict

import threading

from dataclasses import dataclass
from datetime import datetime, timezone

from pynamodb.exceptions import DoesNotExist

from bitflyer import Ticker, Candlestick, ChartType
from chart_handler.models import ChartTable


@dataclass
class OHLCV:
    open: float
    high: float
    low: float
    close: float
    volume: float
    open_timestamp: datetime
    close_timestamp: datetime


def _determine_period(ts: datetime, duration: int) -> datetime:
    if duration < Candlestick.ONE_MINUTE.value:
        second, _ = divmod(ts.second, duration)
        return datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute, second * duration, tzinfo=timezone.utc)

    if duration < Candlestick.ONE_HOUR.value:
        duration, _ = divmod(duration, 60)
        minute, _ = divmod(ts.minute, duration)
        return datetime(ts.year, ts.month, ts.day, ts.hour, minute * duration, tzinfo=timezone.utc)

    if duration < Candlestick.ONE_DAY.value:
        duration, _ = divmod(duration, 60 * 60)
        hour, _ = divmod(ts.hour, duration)
        return datetime(ts.year, ts.month, ts.day, hour * duration, tzinfo=timezone.utc)

    if duration == Candlestick.ONE_DAY.value:
        return datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)

    if duration == Candlestick.ONE_WEEK.value:
        cal = ts.isocalendar()
        return datetime.fromisocalendar(cal.year, cal.week, 1)

    raise RuntimeError(f'Unsupported timeframe: {duration}')


class TickerHandler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.__stick_of: Dict[ChartType, Dict[datetime, OHLCV]] = {}

    @property
    def stick_of(self) -> Dict[ChartType, Dict[datetime, OHLCV]]:
        while self._lock.locked():
            pass
        return self.__stick_of

    def append(self, ticker: Ticker) -> None:
        for c in Candlestick:
            self.__append(c, ticker)

    def __append(self, candle: Candlestick, ticker: Ticker) -> None:
        stick_of = self.stick_of

        self._lock.acquire()

        price = ticker.ltp
        volume = ticker.volume
        timestamp = ticker.timestamp

        chart_type = getattr(ChartType, f'{ticker.product_code.name}_{candle.name}')
        period = _determine_period(timestamp, candle.value)

        if chart_type not in stick_of:
            stick_of[chart_type] = {}

            try:
                c = ChartTable.get(chart_type, period)
            except DoesNotExist:
                pass
            else:
                stick_of[chart_type][period] = OHLCV(**{
                    'open': c.open_value, 'high': c.high_value, 'low': c.low_value, 'close': c.close_value,
                    'volume': c.volume, 'open_timestamp': c.open_timestamp, 'close_timestamp': c.close_timestamp,
                })

        if period not in stick_of[chart_type]:
            stick_of[chart_type][period] = OHLCV(**{
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': volume, 'open_timestamp': timestamp, 'close_timestamp': timestamp,
            })

        self.__update_stick(stick_of[chart_type][period], price, volume, timestamp)

        self._lock.release()

        ohlcv = stick_of[chart_type][period]
        if chart_type is ChartType.BTC_JPY_ONE_MINUTE:
            print(f'{chart_type.name}: {ohlcv.high}, {ohlcv.low}')

    def __update_stick(self, stick: OHLCV, price: float, volume: float, timestamp: datetime) -> None:
        stick.volume += volume
        stick.high = max(stick.high, price)
        stick.low = min(stick.low, price)
        if stick.open_timestamp > timestamp:
            stick.open_timestamp = timestamp
            stick.open = price
        if stick.close_timestamp < timestamp:
            stick.close_timestamp = timestamp
            stick.close = price

    def _flush(self) -> None:
        self._lock.acquire()
        # Write OHLC into DDB
        self._lock.release()
