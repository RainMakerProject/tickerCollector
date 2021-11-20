from typing import Dict

import logging
import threading
import time
import heapq

from dataclasses import dataclass
from datetime import datetime, timezone

from pynamodb.exceptions import DoesNotExist

from bitflyer import Ticker, Candlestick, ChartType
from chart_handler.models import ChartTable

logger = logging.getLogger(__name__)


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


STICK_OF = Dict[ChartType, Dict[datetime, OHLCV]]


class TickerHandler:
    def __init__(self, flush_interval: float = 10.0) -> None:
        self._lock = threading.Lock()
        self.__stick_of: STICK_OF = {}
        self.__start_thread(flush_interval)

    @property
    def stick_of(self) -> Dict[ChartType, Dict[datetime, OHLCV]]:
        while self._lock.locked():
            pass
        return self.__stick_of

    def append(self, ticker: Ticker) -> None:
        self._append(self.stick_of, ticker)

    def _append(self, stick_of: STICK_OF, ticker: Ticker) -> None:
        with self._lock:
            for c in Candlestick:
                self.__append(stick_of, c, ticker)

    def __append(self, stick_of: STICK_OF, candle: Candlestick, ticker: Ticker) -> None:
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

    def __start_thread(self, interval: float) -> None:
        def _continue_flushing() -> None:
            start_time = time.time()

            while True:
                _t = threading.Thread(target=self._flush)
                _t.start()

                try:
                    _t.join()
                except Exception as e:
                    logger.error(e)

                time_to_wait = ((start_time - time.time()) % interval) or interval
                time.sleep(time_to_wait)

        t = threading.Thread(target=_continue_flushing)
        t.start()

    def _flush(self) -> None:
        stick_of = self.stick_of
        self._lock.acquire()

        data = []
        for chart_type, stick_at in stick_of.items():
            for ts, stick in stick_at.items():
                try:
                    c = ChartTable.get(chart_type, ts)
                except DoesNotExist:
                    c = ChartTable(chart_type, ts)

                c.open_value = stick.open
                c.high_value = stick.high
                c.low_value = stick.low
                c.close_value = stick.close
                c.volume = stick.volume
                c.open_timestamp = stick.open_timestamp
                c.close_timestamp = stick.close_timestamp
                data.append(c)

        with ChartTable.batch_write() as b:
            for datum in data:
                b.save(datum)

        new_stick_of: STICK_OF = {}
        for chart_type, stick_at in stick_of.items():
            new_stick_of[chart_type] = {}
            timestamps = list(stick_at.keys())
            timestamps.sort()

            for i in range(1, 6):
                try:
                    _ts = timestamps[-i]
                except IndexError:
                    break
                new_stick_of[chart_type][_ts] = stick_of[chart_type][_ts]

        self.__stick_of = new_stick_of
        self._lock.release()
