import time
import os
import fire
import logging

from dotenv import load_dotenv

from bitflyer import BitFlyerRealTime, PublicChannel, ProductCode

from ticker_collector.ohlc import TickerHandler

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=getattr(logging, os.getenv('LOG_LEVEL', 'WARNING')))

client = BitFlyerRealTime()
handler = TickerHandler(flush_interval=2.0)


def run(*product_code: str) -> None:
    for p in product_code:
        client.subscribe(PublicChannel.lightning_ticker, getattr(ProductCode, p), handler.append)

    client.start()
    handler.start()

    while True:
        time.sleep(2.5)

        if not client.is_alive() or not handler.is_alive():
            if not client.is_alive():
                logger.error('bitFlyer client is now dead')
            if not handler.is_alive():
                logger.error('ticker handler is now dead')

            client.stop()
            handler.stop()

            raise RuntimeError('Either bitFlyer client or ticker handler, or both died')


if __name__ == '__main__':
    fire.Fire(run)
