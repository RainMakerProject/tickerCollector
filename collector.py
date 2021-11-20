import time
import os
import fire
import logging

from dotenv import load_dotenv

from bitflyer import BitFlyerRealTime, PublicChannel, ProductCode

from library.ohlc import TickerHandler

load_dotenv()

logging.basicConfig(level=getattr(logging, os.getenv('LOG_LEVEL', 'WARNING')))

client = BitFlyerRealTime()
handler = TickerHandler()


def run(*product_code: str) -> None:
    for p in product_code:
        client.subscribe(PublicChannel.lightning_ticker, getattr(ProductCode, p), handler.append)
    client.start()

    while True:
        time.sleep(10.0)
        if not client.is_alive():
            raise RuntimeError('bitFlyer client is now dead')
        if not handler.is_alive():
            raise RuntimeError('ticker handler is now dead')


if __name__ == '__main__':
    fire.Fire(run)
