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


if __name__ == '__main__':
    fire.Fire(run)
