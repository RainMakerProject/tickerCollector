import os
import json
import boto3
import fire
import logging

from dotenv import load_dotenv

from bitflyer import BitFlyerRealTime, Ticker, PublicChannel, ProductCode

load_dotenv()

logging.basicConfig(level=getattr(logging, os.getenv('LOG_LEVEL', 'WARNING')))

client = BitFlyerRealTime()

sqs = boto3.resource('sqs')
queue = sqs.Queue(os.environ['SQS_QUEUE_URL'])


def _handler(ticker: Ticker) -> None:
    queue.send_message(MessageBody=json.dumps({
        'price': ticker.ltp,
        'volume': ticker.volume,
        'timestamp': ticker.timestamp.isoformat()
    }))


def run(*product_code: str) -> None:
    for p in product_code:
        client.subscribe(PublicChannel.lightning_ticker, getattr(ProductCode, p), _handler)
    client.start()


if __name__ == '__main__':
    fire.Fire(run)
