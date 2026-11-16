"""
Part D2 — publishes a food-order request every 2 seconds to the 'rapido-dispatch'
topic exchange, routing key city.zone.type.

Usage:
    python stream/dispatcher.py
"""
import json
import os
import random
import time
import uuid

import pika
from dotenv import load_dotenv

load_dotenv()

EXCHANGE = "rapido-dispatch"
BROKER_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

CITIES_ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "whitefield"],
}
TYPES = ["ride", "food"]

# D3: send one bad message (missing 'zone') every 10 messages
BAD_MESSAGE_EVERY = 10


def build_message(seq: int):
    city = random.choice(list(CITIES_ZONES))
    zone = random.choice(CITIES_ZONES[city])
    order_type = random.choice(TYPES)
    routing_key = f"{city}.{zone}.{order_type}"

    message = {
        "id": str(uuid.uuid4()),
        "city": city,
        "zone": zone,
        "type": order_type,
        "ts": time.time(),
    }

    if seq % BAD_MESSAGE_EVERY == 0:
        message.pop("zone")  # deliberately malformed — D3 should NACK this

    return routing_key, message


def main():
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    seq = 0
    try:
        while True:
            seq += 1
            routing_key, message = build_message(seq)
            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(content_type="application/json"),
            )
            print(f"[dispatcher] #{seq} -> {routing_key}: {message}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[dispatcher] stopped")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
