"""
Part D2/D3 — a driver declares a private queue, binds it to 'rapido-dispatch' with
a topic binding pattern, prints every request it receives, and ACKs/NACKs it.

Usage:
    python stream/driver.py NAME PATTERN
    python stream/driver.py Ravi "bangalore.koramangala.*"
"""
import json
import os
import sys

import pika
from dotenv import load_dotenv

load_dotenv()

EXCHANGE = "rapido-dispatch"
BROKER_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def main():
    if len(sys.argv) != 3:
        print('Usage: python stream/driver.py NAME "PATTERN"')
        sys.exit(1)

    name, pattern = sys.argv[1], sys.argv[2]

    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # exclusive, auto-delete private queue — gone when this driver disconnects
    result = channel.queue_declare(queue=f"driver.{name}", exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=pattern)

    print(f"[{name}] bound to pattern '{pattern}', waiting for requests. CTRL+C to exit.")

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body)
        except json.JSONDecodeError:
            message = None

        # D3: reject anything missing the 'zone' field, discard it (no requeue)
        if not message or "zone" not in message:
            print(f"[{name}] REJECTED (missing zone): {body!r}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        print(f"[{name}] received on '{method.routing_key}': {message}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        print(f"\n[{name}] stopped")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
