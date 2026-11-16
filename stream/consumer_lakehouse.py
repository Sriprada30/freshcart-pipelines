"""
Bonus — connects Part D to Part B. Consumes FreshCart-style order messages
from RabbitMQ for 60 seconds, lands them as a raw file, then reuses the
Part B transform SQL so they reach gold.fct_orders. Run it twice to show the
gold totals don't double (same row_hash + MERGE idempotency as Part B).

A FreshCart order message on the wire looks like:
    {"order_id": "...", "customer_id": "...", "amount": 1234.5,
     "currency": "INR", "order_date": "2026-07-01", "updated_at": "..."}

Usage:
    python stream/consumer_lakehouse.py
"""
import json
import os
import sys
import time

import pika
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "elt"))
import freshcart_lib as fc  # noqa: E402

load_dotenv()

EXCHANGE = "rapido-dispatch"
QUEUE_NAME = "lakehouse-consumer"
BROKER_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
COLLECT_SECONDS = 60


def collect_orders_for_one_minute() -> list:
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    # bind to everything -- this consumer is a FreshCart-style order sink, not a driver
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE_NAME, routing_key="#")

    orders = []
    deadline = time.time() + COLLECT_SECONDS
    for method, properties, body in channel.consume(QUEUE_NAME, inactivity_timeout=1):
        if method is None:
            if time.time() >= deadline:
                break
            continue
        try:
            msg = json.loads(body)
            orders.append({
                "order_id": msg.get("id"),
                "customer_id": "CUST-STREAM",
                "amount": 0.0,
                "currency": "INR",
                "order_date": time.strftime("%Y-%m-%d"),
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            })
            channel.basic_ack(method.delivery_tag)
        except json.JSONDecodeError:
            channel.basic_nack(method.delivery_tag, requeue=False)
        if time.time() >= deadline:
            break

    channel.cancel()
    connection.close()
    return orders


def main():
    print(f"[consumer] collecting for {COLLECT_SECONDS}s ...")
    orders = collect_orders_for_one_minute()
    print(f"[consumer] collected {len(orders)} order(s)")

    if not orders:
        print("[consumer] nothing to land — make sure dispatcher.py is running")
        return

    landing_path = fc.write_landing(orders, source="stream")
    print(f"[consumer] wrote landing file: {landing_path}")

    conn = fc.get_connection()
    fc.run_ddl(conn)
    fc.load_file(conn, landing_path, source="web")  # reuse the web schema's load_file path
    fc.run_transform(conn, include_currency_check=True)

    gold = fc.query_df(conn, f"SELECT COUNT(*) AS n, SUM(amount_inr) AS revenue FROM {fc.DATABASE}.GOLD.FCT_ORDERS")
    print("[consumer] gold.fct_orders now:", gold.to_dict("records"))
    print("[consumer] run this script again -- gold totals should NOT double, "
          "because row_hash + MERGE is idempotent (same Part B mechanism).")


if __name__ == "__main__":
    main()
