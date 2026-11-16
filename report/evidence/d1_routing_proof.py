import json
import time
import pika

EXCHANGE = "rapido-dispatch-proof"
BINDINGS = {
    "Ravi": "bangalore.koramangala.*",
    "Priya": "bangalore.indiranagar.*",
    "Arjun": "bangalore.*.ride",
    "Meena": "bangalore.*.food",
    "Karthik": "chennai.tnagar.food",
    "Control": "bangalore.#",
}
TEST_KEYS = [
    "bangalore.koramangala.food",
    "bangalore.indiranagar.ride",
    "bangalore.whitefield.ride",
    "chennai.tnagar.food",
    "chennai.whitefield.ride",
    "bangalore.koramangala",
]

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
ch = conn.channel()
ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=False)

queues = {}
for name, pattern in BINDINGS.items():
    q = ch.queue_declare(queue="", exclusive=True)
    queues[name] = q.method.queue
    ch.queue_bind(exchange=EXCHANGE, queue=q.method.queue, routing_key=pattern)

for i, key in enumerate(TEST_KEYS):
    ch.basic_publish(exchange=EXCHANGE, routing_key=key, body=json.dumps({"seq": i, "key": key}))

time.sleep(1)

results = {key: [] for key in TEST_KEYS}
for name, qname in queues.items():
    while True:
        method, props, body = ch.basic_get(queue=qname, auto_ack=True)
        if method is None:
            break
        msg = json.loads(body)
        results[msg["key"]].append(name)

print("ROUTING PROOF (live RabbitMQ, real topic exchange):")
for key in TEST_KEYS:
    receivers = results[key]
    print(f"  {key:35s} -> {', '.join(receivers) if receivers else 'dropped'}")

for name, qname in queues.items():
    ch.queue_delete(queue=qname)
ch.exchange_delete(exchange=EXCHANGE)
conn.close()
