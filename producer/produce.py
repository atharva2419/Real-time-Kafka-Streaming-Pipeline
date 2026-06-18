import json
import time
import requests
import sseclient
from kafka import KafkaProducer
from kafka.errors import KafkaError

WIKIMEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_TOPIC = "wiki-edits"
KAFKA_BOOTSTRAP = "localhost:9092"
PRINT_EVERY = 100


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def extract_event(raw: dict) -> dict | None:
    try:
        return {
            "id": raw.get("id"),
            "type": raw.get("type"),
            "title": raw.get("title"),
            "wiki": raw.get("wiki"),
            "user": raw.get("user"),
            "timestamp": raw.get("timestamp"),
            "bot": bool(raw.get("bot", False)),
        }
    except Exception:
        return None


def stream_events(producer: KafkaProducer) -> None:
    count = 0
    window_start = time.time()

    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "KafkaWikiPipeline/1.0 (https://github.com/atharva2419; educational project)",
    }
    response = requests.get(WIKIMEDIA_SSE_URL, stream=True, headers=headers, timeout=30)
    response.raise_for_status()
    client = sseclient.SSEClient(response)

    for sse in client.events():
        if not sse.data or sse.data.strip() == "":
            continue
        try:
            raw = json.loads(sse.data)
        except json.JSONDecodeError:
            continue

        event = extract_event(raw)
        if event is None:
            continue

        producer.send(KAFKA_TOPIC, value=event)
        count += 1

        if count % PRINT_EVERY == 0:
            elapsed = time.time() - window_start
            rate = PRINT_EVERY / elapsed if elapsed > 0 else 0
            print(f"Produced {count} events ({rate:.0f}/sec)")
            window_start = time.time()


def main() -> None:
    producer = make_producer()
    print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}, publishing to '{KAFKA_TOPIC}'")

    while True:
        try:
            stream_events(producer)
        except requests.exceptions.RequestException as exc:
            print(f"SSE stream error: {exc} — reconnecting in 5s")
            time.sleep(5)
        except KafkaError as exc:
            print(f"Kafka error: {exc} — reconnecting in 5s")
            time.sleep(5)
            producer = make_producer()
        except KeyboardInterrupt:
            print("Shutting down producer.")
            break

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
