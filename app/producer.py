import time
from kafka import KafkaangProducer


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: v.encode("utf-8"),
    )

    topic = "test-topic"
    print(f"Sending messages to topic '{topic}'. Press Ctrl+C to stop.")

    i = 0
    try:
        while True:
            msg = f"message-{i}"
            producer.send(topic, msg)
            producer.flush()
            print(f"Sent: {msg}")
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
