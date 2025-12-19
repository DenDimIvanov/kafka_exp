import time
from kafka import KafkaProducer


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
    )

    topic = "test-topic"
    print(f"Sending messages to topic '{topic}'. Press Ctrl+C to stop.")

    i = 0
    # Используем 3 ключа, которые будут повторяться
    # Одинаковые ключи всегда попадут в одну и ту же партицию
    keys = ["user-1", "user-2", "user-3"]
    
    try:
        while True:
            key = keys[i % len(keys)]  # циклически: user-1, user-2, user-3, user-1, ...
            value = f"message-{i}"

            producer.send(topic, key=key, value=value)
            producer.flush()
            print(f"Sent: key={key}, value={value}")
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
