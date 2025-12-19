from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer(
        "test-topic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="test-group",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    print("Listening to topic 'test-topic'. Press Ctrl+C to stop.")

    try:
        for msg in consumer:
            print(f"Received: key={msg.key}, value={msg.value} | partition={msg.partition}, offset={msg.offset}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
