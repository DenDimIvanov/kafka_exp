"""
Простой продюсер на базе confluent_kafka Producer.
Отправляет сообщения в Kafka, пока не будет прерван (Ctrl+C).
"""

import signal
import time
from typing import Optional

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "test-topic"
LINGER_MS = 50  # небольшая задержка для батчинга


class GracefulKiller:
    """Флажок для мягкой остановки по Ctrl+C."""

    def __init__(self) -> None:
        self._stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_: object) -> None:
        self._stop = True

    @property
    def stop(self) -> bool:
        return self._stop


def delivery_report(err: Optional[Exception], msg) -> None:
    """Колбэк доставки — логируем ошибки, если появились."""
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        key_str = msg.key().decode("utf-8") if msg.key() else ""
        value_str = msg.value().decode("utf-8") if msg.value() else ""
        print(
            f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} "
            f"| key={key_str} | message_value={value_str}"
        )


def main() -> None:
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "linger.ms": LINGER_MS,
            # "enable.idempotence": True,  # при необходимости идемпотентности
        }
    )

    killer = GracefulKiller()
    # Набор ключей: Kafka гарантирует, что одинаковый ключ всегда попадает в одну и ту же партицию
    keys = ["key-0", "key-1", "key-2"]
    i = 0

    print(f"Sending messages to topic '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        while not killer.stop:
            key = keys[i % len(keys)]
            msg = f"message-{i}"
            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=msg.encode("utf-8"),
                callback=delivery_report,
            )
            # poll обрабатывает внутреннюю очередь и колбэки
            producer.poll(0)
            i += 1
            time.sleep(1)
    finally:
        print("Flushing pending messages...")
        producer.flush(5)
        print("Producer stopped.")


if __name__ == "__main__":
    main()
