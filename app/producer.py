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
        print(
            f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
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
    i = 0

    print(f"Sending messages to topic '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        while not killer.stop:
            msg = f"message-{i}"
            producer.produce(TOPIC, value=msg.encode("utf-8"), callback=delivery_report)
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
