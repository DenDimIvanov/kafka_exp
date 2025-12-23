"""
Тестовый Langflow flow для обработки сообщений из Kafka.

Этот модуль реализует flow-процессор, который может быть вызван из consumer.py.
Flow обрабатывает сообщения, полученные из Kafka топика.
"""

import json
import logging
import os
from typing import Any, Dict, Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

try:
    from langflow import load_flow_from_path
    from langflow.interface.run import run_flow
    LANGFLOW_AVAILABLE = True
except ImportError:
    logger.warning(
        "Langflow SDK не установлен. Будет использована простая обработка."
    )
    LANGFLOW_AVAILABLE = False


class TestFlow:
    """
    Тестовый flow для обработки сообщений из Kafka.
    
    Этот класс реализует flow-логику, которая может быть вызвана из consumer.py.
    Flow обрабатывает сообщения, полученные из Kafka топика.
    """

    def __init__(self, flow_path: Optional[str] = None):
        """
        Инициализирует flow.

        Args:
            flow_path: Опциональный путь к JSON файлу с flow definition для Langflow SDK
        """
        self.flow_path = flow_path
        self.flow = None
        self.processed_count = 0

    async def process(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Асинхронно обрабатывает сообщение из Kafka.

        Это основной метод, который вызывается из consumer.py.

        Args:
            input_data: Словарь с данными сообщения из Kafka

        Returns:
            Результат обработки сообщения
        """
        self.processed_count += 1

        # Если доступен Langflow SDK и указан путь к flow, используем его
        if LANGFLOW_AVAILABLE and self.flow_path and os.path.exists(self.flow_path):
            return await self._process_with_langflow(input_data)
        else:
            # Иначе используем простую обработку
            return self._process_simple(input_data)

    def _process_simple(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Простая обработка сообщения без использования Langflow SDK.

        Args:
            input_data: Входные данные из Kafka

        Returns:
            Результат обработки
        """
        # Извлекаем текст сообщения
        text = input_data.get("message", str(input_data))

        # Простая обработка: добавляем метаданные и преобразуем текст
        result = {
            "original_message": input_data,
            "processed_text": text.upper() if isinstance(text, str) else str(text),
            "processing_count": self.processed_count,
            "status": "processed",
            "metadata": {
                "source": "kafka",
                "processor": "test_flow",
            },
        }

        logger.info(f"Обработано сообщение #{self.processed_count}: {result}")
        return result

    async def _process_with_langflow(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обрабатывает сообщение используя Langflow SDK.

        Args:
            input_data: Входные данные из Kafka

        Returns:
            Результат обработки через Langflow
        """
        try:
            # Загружаем flow при первом использовании
            if self.flow is None:
                logger.info(f"Загрузка Langflow flow из {self.flow_path}")
                self.flow = load_flow_from_path(self.flow_path)

            # Преобразуем input_data в строку для Langflow
            if isinstance(input_data, dict):
                input_value = input_data.get("message", json.dumps(input_data, ensure_ascii=False))
            else:
                input_value = str(input_data)

            # Запускаем flow (run_flow может быть синхронным, оборачиваем в executor)
            import asyncio
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: run_flow(
                    flow=self.flow,
                    input_value=input_value,
                    output_type="chat",
                )
            )

            logger.info(f"Langflow обработал сообщение #{self.processed_count}")
            return {
                "langflow_result": result,
                "input": input_data,
                "status": "processed_by_langflow",
                "processing_count": self.processed_count,
            }

        except Exception as e:
            logger.error(f"Ошибка при выполнении Langflow flow: {e}", exc_info=True)
            # Fallback на простую обработку
            logger.info("Переключение на простую обработку из-за ошибки")
            return self._process_simple(input_data)


# Глобальный экземпляр flow для использования в consumer
_flow_instance: Optional[TestFlow] = None


def get_flow(flow_path: Optional[str] = None) -> TestFlow:
    """
    Получает или создает экземпляр TestFlow.

    Args:
        flow_path: Путь к flow definition файлу (опционально)

    Returns:
        Экземпляр TestFlow
    """
    global _flow_instance
    if _flow_instance is None:
        flow_path = flow_path or os.getenv("LANGFLOW_FLOW_PATH")
        _flow_instance = TestFlow(flow_path=flow_path)
    return _flow_instance


async def test_flow_with_sample_messages():
    """
    Тестирует flow с примерами сообщений.
    """
    logger.info("Запуск тестирования flow...")

    flow = get_flow()

    # Тестовые сообщения
    test_messages = [
        {"message": "Hello, Langflow!"},
        {"message": "Test message from Kafka", "user_id": "123"},
        {"message": "Simple text message"},
        {"data": {"text": "Nested message"}, "metadata": {"source": "kafka"}},
    ]

    logger.info(f"Обработка {len(test_messages)} тестовых сообщений...")

    for i, msg in enumerate(test_messages, 1):
        logger.info(f"\n--- Тест {i}/{len(test_messages)} ---")

        try:
            result = await flow.process(msg)
            logger.info(f"Результат: {json.dumps(result, indent=2, ensure_ascii=False)}")
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)

    logger.info("\nТестирование завершено!")


async def main():
    """Главная функция для запуска тестирования flow"""
    flow_path = os.getenv("LANGFLOW_FLOW_PATH")

    logger.info("=" * 60)
    logger.info("Тестирование Langflow Flow для обработки сообщений Kafka")
    logger.info("=" * 60)

    if flow_path:
        logger.info(f"Путь к flow: {flow_path}")
    else:
        logger.info("Flow path не указан, будет использована простая обработка")

    await test_flow_with_sample_messages()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
