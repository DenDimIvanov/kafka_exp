import asyncio
import json
import logging
import os
from typing import Optional

import httpx
from aiokafka import AIOKafkaConsumer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Импортируем flow из test_flow.py
try:
    from app.test_flow import get_flow
    LOCAL_FLOW_AVAILABLE = True
except ImportError:
    try:
        from test_flow import get_flow
        LOCAL_FLOW_AVAILABLE = True
    except ImportError:
        logger.warning("Не удалось импортировать test_flow. Будет использован только Langflow API.")
        LOCAL_FLOW_AVAILABLE = False
        get_flow = None


class LangflowClient:
    """Асинхронный клиент для взаимодействия с Langflow API"""

    def __init__(
        self,
        base_url: str = "http://localhost:7860",
        flow_id: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.flow_id = flow_id or os.getenv("LANGFLOW_FLOW_ID")
        self.api_key = api_key or os.getenv("LANGFLOW_API_KEY")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Получает или создает асинхронный HTTP клиент"""
        if self._client is None:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(
                headers=headers,
                timeout=self.timeout,
            )
        return self._client

    async def close(self):
        """Закрывает HTTP клиент"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def run_flow(self, input_data: dict) -> dict:
        """
        Асинхронно вызывает flow в Langflow с переданными данными

        Args:
            input_data: Словарь с входными данными для flow

        Returns:
            Результат выполнения flow

        Raises:
            httpx.HTTPError: При ошибке HTTP запроса
        """
        if not self.flow_id:
            raise ValueError("Flow ID не указан. Установите LANGFLOW_FLOW_ID или передайте flow_id")

        # Langflow API endpoint для запуска flow
        url = f"{self.base_url}/api/v1/run/{self.flow_id}"

        # Подготовка данных для запроса
        payload = {
            "input_value": input_data,
            "output_type": "chat",
            "input_type": "chat",
        }

        logger.info(f"Вызываю Langflow flow {self.flow_id} с данными: {input_data}")

        try:
            client = await self._get_client()
            response = await client.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            logger.info(f"Langflow вернул результат: {result}")
            return result
        except httpx.HTTPError as e:
            logger.error(f"Ошибка при вызове Langflow: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Ответ сервера: {e.response.text}")
            raise


class KafkaLangflowConsumer:
    """
    Асинхронный consumer для чтения сообщений из Kafka и вызова Langflow.
    
    Может работать в двух режимах:
    1. Использование локального flow из test_flow.py (если USE_LOCAL_FLOW=true)
    2. Вызов Langflow через HTTP API (по умолчанию)
    """

    def __init__(
        self,
        kafka_topic: str,
        kafka_bootstrap_servers: str = "localhost:9092",
        kafka_group_id: str = "langflow-consumer-group",
        langflow_base_url: str = "http://localhost:7860",
        langflow_flow_id: Optional[str] = None,
        langflow_api_key: Optional[str] = None,
        max_concurrent_requests: int = 10,
        use_local_flow: bool = False,
    ):
        self.topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_group_id = kafka_group_id
        self.use_local_flow = use_local_flow and LOCAL_FLOW_AVAILABLE
        
        if self.use_local_flow:
            logger.info("Используется локальный flow из test_flow.py")
            self.local_flow = get_flow() if get_flow else None
            self.langflow_client = None
        else:
            logger.info("Используется Langflow API")
            self.local_flow = None
            self.langflow_client = LangflowClient(
                base_url=langflow_base_url,
                flow_id=langflow_flow_id,
                api_key=langflow_api_key,
            )
        
        self.max_concurrent_requests = max_concurrent_requests
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._consumer: Optional[AIOKafkaConsumer] = None

    async def _get_consumer(self) -> AIOKafkaConsumer:
        """Получает или создает асинхронный Kafka consumer"""
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=self.kafka_group_id,
                value_deserializer=lambda v: v.decode("utf-8"),
            )
            await self._consumer.start()
        return self._consumer

    async def process_message(self, message_value: str) -> bool:
        """
        Асинхронно обрабатывает одно сообщение: парсит и вызывает flow

        Args:
            message_value: Значение сообщения из Kafka

        Returns:
            True если обработка успешна, False в противном случае
        """
        # Используем семафор для ограничения количества одновременных запросов
        async with self._semaphore:
            try:
                # Пытаемся распарсить JSON, если не получается - используем как строку
                try:
                    input_data = json.loads(message_value)
                except json.JSONDecodeError:
                    # Если не JSON, передаем как текстовое сообщение
                    input_data = {"message": message_value}

                # Вызываем flow (локальный или через API)
                if self.use_local_flow and self.local_flow:
                    result = await self.local_flow.process(input_data)
                    logger.info(f"Успешно обработано сообщение локальным flow. Результат: {result}")
                else:
                    result = await self.langflow_client.run_flow(input_data)
                    logger.info(f"Успешно обработано сообщение через Langflow API. Результат: {result}")
                
                return True

            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения '{message_value}': {e}", exc_info=True)
                return False

    async def start(self):
        """Асинхронно запускает consumer и начинает обработку сообщений"""
        logger.info(f"Запуск асинхронного consumer для топика '{self.topic}'. Нажмите Ctrl+C для остановки.")

        consumer = await self._get_consumer()

        try:
            async for msg in consumer:
                logger.info(
                    f"Получено сообщение: {msg.value} "
                    f"(partition={msg.partition}, offset={msg.offset})"
                )
                # Запускаем обработку сообщения в фоне (не блокируя чтение новых сообщений)
                asyncio.create_task(self.process_message(msg.value))
        except asyncio.CancelledError:
            logger.info("Остановка consumer...")
        finally:
            await self.stop()

    async def stop(self):
        """Останавливает consumer и закрывает соединения"""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self.langflow_client:
            await self.langflow_client.close()
        logger.info("Consumer остановлен")


async def main():
    """Главная асинхронная функция для запуска consumer"""
    # Настройки из переменных окружения или значения по умолчанию
    kafka_topic = os.getenv("KAFKA_TOPIC", "test-topic")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "langflow-consumer-group")
    langflow_base_url = os.getenv("LANGFLOW_BASE_URL", "http://localhost:7860")
    langflow_flow_id = os.getenv("LANGFLOW_FLOW_ID")
    langflow_api_key = os.getenv("LANGFLOW_API_KEY")
    max_concurrent = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
    use_local_flow = os.getenv("USE_LOCAL_FLOW", "false").lower() == "true"

    consumer = KafkaLangflowConsumer(
        kafka_topic=kafka_topic,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_group_id=kafka_group_id,
        langflow_base_url=langflow_base_url,
        langflow_flow_id=langflow_flow_id,
        langflow_api_key=langflow_api_key,
        max_concurrent_requests=max_concurrent,
        use_local_flow=use_local_flow,
    )

    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки...")
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
