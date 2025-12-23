"""
Скрипт для создания тестового Langflow flow definition.

Этот скрипт создает JSON файл с определением простого flow,
который можно импортировать в Langflow UI или использовать программно.
"""

import json
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_simple_test_flow() -> dict:
    """
    Создает определение простого тестового flow для обработки сообщений из Kafka.

    Returns:
        Словарь с определением flow в формате Langflow
    """
    flow_definition = {
        "name": "Kafka Message Processor",
        "description": "Простой flow для обработки сообщений из Kafka топика",
        "data": {
            "nodes": [
                {
                    "id": "chat_input_1",
                    "type": "ChatInput",
                    "position": {"x": 100, "y": 200},
                    "data": {
                        "type": "ChatInput",
                        "node": {
                            "template": {
                                "input_value": {
                                    "type": "str",
                                    "required": True,
                                    "placeholder": "Введите сообщение из Kafka",
                                    "value": "",
                                    "name": "input_value",
                                    "display_name": "Input",
                                }
                            },
                            "display_name": "Chat Input",
                            "description": "Входное сообщение из Kafka",
                        },
                    },
                },
                {
                    "id": "text_output_1",
                    "type": "TextOutput",
                    "position": {"x": 500, "y": 200},
                    "data": {
                        "type": "TextOutput",
                        "node": {
                            "template": {
                                "input_value": {
                                    "type": "str",
                                    "required": True,
                                    "value": "",
                                    "name": "input_value",
                                    "display_name": "Input",
                                }
                            },
                            "display_name": "Text Output",
                            "description": "Обработанное сообщение",
                        },
                    },
                },
            ],
            "edges": [
                {
                    "id": "edge_chat_input_1_text_output_1",
                    "source": "chat_input_1",
                    "target": "text_output_1",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                }
            ],
        },
    }
    return flow_definition


def create_advanced_test_flow() -> dict:
    """
    Создает более сложный flow с обработкой текста.

    Returns:
        Словарь с определением flow в формате Langflow
    """
    flow_definition = {
        "name": "Kafka Message Processor Advanced",
        "description": "Продвинутый flow для обработки сообщений из Kafka с использованием LLM",
        "data": {
            "nodes": [
                {
                    "id": "chat_input_1",
                    "type": "ChatInput",
                    "position": {"x": 100, "y": 200},
                    "data": {
                        "type": "ChatInput",
                        "node": {
                            "template": {
                                "input_value": {
                                    "type": "str",
                                    "required": True,
                                    "placeholder": "Сообщение из Kafka",
                                    "value": "",
                                    "name": "input_value",
                                    "display_name": "Input",
                                }
                            },
                            "display_name": "Chat Input",
                            "description": "Входное сообщение",
                        },
                    },
                },
                {
                    "id": "prompt_1",
                    "type": "Prompt",
                    "position": {"x": 300, "y": 200},
                    "data": {
                        "type": "Prompt",
                        "node": {
                            "template": {
                                "template": {
                                    "type": "str",
                                    "required": True,
                                    "value": "Обработай следующее сообщение из Kafka и верни результат:\n{input_value}",
                                    "name": "template",
                                    "display_name": "Template",
                                },
                                "input_value": {
                                    "type": "str",
                                    "required": False,
                                    "value": "",
                                    "name": "input_value",
                                    "display_name": "Input Value",
                                },
                            },
                            "display_name": "Prompt",
                            "description": "Шаблон для обработки сообщения",
                        },
                    },
                },
                {
                    "id": "text_output_1",
                    "type": "TextOutput",
                    "position": {"x": 700, "y": 200},
                    "data": {
                        "type": "TextOutput",
                        "node": {
                            "template": {
                                "input_value": {
                                    "type": "str",
                                    "required": True,
                                    "value": "",
                                    "name": "input_value",
                                    "display_name": "Input",
                                }
                            },
                            "display_name": "Text Output",
                            "description": "Результат обработки",
                        },
                    },
                },
            ],
            "edges": [
                {
                    "id": "edge_chat_input_1_prompt_1",
                    "source": "chat_input_1",
                    "target": "prompt_1",
                    "sourceHandle": "output",
                    "targetHandle": "input_value",
                },
                {
                    "id": "edge_prompt_1_text_output_1",
                    "source": "prompt_1",
                    "target": "text_output_1",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
            ],
        },
    }
    return flow_definition


def save_flow_definition(flow_definition: dict, output_path: str = "test_flow.json"):
    """
    Сохраняет определение flow в JSON файл.

    Args:
        flow_definition: Определение flow
        output_path: Путь для сохранения файла
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(flow_definition, f, indent=2, ensure_ascii=False)

    logger.info(f"Flow definition сохранен в {output_file.absolute()}")


def main():
    """Главная функция для создания тестового flow"""
    logger.info("=" * 60)
    logger.info("Создание тестового Langflow flow для обработки сообщений Kafka")
    logger.info("=" * 60)

    # Создаем простой flow
    simple_flow = create_simple_test_flow()
    output_dir = Path("flows")
    output_dir.mkdir(exist_ok=True)

    simple_flow_path = output_dir / "simple_test_flow.json"
    save_flow_definition(simple_flow, str(simple_flow_path))
    logger.info(f"Простой flow создан: {simple_flow_path}")

    # Создаем продвинутый flow
    advanced_flow = create_advanced_test_flow()
    advanced_flow_path = output_dir / "advanced_test_flow.json"
    save_flow_definition(advanced_flow, str(advanced_flow_path))
    logger.info(f"Продвинутый flow создан: {advanced_flow_path}")

    logger.info("\n" + "=" * 60)
    logger.info("Инструкции по использованию:")
    logger.info("=" * 60)
    logger.info("1. Импортируйте JSON файл в Langflow UI")
    logger.info("2. Или используйте путь к файлу в переменной окружения LANGFLOW_FLOW_PATH")
    logger.info("3. Запустите consumer.py для обработки сообщений из Kafka")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()





