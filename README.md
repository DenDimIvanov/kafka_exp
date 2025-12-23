# Kafka + Langflow Integration

Проект для интеграции Apache Kafka с Langflow. Consumer читает сообщения из Kafka топика и передает их в Langflow flow для обработки.

## Архитектура

```
┌─────────────────┐
│  Внешняя система │
│   (Producer)    │
└────────┬────────┘
         │
         │ Сообщения
         ▼
┌─────────────────┐
│  Kafka Broker   │
│   (Topic)       │
└────────┬────────┘
         │
         │ Чтение сообщений
         ▼
┌─────────────────┐
│ Kafka Consumer  │
│  (Python app)   │
└────────┬────────┘
         │
         │ HTTP API запрос
         ▼
┌─────────────────┐
│    Langflow     │
│   (Flow API)    │
└─────────────────┘
```

## Компоненты

1. **Kafka Broker** - Брокер сообщений (настроен в docker-compose.yml)
2. **Producer** - Внешняя система, отправляющая сообщения в Kafka топик
3. **Consumer** (`app/consumer.py`) - Асинхронный Python скрипт, который:
   - Читает сообщения из Kafka топика асинхронно
   - Для каждого сообщения вызывает flow для обработки
   - Может работать в двух режимах:
     - **Локальный flow** (`app/test_flow.py`) - использует flow как библиотеку
     - **Langflow API** - вызывает Langflow через HTTP API
   - Обрабатывает несколько сообщений параллельно (с ограничением)
   - Обрабатывает ошибки и логирует результаты
4. **Test Flow** (`app/test_flow.py`) - Flow-процессор, который:
   - Обрабатывает сообщения из Kafka
   - Может использовать Langflow SDK для сложной обработки
   - Имеет fallback на простую обработку, если Langflow недоступен

## Установка

1. Установите зависимости:
```bash
uv sync
```

2. Запустите Kafka брокер:
```bash
docker-compose up -d
```

3. Настройте переменные окружения (создайте `.env` файл или экспортируйте переменные):
```bash
# Kafka настройки
export KAFKA_TOPIC=test-topic
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_GROUP_ID=langflow-consumer-group

# Langflow настройки
export LANGFLOW_BASE_URL=http://localhost:7860
export LANGFLOW_FLOW_ID=your-flow-id-here
export LANGFLOW_API_KEY=your-api-key-here  # Опционально

# Производительность
export MAX_CONCURRENT_REQUESTS=10  # Максимальное количество одновременных запросов к Langflow

# Режим работы consumer
export USE_LOCAL_FLOW=false  # true - использовать локальный flow из test_flow.py, false - использовать Langflow API
```

## Использование

### Запуск Consumer

```bash
uv run python app/consumer.py
```

Consumer будет:
- Подключаться к Kafka и асинхронно читать сообщения из указанного топика
- Для каждого сообщения вызывать flow для обработки:
  - Если `USE_LOCAL_FLOW=true` - использует локальный flow из `test_flow.py`
  - Если `USE_LOCAL_FLOW=false` - вызывает Langflow через HTTP API
- Обрабатывать несколько сообщений параллельно (до `MAX_CONCURRENT_REQUESTS`)
- Логировать все операции и ошибки

**Важно:** Consumer работает в асинхронном режиме, что позволяет обрабатывать сообщения параллельно и не блокировать чтение новых сообщений во время ожидания ответа от flow.

### Режимы работы Consumer

#### 1. Локальный Flow (USE_LOCAL_FLOW=true)
Использует flow из `test_flow.py` напрямую как библиотеку:
```bash
export USE_LOCAL_FLOW=true
export LANGFLOW_FLOW_PATH=flows/simple_test_flow.json  # Опционально, для использования Langflow SDK
uv run python app/consumer.py
```

#### 2. Langflow API (USE_LOCAL_FLOW=false, по умолчанию)
Вызывает Langflow через HTTP API:
```bash
export USE_LOCAL_FLOW=false
export LANGFLOW_BASE_URL=http://localhost:7860
export LANGFLOW_FLOW_ID=your-flow-id
uv run python app/consumer.py
```

### Тестирование с Producer

В отдельном терминале запустите producer для отправки тестовых сообщений:

```bash
uv run python app/producer.py
```

## Формат сообщений

Consumer поддерживает два формата сообщений:

1. **JSON формат** - будет распарсен и передан в Langflow как словарь:
```json
{"message": "Hello, Langflow!", "user_id": "123"}
```

2. **Текстовый формат** - будет передан как `{"message": "текст сообщения"}`

## Настройка Langflow

1. Убедитесь, что Langflow запущен и доступен по адресу `LANGFLOW_BASE_URL`
2. Создайте flow в Langflow UI
3. Получите Flow ID из URL или API Langflow
4. Установите `LANGFLOW_FLOW_ID` в переменных окружения

## Создание тестового Flow

Для тестирования интеграции можно создать тестовый flow двумя способами:

### 1. Создание Flow Definition (JSON)

Создайте JSON файл с определением flow:

```bash
uv run python app/create_test_flow.py
```

Этот скрипт создаст два файла в директории `flows/`:
- `simple_test_flow.json` - простой flow для базовой обработки
- `advanced_test_flow.json` - более сложный flow с дополнительной обработкой

Затем:
1. Импортируйте JSON файл в Langflow UI
2. Или используйте путь к файлу в переменной окружения `LANGFLOW_FLOW_PATH`

### 2. Тестирование Flow локально

Используйте скрипт `test_flow.py` для тестирования обработки сообщений:

```bash
uv run python app/test_flow.py
```

Этот скрипт:
- Тестирует обработку сообщений с примерами данных
- Может использовать Langflow SDK для локальной обработки (если установлен)
- Или использует простую обработку для тестирования логики

Переменные окружения для тестирования:
```bash
export LANGFLOW_FLOW_PATH=flows/simple_test_flow.json  # Путь к flow файлу
export USE_LANGFLOW=true  # Использовать Langflow SDK (если доступен)
```

## Структура проекта

```
kafka_exp/
├── app/
│   ├── consumer.py          # Consumer: читает из Kafka и вызывает flow
│   ├── producer.py          # Producer для тестирования
│   ├── test_flow.py          # Flow-процессор: обрабатывает сообщения (вызывается из consumer)
│   └── create_test_flow.py  # Скрипт для создания flow definition
├── flows/                   # Директория для flow definitions (создается автоматически)
├── docker-compose.yml       # Kafka и Zookeeper
├── pyproject.toml           # Зависимости проекта
└── README.md               # Документация
```

### Ответственность скриптов

1. **`consumer.py`** - читает сообщения из Kafka и вызывает flow для обработки
2. **`test_flow.py`** - реализует flow-логику обработки сообщений, вызывается из consumer

## Асинхронная обработка

Consumer использует асинхронный режим работы:
- **Асинхронное чтение из Kafka** - используется `aiokafka` для неблокирующего чтения
- **Асинхронные HTTP запросы** - используется `httpx` для параллельных запросов к Langflow
- **Параллельная обработка** - несколько сообщений могут обрабатываться одновременно
- **Семафор для ограничения** - количество одновременных запросов к Langflow ограничено параметром `MAX_CONCURRENT_REQUESTS`

Это позволяет эффективно обрабатывать большие объемы сообщений, не блокируя чтение новых сообщений из Kafka во время ожидания ответа от Langflow.

## Обработка ошибок

- Consumer логирует все ошибки с подробной информацией
- При ошибке обработки сообщения consumer продолжает работу
- Ошибки HTTP запросов к Langflow логируются с деталями ответа сервера
- Каждое сообщение обрабатывается независимо - ошибка одного сообщения не влияет на другие

## Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `KAFKA_TOPIC` | Название Kafka топика | `test-topic` |
| `KAFKA_BOOTSTRAP_SERVERS` | Адрес Kafka брокера | `localhost:9092` |
| `KAFKA_GROUP_ID` | ID consumer group | `langflow-consumer-group` |
| `LANGFLOW_BASE_URL` | Базовый URL Langflow API | `http://localhost:7860` |
| `LANGFLOW_FLOW_ID` | ID flow в Langflow (для режима API) | *обязательно при USE_LOCAL_FLOW=false* |
| `LANGFLOW_FLOW_PATH` | Путь к JSON файлу с flow definition (для локального flow) | *опционально* |
| `LANGFLOW_API_KEY` | API ключ для аутентификации | *опционально* |
| `MAX_CONCURRENT_REQUESTS` | Максимальное количество одновременных запросов | `10` |
| `USE_LOCAL_FLOW` | Использовать локальный flow из test_flow.py вместо API | `false` |

