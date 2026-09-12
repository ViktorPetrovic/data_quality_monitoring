# Data Quality Monitoring Pipeline

> ETL-пайплайн для мониторинга качества данных с датчиков. Проект показывает оркестрацию задач с Apache Airflow, генерацию тестовых данных с аномалиями и автоматическую проверку качества данных.

---

## Оглавление

- [Описание](#описание)
- [Архитектура](#архитектура)
- [Структура проекта](#структура-проекта)
- [Технологии](#технологии)
- [Установка и запуск](#установка-и-запуск)
- [Расписание и ручной запуск](#расписание-и-ручной-запуск)
- [Как это работает](#как-это-работает)
- [Пример работы](#пример-работы)
- [Добавление алертов в Telegram](#добавление-алертов-в-telegram)
- [Мониторинг и логи](#мониторинг-и-логи)
- [Устранение неполадок](#устранение-неполадок)
- [Полезные команды](#полезные-команды)

---

## Описание

Проект автоматизирует процесс проверки качества данных, поступающих с датчиков. Каждый день запускается DAG, который:

1. **Генерирует** тестовые данные с аномалиями (отрицательная температура, высокая влажность, пропуски)
2. **Проверяет** качество данных (пропуски, некорректные значения)
3. **Вычисляет** оценку качества (0–100) и определяет статус: PASS/FAIL
4. **Отправляет** алерты при низком качестве данных

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        DAG: data_quality_pipeline               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────┐                                        │
│  │  generate_data      │  → Генерирует CSV с аномалиями         │
│  │  (PythonOperator)   │  → Сохраняет в data/raw/               │
│  └─────────────────────┘                                        │
│           │                                                     │
│           │ XCom: filepath                                      │
│           ▼                                                     │
│  ┌─────────────────────┐                                        │
│  │  check_quality      │  → Проверяет качество                  │
│  │  (PythonOperator)   │  → Сохраняет JSON в data/metrics/      │
│  └─────────────────────┘                                        │
│           │                                                     │
│           │ XCom: metrics                                       │
│           ▼                                                     │
│  ┌─────────────────────┐                                        │
│  │  send_alert         │  → Логирует результат                  │
│  │  (PythonOperator)   │  → (Опционально: Telegram)             │
│  └─────────────────────┘                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Структура проекта

```
data-quality-project/
├── docker-compose.yml          # Docker Compose для Airflow 3.3.1 + PostgreSQL
├── .gitignore                  # Игнорируемые файлы
├── .env                        # Переменные окружения (токены, секреты)
├── requirements.txt            # Python зависимости
├── README.md                   # Документация
├── Dockerfile                  # Docker сборка
│
├── dags/                       # DAG-и Airflow
│   └── data_quality_pipeline.py
│
├── src/                        # Переиспользуемый код
│   ├── __init__.py
│   ├── generators/
│   │   ├── __init__.py
│   │   └── data_generators.py      # Генерация данных с аномалиями
│   └── checker/
│       ├── __init__.py
│       └── quality_checker.py      # Проверка качества данных
│
├── data/                       # Данные
│   ├── raw/                    # CSV файлы с датчиков
│   └── metrics/                # JSON с метриками качества
│
└── logs/                       # Логи Airflow
```

---

## Технологии

| Технология         | Версия    | Назначение                  |
|--------------------|-----------|-----------------------------|
| **Apache Airflow** | 3.3.1     | Оркестрация задач           |
| **PostgreSQL**     | 15-alpine | Хранение метаданных Airflow |
| **Python**         | 3.11      | Основной язык               |
| **Pandas**         | 3.0.5     | Обработка данных            |
| **NumPy**          | 2.4.6     | Генерация случайных данных  |
| **Docker**         | Latest    | Контейнеризация             |

---

## Установка и запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/ViktorPetrovic/data_quality_monitoring.git
cd data-quality-project
```

### 2. Создать файл `.env`

```bash
# Скопировать пример конфигурации
cp .env.example .env

# Отредактировать .env (если нужно)
nano .env  # или откройте в любом редакторе
```

### 3. Запустить Docker контейнеры

```bash
# Собрать образ и запустить контейнеры
docker-compose up -d --build

# Проверить, что все работает
docker-compose ps
```

**Ожидаемый результат:**

```
NAME                                 IMAGE                             COMMAND                  SERVICE              STATUS                   
data_quality_monitoring-airflow-1    apache/airflow:3.3.1-python3.11   "/usr/bin/dumb-init …"   airflow       Up 43 minutes (healthy)   
data_quality_monitoring-postgres-1   postgres:15-alpine                "docker-entrypoint.s…"   postgres      Up 43 minutes (healthy)   
```

### 4. Доступ к Airflow UI
> **Важно:** Пароль от доступа в Airflow UI для admin генерируется случайно при первом запуске.

- **URL:** http://localhost:8080
- **Логин:** `admin`
- **Пароль:** `сгенерируется автоматически. Чтобы узнать его в терминале Docker введите:`

```bash
docker-compose logs airflow | findstr "admin"
```
### 5. Запустить DAG

1. Открыть Airflow UI → `http://localhost:8080`
2. Найти DAG `data_quality_pipeline`
3. Включить DAG (переключатель ON)
4. Нажать кнопку "Trigger DAG"
> При первом запуске даг выполняется автоматически

---

## Расписание и ручной запуск

### Расписание

DAG запускается ежедневно в **00:00 UTC**:

> **Важно:** Время указано в UTC. Для Москвы (UTC+3) это **03:00**.

### Ручной запуск

1. Открой `http://localhost:8080`
2. Найди DAG `data_quality_pipeline`
3. Нажми кнопку "Trigger DAG"

**Через CLI:**

```bash
docker exec -it data_quality_monitoring-airflow-1 airflow dags trigger data_quality_pipeline
```

---

## Как это работает

### 1. Генерация данных (`generate_data`)

Создает 100 записей с датчиков с 10% аномалий:

| Аномалия                  | Пример |
|---------------------------|--------|
| Отрицательная температура | -25°C  |
| Высокая влажность         | 150%   |
| Пропуски (NaN)            | null   |

**Результат:** `data/raw/sensor_data_YYYYMMDD.csv`

### 2. Проверка качества (`check_data_quality`)

Анализирует данные по трем параметрам:

| Проверка    | Норма           | Аномалия              |
|-------------|-----------------|-----------------------|
| Температура | 10°C – 40°C     | < 10°C или > 40°C     |
| Влажность   | 0% – 100%       | < 0% или > 100%       |
| Давление    | 950 – 1100 мбар | < 950 или > 1100 мбар |

**Оценка качества (0–100):**
- `100` — идеальные данные
- `≥ 80` — PASS
- `< 80` — FAIL

**Результат:** `data/metrics/metrics_YYYYMMDD.json`

### 3. Алерты (`send_alert`)

| Оценка  | Уровень        |
|---------|----------------|
| ≥ 80    | PASS           |
| 50 – 79 | Предупреждение |
| < 50    | Критическое    |

---

## Пример работы

### CSV с данными

```csv
sensor_id,city,temperature,pressure,humidity,date
SENSOR_010,Moscow,22.48,1018.18,139.59,20260910
SENSOR_002,Moscow,27.62,1011.13,46.49,20260910
SENSOR_014,Paris,27.9,1009.24,122.54,20260910
SENSOR_001,London,22.71,1009.27,43.05,20260910
SENSOR_009,Moscow,21.21,999.2,21.3,20260910
SENSOR_011,Paris,17.19,1015.51,34.81,20260910
SENSOR_007,Paris,15.46,1024.73,28.82,20260910
```

### JSON с метриками

```json
{
    "total_records": 100,
    "nulls_temperature": 4,
    "nulls_humidity": 4,
    "nulls_pressure": 4,
    "total_nulls": 12,
    "nulls_percent": 4.0,
    "invalid_temperature": 5,
    "invalid_humidity": 5,
    "invalid_pressure": 0,
    "total_invalid": 10,
    "invalid_percent": 3.33,
    "quality_score": 92.67,
    "status": "PASS",
    "file": "/opt/airflow/data/raw/sensor_data_20260910.csv",
    "date": "20260910"
}
```
---

## Добавление алертов в Telegram

В коде `send_alert` закомментирована отправка алертов в Telegram. Чтобы включить:

### 1. Создайте бота в Telegram

1. Напишите [@BotFather](https://t.me/botfather)
2. Создайте бота: `/newbot`
3. Получите токен: `Ваш токен`

### 2. Получите Chat ID

1. Напишите боту любое сообщение
2. Отправьте запрос: `https://api.telegram.org/bot<Ваш токен>/getUpdates`
3. Получите `chat_id`

### 3. Добавьте токены в .env

```env
TELEGRAM_BOT_TOKEN=Ваш токен
TELEGRAM_CHAT_ID=Чат ID
```

### 4. Раскомментируйте код в `send_alert`

```python
if alert_level != 'None':
    import requests
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    
    if BOT_TOKEN and CHAT_ID:
        message = "\n".join(alert_messages)
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        response = requests.post(url, json={
            'chat_id': CHAT_ID,
            'text': message,
            'parse_mode': 'HTML'
        })
```

---

## Мониторинг и логи

### Логи контейнеров

```bash
# Логи Airflow
docker-compose logs airflow --tail=50

# Логи PostgreSQL
docker-compose logs postgres --tail=50

```

### Логи DAG в UI

1. Открыть DAG в Airflow UI
2. Нажать на конкретный Task
3. Выбрать **"Log"**

### Проверка данных

```bash
# Проверить CSV
docker exec -it data_quality_monitoring-airflow-1 ls -la /opt/airflow/data/raw/
docker exec -it data_quality_monitoring-airflow-1 cat /opt/airflow/data/raw/sensor_data_*.csv

# Проверить метрики
docker exec -it data_quality_monitoring-airflow-1 ls -la /opt/airflow/data/metrics/
docker exec -it data_quality_monitoring-airflow-1 cat /opt/airflow/data/metrics/metrics_*.json
```

### Проверка состояния

```bash
# Статус контейнеров
docker-compose ps

# Использование ресурсов
docker stats

# Проверить, что DAG загружен
docker exec -it data_quality_monitoring-airflow-1 airflow dags list
```

---

## Устранение неполадок

### Ошибка: `FileNotFoundError: /opt/airflow/data/raw/sensor_data_*.csv`

**Решение:** Проверьте, что файл существует:
```bash
docker exec -it data_quality_monitoring-airflow-1 ls -la /opt/airflow/data/raw/
```

### Порт 8080 уже занят

**Решение:** Освободите или измените порт в `docker-compose.yml`:
```yaml
ports:
  - "8081:8080" 
```

### Ошибка: `DAG seems to be missing`

**Решение:**
1. Проверьте, что файл находится в папке `dags/`
2. Проверьте ошибки импорта:
```bash
docker exec -it data_quality_monitoring-airflow-1 airflow dags list-import-errors
```
3. Перезапустите Airflow:
```bash
docker-compose restart airflow
```

---

## Полезные команды

### Управление контейнерами

```bash
# Запустить
docker-compose up -d

# Остановить
docker-compose down

# Перезапустить
docker-compose restart

# Пересобрать и запустить
docker-compose up -d --build

# Остановить и удалить все (включая данные)
docker-compose down -v
```

### Доступ к контейнеру Airflow

```bash
# Зайти в контейнер Airflow
docker exec -it data_quality_monitoring-airflow-1 bash

# Проверить установленные пакеты
docker exec -it data_quality_monitoring-airflow-1 pip list

# Проверить переменные окружения
docker exec data_quality_monitoring-airflow-1 env
```

### Работа с Airflow CLI

```bash
# Список всех DAG
docker exec -it data_quality_monitoring-airflow-1 airflow dags list

# Ошибки импорта DAG
docker exec -it data_quality_monitoring-airflow-1 airflow dags list-import-errors

# Статус запусков DAG
docker exec -it data_quality_monitoring-airflow-1 airflow dags list-runs data_quality_pipeline

# Запустить DAG
docker exec -it data_quality_monitoring-airflow-1 airflow dags trigger data_quality_pipeline

# Приостановить / возобновить DAG
docker exec -it data_quality_monitoring-airflow-1 airflow dags pause data_quality_pipeline
docker exec -it data_quality_monitoring-airflow-1 airflow dags unpause data_quality_pipeline
```

### Проверка данных в PostgreSQL

```bash
# Подключиться к PostgreSQL
docker exec -it data_quality_monitoring-postgres-1 psql -U airflow -d airflow
```

**Внутри `psql`:**

```sql
-- Посмотреть таблицы XCom
SELECT * FROM xcom ORDER BY id DESC LIMIT 10;

-- Посмотреть запуски DAG
SELECT dag_id, state, execution_date FROM dag_run 
WHERE dag_id = 'data_quality_pipeline' 
ORDER BY execution_date DESC LIMIT 10;

-- Выйти
\q
```

---

## Зависимости

### requirements.txt

```txt
apache-airflow==3.3.1
pandas==3.0.5
numpy==2.4.6
```

---

## Контакты

- **Автор:** [Александр]
- **Email:** [navselv3@yandex.ru]
- **GitHub:** [github.com/ViktorPetrovic](https://github.com/ViktorPetrovic)
---

## Благодарности

- [Apache Airflow](https://airflow.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Docker](https://www.docker.com/)
```