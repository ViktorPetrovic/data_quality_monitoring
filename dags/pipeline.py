import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.append('/opt/airflow')
import logging

from src.checker.quality_checker import QualityChecker
from src.generators.data_generators import DataGenerator

logger = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data/raw")
METRICS_DIR = Path("/opt/airflow/data/metrics")

def generate_data(**context):
    logger.info("Начинаю генерацию данных")
    try:
        generator = DataGenerator(num_sensor=15, seed=42)

        df = generator.generate_dataset(total_records=100, anomaly_rate=0.1)
        logger.info(f"Сгенерировано {len(df)} записей")

        execution_date = context.get('execution_date', datetime.now())
        date_str = execution_date.strftime('%Y%m%d')
        file_name = f"sensor_data_{date_str}.csv"
        filepath = DATA_DIR / file_name

        logger.info(f"Сохраняю в: {filepath}")
        filepath = generator.dataframe_to_csv(df=df, filepath=filepath)
        logger.info(f"Данные сохранены в: {filepath}")

        return str(filepath)
    
    except Exception as e:
        logger.error(f"Генерация данных прервана с ошибкой {e!s}")
        raise


def check_data_quality(**context):

    logger.info("Запуск проверки качества")
    try:
        filepath = context['task_instance'].xcom_pull(task_ids="generate_data")
        logger.info(f"Получен путь из XCom {filepath}")

    
        if not filepath:
            raise ValueError("Не удалось получить путь к файлу из предыдущей задачи")

        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"Файл не найден: {filepath}")

        logger.info(f"Обрабатываю {filepath}")

        df = pd.read_csv(filepath)

        checker = QualityChecker()
        metrics = checker.quality_check(df=df)

        execution_date = context.get('execution_date', datetime.now())
        date_dir = execution_date.strftime('%Y%m%d')

        metrics['file'] = str(filepath)
        metrics['date'] = date_dir

        metrics_file = METRICS_DIR / f"metrics_{date_dir}.json"
        checker.save_to_json(metrics=metrics, filepath=metrics_file)

        checker.print_report(metrics=metrics)
        logger.info("Проверка завершена")
        
        return metrics

    except Exception as e:
        logger.error(f"Ошибка при проверке качества - {e!s}")
        raise


def send_alert(**context):
    logger.info("Проверка алертов")
    try:
        metrics = context['task_instance'].xcom_pull(task_ids="check_quality")

        if not metrics:
            logger.warning("Метрики не получены. Пропуск алертов.")
            return
        logger.info("Получены метрики из XCom")

        quality_score = metrics.get('quality_score', 0)
        status = metrics.get('status', 'Unknown')

        logger.info(f"Оценка качества - {quality_score}/100")
        logger.info(f"Статус - {status}")

        alert_level = 'None'
        alert_messages = []

        if quality_score < 50:
            alert_level = 'КРИТИЧЕСКОЕ'
            alert_messages.append(f"Критическое качество данных: {quality_score}/100")
            alert_messages.append(f"Пропуски: {metrics.get('null_percent', 0)}%")
            alert_messages.append(f"Некоректных данных: {metrics.get('invalid_percent', 0)}%")
        elif quality_score < 80:
            alert_level = 'Предупреждение'
            alert_messages.append(f"Качество данных требует внимания: {quality_score}/100")
            alert_messages.append(f"Пропуски: {metrics.get('null_percent', 0)}%")
            alert_messages.append(f"Некоректных данных: {metrics.get('invalid_percent', 0)}%")
        else:
            alert_messages.append(f"Качество данных хорошее: {quality_score}/100")

        logger.info(f"Уровень алерта: {alert_level}")

        for messages in alert_messages:
            if alert_level == 'КРИТИЧЕСКОЕ':
                logger.error(messages)
            elif alert_level == 'Предупреждение':
                logger.warning(messages)
            else:
                logger.info(messages)

        # Чтобы отправить алерт в Telegram, раскомментируйте код ниже
        # и добавьте ваши токены в .env
        #
        # if alert_level != 'None':
        #     import requests
        #     import os
        #     from dotenv import load_dotenv
        #     
        #     load_dotenv()
        #     BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
        #     CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
        #     
        #     if BOT_TOKEN and CHAT_ID:
        #         message = "\n".join(alert_messages)
        #         url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        #         response = requests.post(url, json={
        #             'chat_id': CHAT_ID,
        #             'text': message,
        #             'parse_mode': 'HTML'
        #         })
        #         if response.status_code == 200:
        #             logger.info("Алерт отправлен в Telegram")
        #         else:
        #             logger.error(f"Ошибка отправки в Telegram: {response.text}")

        logger.info("Проверка алертов завершена")

        return {
            'alert_level':alert_level,
            'alert_messages':alert_messages,
            'quality_score': quality_score
            }

    except Exception as e:
        logger.error(f"Ошибка при отправке алертов - {e!s}")
        raise

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG (   
    dag_id="data_quality_pipeline",
    default_args=default_args,
    description="Пайплайн для генерации и проверки качества данных",
    schedule= "@daily",
    catchup=False,
    ) as dag:


    generate_task = PythonOperator(
        task_id = "generate_data",
        python_callable=generate_data,
        )

    check_quality_task = PythonOperator(
        task_id = "check_quality",
        python_callable=check_data_quality
        )

    alert_task = PythonOperator(
        task_id = "send_alert",
        python_callable=send_alert)

    generate_task >> check_quality_task >> alert_task