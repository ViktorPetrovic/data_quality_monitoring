import json
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

logging.basicConfig(

    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S', 
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'), 
        logging.StreamHandler() 
    ]
)


def data_generator(**context):
    logging.info("Начинаю генерацию данных")
    try:
        num_records = 100
        anomaly_rate = 0.1

        cities = ["Moscow", "London", "New York", "Tokyo"]

        sensor_ids = [f'SENSOR_{i:03d}' for i in range(10)]

        data_dir = Path("/opt/airflow/data/raw")
        data_dir.mkdir(parents=True, exist_ok=True)

        temperatures = []
        humidities = []
        pressures = []
        sensor_list = []
        city_list = []
        timestamps = []
        logging.info(f"Генерирую {num_records} записей с {anomaly_rate * 100} % аномалий")
        for i in range(num_records):
            temp = np.random.normal(20, 5)
            humidity = np.random.normal(50, 15)
            pressure = np.random.normal(1013, 8)

            if random.random() < anomaly_rate:
                anomaly_type = random.choice(['negative_temp', 'high_humidity', 'missing_values'])
                if anomaly_type == 'negative_temp':
                    temp = random.uniform(-30, -10)
                elif anomaly_type == 'high_humidity':
                    humidity=random.uniform(120, 200)
                elif anomaly_type == 'missing_values':
                    temp = np.nan
                    humidity= np.nan
                    pressure = np.nan
            temperatures.append(round(temp, 2) if not np.isnan(temp) else None)
            pressures.append(round(pressure, 2) if not np.isnan(pressure) else None)
            humidities.append(round(humidity, 2) if not np.isnan(humidity) else None)
            sensor_list.append(random.choice(sensor_ids))
            city_list.append(random.choice(cities))
            timestamps.append(datetime.now().isoformat())


        df = pd.DataFrame({
            'sensor_id': sensor_list,
            'city': city_list,
            'temperature': temperatures,
            'pressure': pressures,
            'humidity': humidities,
            'date': timestamps,
            })

        date_str = datetime.now().strftime('%Y%m%d')
        file_name = f"sensor_data_{date_str}.csv"
        file_path = data_dir / file_name

        df.to_csv(file_path, index=False)

        logging.info(f"Сгенерировано {len(df)} записей в {file_path}")

        return str(file_path)

    except Exception as e:
        logging.error(f"Генерация данных прервана с ошибкой {e!s}")
        raise


def check_data_quality(**context):

    logging.info("Запуск проверки качества")
    try:
        filepath = context['task_instance'].xcom_pull(task_ids="generate_data")
        logging.info(f"Получен путь из XCom {filepath}")

    
        if not filepath:
            raise ValueError("Не удалось получить путь к файлу из предыдущей задачи")

        filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"Файл не найден: {filepath}")

        logging.info(f"Обрабатываю {filepath}")

        df = pd.read_csv(filepath)

        total_records = len(df)
        logging.info(f"Загружено записей - {total_records}")

        null_temperature = df['temperature'].isna().sum()
        null_humidity = df['humidity'].isna().sum()
        null_pressure = df['pressure'].isna().sum()
        total_nulls = null_temperature + null_humidity + null_pressure

        logging.info(f"Пропуски в температуре - {null_temperature}")
        logging.info(f"Пропуски в давлении - {null_pressure}")
        logging.info(f"Пропуски во влажности - {null_humidity}")
        logging.info(f"Общее колличество пропусков - {total_nulls}")

        invalid_temp = ((df['temperature'] < 10) | (df['temperature'] > 40)).sum()
        invalid_humidity = ((df['humidity'] < 0) | (df['humidity'] > 100)).sum()
        invalid_pressure = ((df['pressure'] < 950) | (df['pressure'] > 1100)).sum()  
        total_invalid = invalid_temp + invalid_humidity + invalid_pressure

        logging.info(f"Некорректная температура - {invalid_temp}")
        logging.info(f"Некорректное давление - {invalid_pressure}")
        logging.info(f"Некорректная влажность - {invalid_humidity}")
        logging.info(f"Общее колличество некорректных значеий - {total_invalid}")


        logging.info("Расчет показаний качества")
        null_percent = (total_nulls / (total_records * 3)) * 100 if total_records > 0 else 0

        invalid_percent = (total_invalid / (total_records * 3)) * 100 if total_records > 0 else 0

        quality_score = 100 
        quality_score -= null_percent
        quality_score -= invalid_percent
        quality_score = round(max(0, min(100, quality_score)), 2)
        logging.info(f"Пропуски - {null_percent} %")
        logging.info(f"Некоректные - {invalid_percent} %")
        logging.info(f"Оценка качества {quality_score}/100 %")

        logging.info("Сохранение данных")
        metrics = {
        'data': datetime.now().strftime('%Y%m%d'),
        'file':str(filepath),
        'total_records':total_records,
        'null_temperature':int(null_temperature),
        'null_humidity':int(null_humidity),
        'null_pressure':int(null_pressure),
        'total_nulls':int(total_nulls),
        'null_percent':round(null_percent, 2),
        'invalid_temp':int(invalid_temp),
        'invalid_humidity':int(invalid_humidity),
        'invalid_pressure':int(invalid_pressure),
        'total_invalid':int(total_invalid),
        'invalid_percent':round(invalid_percent, 2),
        'quality_score':int(quality_score),
        'status': 'PASS' if quality_score > 80 else 'FAIL'

        }

        metrics_dir = Path("/opt/airflow/data/metrics")

        metrics_dir.mkdir(parents=True, exist_ok=True)

        date_dir = datetime.now().strftime('%Y%m%d')

        metrics_file = metrics_dir / f"metrics{date_dir}.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=4)
        logging.info(f"Метрики сохранены в {metrics_file}")
        logging.info("Отчёт о качестве данных")
        logging.info(f"Дата: {metrics['data']}")
        logging.info(f"Колличество записей: {total_records}")
        logging.info(f"Пропусков: {metrics['null_percent']}%")
        logging.info(f"Некорректных: {metrics['invalid_percent']}%")
        logging.info(f"Оценка: {metrics['quality_score']}/100")

        if metrics['status'] == 'PASS':
            logging.info("Данные успешно прошли проверку")
        else:
            logging.warning("FAIL Данные требуют рассмотрения")
            if metrics['null_percent'] > 10:
                logging.warning(f"Слишком много пропусков: {metrics['null_percent']}%")
            if metrics['invalid_percent'] > 5:
                logging.warning(f"Слишком много некорректных значений: {metrics['invalid_percent']}%")

        logging.info("Проверка завершена")
        
        return metrics

    except Exception as e:
        logging.error(f"Ошибка при проверке качества - {e!s}")
        raise


def send_alert(**context):
    logging.info("Проверка алертов")
    try:
        metrics = context['task_instance'].xcom_pull(task_ids="check_quality")

        if not metrics:
            logging.warning("Метрики не получены. Пропуск алертов.")
            return
        logging.info("Получены метрики из XCom")

        quality_score = metrics.get('quality_score', 0)
        status = metrics.get('status', 'Unknown')

        logging.info(f"Оценка качества - {quality_score}/100")
        logging.info(f"Статус - {status}")

        alert_level = 'None'
        alert_messages = []

        if quality_score < 50:
            alert_level = 'CRITICAL'
            alert_messages.append(f"Критическое качество данных: {quality_score}/100")
            alert_messages.append(f"Пропуски: {metrics.get('null_percent', 0)}%")
            alert_messages.append(f"Некоректных данных: {metrics.get('invalid_percent', 0)}%")
        elif quality_score < 80:
            alert_level = 'WARNING'
            alert_messages.append(f"Качество данных требует внимания: {quality_score}/100")
            alert_messages.append(f"Пропуски: {metrics.get('null_percent', 0)}%")
            alert_messages.append(f"Некоректных данных: {metrics.get('invalid_percent', 0)}%")
        else:
            alert_messages.append(f"Качество данных хорошее: {quality_score}/100")

        logging.info(f"Уровень алерта: {alert_level}")

        for messages in alert_messages:
            if alert_level == 'CRITICAL':
                logging.error(messages)
            elif alert_level == 'WARNING':
                logging.warning(messages)
            else:
                logging.info(messages)

        # Чтобы отправить алерт в Telegram, раскомментируйте код ниже
        # и добавьте ваши токены в .env
        #
        # if alert_level != 'NONE':
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

        logging.info("Проверка алертов завершена")

        return {
            'alert_level':alert_level,
            'alert_messages':alert_messages,
            'quality_score': quality_score
            }

    except Exception as e:
        logging.error(f"Ошибка при отправке алертов - {e!s}")
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
    dag_id="pipeline",
    default_args=default_args,
    description="Пайплайн для генерации и проверки качества данных",
    schedule= "@daily",
    catchup=False,
    ) as dag:


    generate_data = PythonOperator(
        task_id = "generate_data",
        python_callable=data_generator,
        )

    check_quality = PythonOperator(
        task_id = "check_quality",
        python_callable=check_data_quality
        )

    alert_task = PythonOperator(
        task_id = "send_alert",
        python_callable=send_alert)

    generate_data >> check_quality >> alert_task