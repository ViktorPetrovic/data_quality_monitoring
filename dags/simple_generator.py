from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import numpy as np
import random
from airflow.providers.standard.operators.python import PythonOperator
import json
from pathlib import Path




def simple_generator(**context):
    num_records = 100
    anomaly_rate = 0.1

    cities = ["Moscow", "London", "New York", "Tokyo"]

    sensor_ids = [f'SENSOR_{i:03d}' for i in range(num_records)]

    data_dir = Path("/otp/airflow/data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    temperatures = []
    humidities = []
    pressures = []
    sensor_list = []
    city_list = []
    timestamps = []

    for i in range(num_records):
        temp = np.random.normal(20, 5)
        humidity = np.random.normal(50, 15)
        pressure = np.random.normal(1013, 8)

        if random.random() < anomaly_rate:
            anomaly_type = random.choice(['negative_temp', 'high_humidity'])
            if anomaly_type == 'negative_temp':
                temp = random.uniform(-30, -10)
            else:
                humidity=random.uniform(120, 200)
    temperatures.append(round(temp, 2))
    pressures.append(round(pressure, 2))
    humidities.append(round(humidity, 2))
    sensor_list.append(random.choice(sensor_ids))
    city_list.append(random.choice(cities))
    timestamps.append(datetime.now().isoformat)


    df = pd.DataFrame({
        'sensor_id': sensor_list,
        'city': city_list,
        'temperature': temperatures,
        'pressure': pressures,
        'humidity': humidities,
        'date': timestamps,
        })

    date_str = datetime.now().strptime('%Y%m%d')
    file_name = f"sensor_data_{date_str}.csv"
    file_path = data_dir / file_name

    df.to_csv(file_path, index=False)

    print(f'Сгенерировано {len(df)} записей в {file_path}')

    return str(file_path)



default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG (
    
    dag_id="simple_generator",
    default_args=default_args,
    description="Генерируем метеоданные",
    schedule= "@daily",
    catchup=False,
    ) as dag:
    generate_task = PythonOperator(
        task_id = "generate_data",
        python_callable=simple_generator,
        )

    generate_task