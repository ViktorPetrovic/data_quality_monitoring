import logging
import random
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataGenerator:
    def __init__(self, num_sensor: int = 15, seed: int = 42): 
        self.total_record = num_sensor
        random.seed(seed)
        np.random.seed(seed)

        self.cities = ["Moscow", "London", "New York", "Tokyo", "Paris"]
        self.sensor_ids = [f'SENSOR_{i:03d}' for i in range(num_sensor)]
        logger.info(f"Класс генератор данных создан с {num_sensor} сенсоров")

    def generate_record(self, anomaly_rate: float = 0.1)-> dict:
        sensor_id = random.choice(self.sensor_ids)
        city = random.choice(self.cities)
        data = datetime.now().strftime("%Y%m%d")

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
        return {
                'sensor_id': sensor_id,
                'city': city,
                'temperature': round(temp, 2) if not np.isnan(temp) else None,
                'pressure': round(pressure, 2) if not np.isnan(pressure) else None,
                'humidity': round(humidity, 2) if not np.isnan(humidity) else None,
                'date': data,
            }

    def generate_dataset(self, total_records: int = 100, anomaly_rate: float = 0.1)-> pd.DataFrame:
        records = [self.generate_record(anomaly_rate) for _ in range(total_records)]
        return pd.DataFrame(records)

    def dataframe_to_csv(self, df: pd.DataFrame, filepath: Path) -> Path:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(filepath, index=False, encoding='utf-8')
        return filepath
