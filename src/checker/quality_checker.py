import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class QualityChecker:
    def __init__(self):
        self.rules = {
            'temperature':{'min': -10, 'max':40},
            'humidity':{'min': 0, 'max': 100},
            'pressure':{'min': 950, 'max': 1100}
            }
        logger.info("Класс проверки качества создан")
        
    def quality_check(self, df:pd.DataFrame) -> dict:
        total_records = len(df)
        logger.info(f"Начинаю проверку {total_records} записей")
        nulls ={}
        for col in ['temperature', 'humidity', 'pressure']:
            nulls[f'nulls_{col}'] = int(df[col].isna().sum())
        nulls['total_nulls'] = int(sum(nulls.values()))
        nulls['nulls_percent'] = float(round(nulls['total_nulls'] / (total_records * 3) * 100, 2)) if total_records > 0 else 0

        invalid = {}
        for col, limits in self.rules.items():
            invalid[f'invalid_{col}'] = int(((df[col] < limits['min']) | (df[col] > limits['max'])).sum())
        invalid['total_invalid'] = int(sum(invalid.values()))
        invalid['invalid_percent'] = float(round(invalid['total_invalid'] / (total_records * 3) * 100, 2)) if total_records > 0 else 0

        quality_score = 100 
        quality_score -= nulls['nulls_percent']
        quality_score -= invalid['invalid_percent']
        quality_score = round(max(0, min(100, quality_score)), 2)


       
        return {
            'total_records':int(total_records),
            **nulls,
            **invalid,
            'quality_score': float(quality_score),
            'status': 'PASS' if quality_score >= 80 else 'FAIL'
    
            }

    def save_to_json(self, metrics: dict, filepath: Path) -> Path:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=4, ensure_ascii=False)
        logger.info(f"Данные проверены метрики записаны в {filepath}")
        return filepath

    def print_report(self, metrics: dict):
        logger.info("Отчёт о качестве данных")
        logger.info(f"Дата: {metrics.get('date', 'N/A')}")
        logger.info(f"Записей: {metrics.get('total_records', 0)}")
        logger.info(f"Пропусков: {metrics.get('nulls_percent', 0)}%")
        logger.info(f"Некорректных: {metrics.get('invalid_percent', 0)}%")
        logger.info(f"Оценка: {metrics.get('quality_score', 0)}/100")

        if metrics.get('status') == 'PASS':
            logger.info("Данные успешно прошли проверку")
        else:
            logger.warning("FAIL данные требуют рассмотрения")
            if metrics.get('null_percent', 0) > 10:
                logger.warning(f"Слишком много пропусков: {metrics['null_percent']}%")
            if metrics.get('invalid_percent', 0) > 5:
                logger.warning(f"Слишком много некорректных значений: {metrics['invalid_percent']}%")
