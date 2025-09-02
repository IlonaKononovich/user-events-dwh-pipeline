"""
Модуль dds_validation_and_loader

Функции для валидации данных и основной загрузки батчей событий из staging слоя.
"""

import logging
from typing import Any, Dict
from pydantic import ValidationError


def validate_data(model_dict: Dict[str, Any], model_name: str, data: Dict[str, Any]) -> bool:
    """
    Валидация данных по pydantic-модели.

    :param model_dict: Словарь моделей (pydantic).
    :param model_name: Имя модели для валидации.
    :param data: Словарь данных для проверки.
    :return: True, если валидация пройдена или модель не найдена; False при ошибках.
    """
    model = model_dict.get(model_name)
    if not model:
        logging.warning(f"validate_data: модель для {model_name} не найдена, пропускаем валидацию")
        return True
    try:
        model(**data)
        return True
    except ValidationError as e:
        logging.error(f"Валидация {model_name} не пройдена: {e}")
        return False