from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Optional,Union
from uuid import UUID


def get_surrogate_key(
    table: str,
    raw_value: Union[str, UUID],
    pg: PostgresHook,
    search_column: Optional[str] = None,
    key_column: Optional[str] = None
) -> Optional[Union[int, UUID]]:
    """
    Возвращает surrogate key из DIM таблицы по значению ключа.

    :param table: Имя DIM таблицы (например, 'dim_user', 'dim_date').
    :param raw_value: Значение для поиска surrogate key.
    :param pg: Экземпляр PostgresHook для выполнения запроса.
    :param search_column: Имя столбца для поиска (если None, будет определено по умолчанию).
    :param key_column: Имя столбца surrogate key (если None, будет определено по умолчанию).
    :return: surrogate key (int или UUID) или None, если не найдено.
    """
    if raw_value is None:
        return None

    if not search_column or not key_column:
        if table == 'dim_date':
            search_column = 'date'
            key_column = 'id'
        elif table == 'dim_user':
            search_column = 'user_id'
            key_column = 'user_id'
        else:
            entity = table.replace('dim_', '')
            search_column = f"{entity}_id"
            key_column = 'id'

    sql = f"SELECT {key_column} FROM dds.{table} WHERE {search_column} = %s"
    params = (str(raw_value),)
    result = pg.get_first(sql, parameters=params)

    return result[0] if result else None




if __name__ == "__main__":
    pass