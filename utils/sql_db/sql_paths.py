"""
Конфигурация путей к SQL-скриптам проекта.

Здесь задаются абсолютные пути к файлам с SQL для RAW и DDS слоёв,
а также подгружается содержимое некоторых SQL-файлов для удобства использования.
"""

import os
from utils.sql_db.sql_utils import read_sql_file

# Корневая директория проекта
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
# Пути к RAW SQL скриптам
BASE_SQL_RAW_CREATE = os.path.join(BASE_DIR, 'sql', 'raw', 'create')
BASE_SQL_RAW_INSERT = os.path.join(BASE_DIR, 'sql', 'raw', 'insert')
BASE_SQL_RAW_SELECT = os.path.join(BASE_DIR, 'sql', 'raw', 'select')

# Пути к DDS SQL скриптам
BASE_SQL_DDS_CREATE = os.path.join(BASE_DIR, 'sql', 'dds', 'create')
BASE_SQL_DDS_INSERT = os.path.join(BASE_DIR, 'sql', 'dds', 'insert')

# RAW schema
SQL_CREATE_RAW_SCHEMA = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_schema.sql')

# DDS schema
SQL_CREATE_DDS_SCHEMA = os.path.join(BASE_SQL_DDS_CREATE, 'create_dds_schema.sql')

# RAW таблицы создания
SQL_CREATE_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_events.sql')
SQL_CREATE_PROCESSED = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_processed_files.sql')
SQL_CREATE_PROCESSED_EVENTS = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_processed_events.sql')
SQL_CREATE_EVENTS_INVALID = os.path.join(BASE_SQL_RAW_CREATE, 'create_raw_events_invalid.sql')

# RAW вставки
SQL_MARK_PROCESSED = os.path.join(BASE_SQL_RAW_INSERT, 'mark_file_processed.sql')
SQL_MARK_EVENT_PROCESSED = os.path.join(BASE_SQL_RAW_INSERT, 'mark_event_processed.sql')
SQL_INSERT_EVENT = os.path.join(BASE_SQL_RAW_INSERT, 'insert_raw_events.sql')
SQL_INSERT_EVENT_INVALID = read_sql_file(os.path.join(BASE_SQL_RAW_INSERT, 'insert_raw_events_invalid.sql'))

# RAW выборки
SQL_SELECT_PROCESSED_FILES = os.path.join(BASE_SQL_RAW_SELECT, 'select_processed_files.sql')
SQL_SELECT_NEW_EVENTS = os.path.join(BASE_SQL_RAW_SELECT, 'select_raw_events.sql')
