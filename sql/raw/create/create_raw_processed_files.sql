CREATE TABLE IF NOT EXISTS raw.processed_files (
    filename TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE raw.processed_files IS 'Список уже обработанных файлов, чтобы избежать повторной загрузки';

COMMENT ON COLUMN raw.processed_files.filename IS 'Имя файла-источника, уникальный идентификатор (PK)';
COMMENT ON COLUMN raw.processed_files.processed_at IS 'Время, когда файл был успешно обработан и загружен';