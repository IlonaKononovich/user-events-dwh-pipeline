CREATE TABLE IF NOT EXISTS raw.events_invalid (
    id BIGSERIAL PRIMARY KEY,            
    filename TEXT NOT NULL,              
    event_id UUID NULL,                  
    event_time TIMESTAMP NULL,           
    error_message TEXT NOT NULL,        
    raw_payload JSONB NOT NULL,          
    loaded_at TIMESTAMP DEFAULT now()    
);

COMMENT ON TABLE raw.events_invalid IS 'Хранилище невалидных событий, не прошедших валидацию';

COMMENT ON COLUMN raw.events_invalid.id IS 'Уникальный идентификатор записи (PK)';
COMMENT ON COLUMN raw.events_invalid.filename IS 'Имя файла-источника с сырыми данными';
COMMENT ON COLUMN raw.events_invalid.event_id IS 'UUID события (может отсутствовать, если не считался)';
COMMENT ON COLUMN raw.events_invalid.event_time IS 'Время события (как было в исходном JSON)';
COMMENT ON COLUMN raw.events_invalid.error_message IS 'Причина, по которой событие признано невалидным';
COMMENT ON COLUMN raw.events_invalid.raw_payload IS 'Полный JSON исходного события';
COMMENT ON COLUMN raw.events_invalid.loaded_at IS 'Время загрузки записи в таблицу';
