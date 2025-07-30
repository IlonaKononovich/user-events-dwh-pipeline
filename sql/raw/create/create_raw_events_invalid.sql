CREATE TABLE IF NOT EXISTS raw.events_invalid (
    id BIGSERIAL PRIMARY KEY,            
    filename TEXT NOT NULL,              
    event_id UUID NULL,                  
    event_time TIMESTAMP NULL,           
    error_message TEXT NOT NULL,        
    raw_payload JSONB NOT NULL,          
    loaded_at TIMESTAMP DEFAULT now()    
);
