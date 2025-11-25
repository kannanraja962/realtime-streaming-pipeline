-- Create aggregated events table
CREATE TABLE IF NOT EXISTS aggregated_events (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    event_type VARCHAR(50),
    event_count BIGINT,
    avg_value DECIMAL(10, 2),
    total_value DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX idx_window_start ON aggregated_events(window_start);
CREATE INDEX idx_event_type ON aggregated_events(event_type);

-- Create raw events table (optional - for debugging)
CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR(50) PRIMARY KEY,
    event_type VARCHAR(50),
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    timestamp TIMESTAMP,
    value DECIMAL(10, 2),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
