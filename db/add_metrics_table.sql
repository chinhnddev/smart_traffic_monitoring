CREATE TABLE IF NOT EXISTS operational_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_type VARCHAR(50) NOT NULL,
    value FLOAT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_operational_metrics_timestamp ON operational_metrics (timestamp);
CREATE INDEX IF NOT EXISTS idx_operational_metrics_type ON operational_metrics (metric_type);