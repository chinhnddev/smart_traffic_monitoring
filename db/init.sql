-- Initialize core tables for smart_traffic_monitoring

-- Main results table
CREATE TABLE IF NOT EXISTS crowd_results (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    location_id VARCHAR(100) NOT NULL,
    frame_id VARCHAR(150) UNIQUE NOT NULL,
    crowd_count INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crowd_results_timestamp ON crowd_results (timestamp);
CREATE INDEX IF NOT EXISTS idx_crowd_results_location_id ON crowd_results (location_id);
CREATE INDEX IF NOT EXISTS idx_crowd_results_frame_id ON crowd_results (frame_id);

-- Soft pause control table
CREATE TABLE IF NOT EXISTS pipeline_control (
    id SERIAL PRIMARY KEY,
    status VARCHAR(50) NOT NULL DEFAULT 'RUNNING',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO pipeline_control (id, status)
VALUES (1, 'RUNNING')
ON CONFLICT (id) DO NOTHING;

-- Operational metrics table for monitoring system performance
CREATE TABLE IF NOT EXISTS operational_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_type VARCHAR(50) NOT NULL,  -- 'throughput', 'latency_avg', 'latency_p95', 'drop_rate', 'alert_rate'
    value FLOAT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,  -- Additional info (location_id, window_size, etc.)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_operational_metrics_timestamp ON operational_metrics (timestamp);
CREATE INDEX IF NOT EXISTS idx_operational_metrics_type ON operational_metrics (metric_type);



