CREATE TABLE IF NOT EXISTS dlq_messages (
    id SERIAL PRIMARY KEY,
    message_id UUID NOT NULL UNIQUE,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    error TEXT,
    retry_count INT DEFAULT 0,
    status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_messages (
    id SERIAL PRIMARY KEY,
    message_id UUID NOT NULL UNIQUE,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS failed_messages (
    id SERIAL PRIMARY KEY,
    message_id UUID NOT NULL UNIQUE,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    error TEXT NOT NULL,
    retry_count INT NOT NULL,
    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dlq_messages_status ON dlq_messages(status);
CREATE INDEX IF NOT EXISTS idx_dlq_messages_timestamp ON dlq_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_messages_topic ON processed_messages(topic);
CREATE INDEX IF NOT EXISTS idx_failed_messages_topic ON failed_messages(topic);