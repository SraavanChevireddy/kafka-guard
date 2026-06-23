-- Initial schema for kafka-guard.

CREATE TABLE cluster_policy (
    id          BIGSERIAL PRIMARY KEY,
    cluster     VARCHAR(255) NOT NULL UNIQUE,
    policy_json TEXT         NOT NULL,
    updated_by  VARCHAR(255) NOT NULL,
    updated_at  TIMESTAMP    NOT NULL
);

CREATE TABLE topic_request (
    id           BIGSERIAL PRIMARY KEY,
    cluster      VARCHAR(255) NOT NULL,
    topic_name   VARCHAR(512) NOT NULL,
    request_json TEXT         NOT NULL,
    status       VARCHAR(32)  NOT NULL,
    violations   TEXT,
    requested_by VARCHAR(255) NOT NULL,
    requested_at TIMESTAMP    NOT NULL,
    decided_by   VARCHAR(255),
    decided_at   TIMESTAMP
);

CREATE INDEX idx_treq_cluster ON topic_request (cluster);
