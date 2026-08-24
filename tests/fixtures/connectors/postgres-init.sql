CREATE USER epoch_replication WITH REPLICATION LOGIN PASSWORD 'epoch-replication-password';
CREATE DATABASE epoch_connectors OWNER epoch_replication;

\connect epoch_connectors

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    description TEXT NOT NULL
);
ALTER TABLE orders OWNER TO epoch_replication;
CREATE PUBLICATION epoch_orders FOR TABLE orders;
SELECT pg_create_logical_replication_slot('epoch_orders_slot', 'pgoutput');
INSERT INTO orders (id, description) VALUES (1, 'connector-conformance');
