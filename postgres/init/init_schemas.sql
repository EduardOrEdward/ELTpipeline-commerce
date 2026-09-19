-- Initialize the database schemas used by the ELT pipeline.
-- PostgreSQL executes files in /docker-entrypoint-initdb.d
-- only when the database volume is initialized for the first time.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS mart;
