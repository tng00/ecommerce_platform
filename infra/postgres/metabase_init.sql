-- infra/postgres/metabase_init.sql
CREATE USER metabase_user WITH PASSWORD 'metabase_password';
CREATE DATABASE metabase_db OWNER metabase_user;
GRANT ALL PRIVILEGES ON DATABASE metabase_db TO metabase_user;