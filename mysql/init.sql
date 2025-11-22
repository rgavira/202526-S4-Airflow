-- Create Hive Metastore Database
CREATE DATABASE IF NOT EXISTS metastore_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create or replace Hive user
DROP USER IF EXISTS 'hive'@'%';
CREATE USER 'hive'@'%' IDENTIFIED BY 'password';

-- Grant all privileges
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%' WITH GRANT OPTION;

-- Flush privileges
FLUSH PRIVILEGES;