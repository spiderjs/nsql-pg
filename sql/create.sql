-- DROP DATABASE IF EXISTS test;
-- CREATE DATABASE test;

DROP TABLE IF EXISTS test;

CREATE TABLE test(
  id SERIAL,
  v1 VARCHAR(20),
  v2 INTEGER,
  v3 BOOLEAN,
  v4 jsonb
);



