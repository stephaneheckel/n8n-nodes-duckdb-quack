INSTALL quack;
LOAD quack;
FROM quack_query('quack:172.30.87.150:9494', '
  CREATE TABLE test_alter (id INTEGER);
  ALTER TABLE test_alter ADD COLUMN name VARCHAR;
  DROP TABLE test_alter;
', token := 'test', disable_ssl := true);
