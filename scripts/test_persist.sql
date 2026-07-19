INSTALL quack;
LOAD quack;
FROM quack_query('quack:172.30.87.150:9494', '
  SELECT * FROM nonexistent_table;
', token := 'test', disable_ssl := true);
