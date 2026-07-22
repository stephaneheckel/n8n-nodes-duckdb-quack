INSTALL quack;
LOAD quack;
FROM quack_query('quack:82.29.172.40:9494', '
  DROP TABLE IF EXISTS test_alter;
', token := 'super_secret_token_1234', disable_ssl := true);
