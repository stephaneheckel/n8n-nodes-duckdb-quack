INSTALL quack;
LOAD quack;
FROM quack_query('quack:172.30.87.150:9494', 'SELECT 1', token := 'bad_token_1234', disable_ssl := true);
