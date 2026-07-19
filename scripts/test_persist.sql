INSTALL quack;
LOAD quack;
FROM quack_query('quack:172.30.87.150:9494', '
  DETACH _preflight;
', token := 'test', disable_ssl := true);
