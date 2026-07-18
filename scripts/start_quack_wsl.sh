#!/bin/bash
export PATH="$HOME/.duckdb/cli/latest:$PATH"

echo "Starting Quack server on WSL2..."

# Pipe commands to duckdb, then sleep infinity keeps stdin open
# so duckdb stays alive after executing the SQL.
(echo "
INSTALL quack;
LOAD quack;
CREATE TABLE IF NOT EXISTS products AS SELECT * FROM (VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.50), (3, 'Thing', 3.75)) t(id, name, price);
CALL quack_serve('quack:0.0.0.0:9494', token='test', allow_other_hostname:=true);
"; sleep infinity) | duckdb &
PID=$!

echo -n "Waiting for port 9494"
for i in $(seq 1 30); do
  if nc -z localhost 9494 2>/dev/null; then
    echo " OK"
    break
  fi
  if ! ps -p $PID > /dev/null 2>&1; then
    echo ""
    echo "Server process died unexpectedly (PID $PID)"
    exit 1
  fi
  echo -n "."
  sleep 1
done

if ! ps -p $PID > /dev/null 2>&1; then
  echo "Server failed to start within 30s"
  exit 1
fi

echo ""
echo "Server PID: $PID — running on port 9494"
echo ""
echo "n8n credential:"
echo "  URI:  quack:localhost:9494"
echo "  Token: test"
echo "  Disable SSL: checked"
echo ""
echo "n8n credential (try localhost first; use IP if localhost fails):"
WSL_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
echo "  URI:  quack:$WSL_IP:9494"
echo "  (or quack:localhost:9494 if WSL2 forwarding is active)"
echo ""
echo "Press Ctrl+C to stop."
echo ""

trap "kill $PID 2>/dev/null; wait $PID 2>/dev/null; exit" INT TERM
wait $PID
