#!/bin/bash
export PATH="$HOME/.duckdb/cli/latest:$PATH"

echo "Starting Quack server on WSL2..."

duckdb -c "
INSTALL quack;
LOAD quack;
CREATE TABLE IF NOT EXISTS products AS SELECT * FROM (VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.50), (3, 'Thing', 3.75)) t(id, name, price);
CALL quack_serve('quack:0.0.0.0:9494', token='test', allow_other_hostname:=true);
" &

PID=$!
sleep 2

if ps -p $PID > /dev/null 2>&1; then
    echo "Server PID: $PID — running"
else
    echo "Server failed to start (PID $PID)"
    exit 1
fi

echo ""
echo "n8n credential:"
echo "  URI:  quack:$(hostname -I | awk '{print $1}'):9494"
echo "  Token: test"
echo "  Disable SSL: checked"
echo ""
echo "Press Ctrl+C to stop."

trap "kill $PID 2>/dev/null; exit" INT TERM
wait $PID
