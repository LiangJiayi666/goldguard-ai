#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/01_可运行Web产品_离线演示"
URL="http://127.0.0.1:8765"
( sleep 1; command -v open >/dev/null && open "$URL" || (command -v xdg-open >/dev/null && xdg-open "$URL") ) &
exec python3 run.py
