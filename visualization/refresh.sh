#!/usr/bin/env bash
# 刷新数据并打开可视化站点。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[1/2] 导出只读数据 → visualization/data.js"
uv run python visualization/export_data.py

echo "[2/2] 打开 visualization/index.html"
if command -v open >/dev/null 2>&1; then
  open visualization/index.html
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open visualization/index.html
else
  echo "请手动打开：$(pwd)/visualization/index.html"
fi
