#!/bin/bash
# 增加本金快捷脚本

PYTHON_EXE="/home/liuwq/miniconda3/envs/contesttrade/bin/python"
SCRIPT_PATH="/home/liuwq/work/ContestTrade/cli/add_cash.py"

if [ -z "$1" ]; then
    echo "用法: ./add_cash.sh <金额>"
    echo "示例: ./add_cash.sh 10000"
    exit 1
fi

$PYTHON_EXE $SCRIPT_PATH "$@"
