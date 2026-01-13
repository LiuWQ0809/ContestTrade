#!/bin/bash
# 增加持仓快捷脚本

PYTHON_EXE="/home/liuwq/miniconda3/envs/contesttrade/bin/python"
SCRIPT_PATH="/home/liuwq/work/ContestTrade/cli/add_holding.py"

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
    echo "用法: ./add_holding.sh <代码> <单价> <数量> [名称]"
    echo "示例: ./add_holding.sh 600519 1800 100 贵州茅台"
    exit 1
fi

$PYTHON_EXE $SCRIPT_PATH "$@"
