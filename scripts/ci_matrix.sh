#!/usr/bin/env bash
# lizysdk 多版本测试矩阵（conda 环境驱动）。
#
# 环境创建（一次性）：
#   conda create -y -n lizy39  python=3.9
#   conda create -y -n lizy311 python=3.11
#   conda create -y -n lizy312 python=3.12
#   conda create -y -n lizy313 python=3.13
#   conda run -n <env> python -m pip install -e ".[dev]"
#
# 用法：
#   scripts/ci_matrix.sh                    # 跑全矩阵
#   scripts/ci_matrix.sh lizy39:3.9         # 只跑指定环境
# 退出码：任一版本失败则非零。

set -u
cd "$(dirname "$0")/.."

MATRIX_DEFAULT="lizy39:3.9 lizy311:3.11 lizy312:3.12 lizy313:3.13"
MATRIX="${*:-$MATRIX_DEFAULT}"

fail=0
printf "%-10s %-8s %-8s %s\n" "ENV" "PY" "RESULT" "DETAIL"
for entry in $MATRIX; do
  env="${entry%%:*}"
  ver="${entry##*:}"
  out=$(conda run -n "$env" python -m pytest -q 2>&1 | tail -1)
  if echo "$out" | grep -q "passed"; then
    printf "%-10s %-8s %-8s %s\n" "$env" "$ver" "OK" "$out"
  else
    printf "%-10s %-8s %-8s %s\n" "$env" "$ver" "FAIL" "$out"
    fail=1
  fi
done
exit $fail
