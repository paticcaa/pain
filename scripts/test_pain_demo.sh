#!/bin/bash

# 计算当前脚本路径，使用绝对路径计算 env.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source $SCRIPT_DIR/env.sh
./bazel-bin/src/examples/pain_demo/pain_demo --filesystem $PAIN_ASURA_ADDRESS
