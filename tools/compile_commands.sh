#!/bin/bash

if [ "$1" == "all" ]; then
    bazel run @hedron_compile_commands//:refresh_all
else
    bazel run :refresh_compile_commands
fi
