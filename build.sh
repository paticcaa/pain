#!/bin/bash

bazel build --disk_cache=/mnt/bazel_cache -s --verbose_failures $@
