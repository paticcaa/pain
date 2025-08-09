#!/bin/bash

TARGET=$1
bazel aquery "mnemonic('CppLink', $TARGET)" --output=text
