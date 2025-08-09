#!/bin/bash

TARGET=$1
bazel aquery "mnemonic('CppCompile', $TARGET)" --output=text
