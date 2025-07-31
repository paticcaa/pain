#!/bin/bash

# Usage: ./sha256.sh <file>

# Check if a file is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <file>"
    exit 1
fi

# Get the file name from the command line argument
file="$1"
digest=$(cat $file | openssl dgst -sha256 -binary | openssl base64 -A)
echo "sha256-$digest"
