#!/bin/bash

# This script is used to test the Manusya program

SCRIPT_DIR=$(dirname $0)
SAD=${SCRIPT_DIR}/../output/bin/sad

uuid=$($SAD manusya create-chunk --enable-append-out-of-order | jq .uuid | tr -d '"')
echo "chunk uuid: $uuid"

if [ -z $uuid ]; then
    echo "Failed to create chunk"
    exit 1
fi

$SAD manusya append-chunk -c $uuid -d 4 -o 4 &
$SAD manusya append-chunk -c $uuid -d 3 -o 3 &
$SAD manusya append-chunk -c $uuid -d 2 -o 2 &
$SAD manusya append-chunk -c $uuid -d 1 -o 1 &
$SAD manusya append-chunk -c $uuid -d 0 -o 0
$SAD manusya read-chunk -c $uuid --offset 0 --length 5
