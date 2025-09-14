#!/bin/bash

# This script is used to test the Manusya program

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source $SCRIPT_DIR/env.sh

SAD=${SCRIPT_DIR}/../bazel-bin/src/sad/sad

chunk_id=$($SAD manusya create-chunk --host $PAIN_MANUSYA_ADDRESS | jq .chunk_id | tr -d '"')
echo "chunk id: $chunk_id"

if [ -z $chunk_id ]; then
    echo "Failed to create chunk"
    exit 1
fi


$SAD manusya append-chunk -c $chunk_id -d 4 -o 4 --host $PAIN_MANUSYA_ADDRESS &
$SAD manusya append-chunk -c $chunk_id -d 3 -o 3 --host $PAIN_MANUSYA_ADDRESS &
$SAD manusya append-chunk -c $chunk_id -d 2 -o 2 --host $PAIN_MANUSYA_ADDRESS &
$SAD manusya append-chunk -c $chunk_id -d 1 -o 1 --host $PAIN_MANUSYA_ADDRESS &
$SAD manusya append-chunk -c $chunk_id -d 0 -o 0 --host $PAIN_MANUSYA_ADDRESS

data=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 4096)
$SAD manusya append-chunk -c $chunk_id --offset 5 --data $data --host $PAIN_MANUSYA_ADDRESS

$SAD manusya read-chunk -c $chunk_id --offset 0 --length 4101 --host $PAIN_MANUSYA_ADDRESS
