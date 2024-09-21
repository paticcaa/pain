#!/bin/bash

# This script is used to test the Manusya program

SAD=./output/bin/sad

uuid=$($SAD manusya create-chunk| jq .uuid | tr -d '"')
echo "chunk uuid: $uuid"

$SAD manusya append-chunk -c $uuid -d 4 -o 4 &
$SAD manusya append-chunk -c $uuid -d 3 -o 3 &
$SAD manusya append-chunk -c $uuid -d 2 -o 2 &
$SAD manusya append-chunk -c $uuid -d 1 -o 1 &
$SAD manusya append-chunk -c $uuid -d 0 -o 0
$SAD manusya read-chunk -c $uuid --offset 0 --length 5
