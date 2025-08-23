#!/bin/bash

# Script to register deva nodes with asura
# asura runs on 127.0.0.1:8201
# deva nodes run on the same IP address with ports 8001, 8002, 8003

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAD_TOOL="$SCRIPT_DIR/../output/bin/sad"
NODE_IP="$1"

ASURA_HOST="$NODE_IP:8201"

# Check if IP address is provided as argument
if [ $# -ne 1 ]; then
    echo "Usage: $0 <ip_address>"
    echo "Example: $0 192.168.1.100"
    exit 1
fi


# Validate IP address format (basic validation)
if [[ ! $NODE_IP =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Invalid IP address format: $NODE_IP"
    exit 1
fi

# Build deva nodes array with the same IP but different ports
DEVA_NODES=(
    "$NODE_IP:8001"
    "$NODE_IP:8002"
    "$NODE_IP:8003"
)

echo "Starting to register deva nodes with asura..."
echo "asura address: $ASURA_HOST"
echo "deva node IP: $NODE_IP"
echo "deva nodes to register: ${#DEVA_NODES[@]}"
echo ""

for deva_node in "${DEVA_NODES[@]}"; do
    IFS=':' read -r ip port <<< "$deva_node"
    echo "Registering deva node: $ip:$port"
    
    # Use sad tool to register deva with asura
    "$SAD_TOOL" asura register-deva --host "$ASURA_HOST" --ip "$ip" --port "$port"
    
    if [ $? -eq 0 ]; then
        echo "✓ Successfully registered deva node: $ip:$port"
    else
        echo "✗ Failed to register deva node: $ip:$port"
    fi
    echo ""
done

echo "Registration completed!"
echo ""
echo "Viewing registered deva node list:"
"$SAD_TOOL" asura list-deva --host "$ASURA_HOST" 
