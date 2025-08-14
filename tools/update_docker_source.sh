#!/bin/bash

# This script is used to update the docker source to daocloud

mkdir -p /etc/docker
tee /etc/docker/daemon.json <<-'EOF'
{
    "registry-mirrors": [
        "https://docker.m.daocloud.io"
    ]
}
EOF

systemctl restart docker
