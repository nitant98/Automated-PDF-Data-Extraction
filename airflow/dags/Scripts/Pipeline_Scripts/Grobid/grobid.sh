#!/bin/bash
set -e

# Step 1: Pull the Grobid Docker image
# docker pull grobid/grobid:0.7.1

# Step 2: Run the Grobid Docker container in the background
# docker run -d --rm --name grobid -p 8070:8070 -p 8071:8071 grobid/grobid:0.7.1
# docker run -t --rm -p 8070:8070 lfoppiano/grobid:0.8.0

docker run --rm --gpus all --init --ulimit core=0 -p 8070:8070 grobid/grobid:0.8.0

# Wait a bit for the container to initialize (Grobid needs some time to start)
