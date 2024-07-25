#!/bin/bash
set -e

echo "Waiting for Grobid to start..."
sleep 30 # Adjust this based on your system's performance
echo "Grobid has been started and is now ready to process files."

# Note: The Python script should be triggered after this message.
# Example:
pip install -r requirements.txt
python grobid_process.py
# Stop the Grobid container
# docker stop grobid

echo "Processing completed."