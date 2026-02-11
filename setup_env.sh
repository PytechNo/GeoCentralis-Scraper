#!/bin/bash
set -e

# Update and install system dependencies
echo "Updating system..."
apt-get update && apt-get upgrade -y
apt-get install -y python3 python3-pip python3-venv python3-full wget curl unzip gnupg2 git libnss3 libgconf-2-4 libfontconfig1 ca-certificates

# Install Google Chrome
if ! command -v google-chrome &> /dev/null; then
    echo "Installing Google Chrome..."
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-archive-keyring.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-archive-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list
    apt-get update
    apt-get install -y google-chrome-stable
else
    echo "Google Chrome is already installed."
fi

# Setup Python Virtual Environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate and install requirements
echo "Installing Python dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Make start script executable
chmod +x start_app.sh

echo "Setup complete!"
echo "To run the application, use: ./start_app.sh"
