#!/bin/bash

# Service file name
DAEMON_DIR="Daemon_Services"
DEST_PATH="/etc/systemd/system/"

# Check if the script was called with the required argument
if [ -z "$1" ]; then
    echo "❌ Error: Missing required argument."
    echo "Usage: sudo ./Setup_Daemon.sh [ Server | Node ]"
    exit 1
fi

MODE=$1

# Set the source and destination service names based on the argument
if [ "$MODE" == "Server" ]; then
    SERVICE_FILE="start-server.service"
elif [ "$MODE" == "Node" ]; then
    SERVICE_FILE="start-client.service"
else
    echo "❌ Error: Invalid argument '$MODE'."
    echo "Usage: sudo ./Setup_Daemon.sh [ Server | Node ]"
    exit 1
fi

# Determine the absolute project path
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")/..
PROJECT_DIR_PATH="$SCRIPT_DIR"


# ----------------------------------------------------
# 1. Permissions Check (Requires root/sudo)
# ----------------------------------------------------
if [ "$EUID" -ne 0 ]; then
    echo "❌ This script must be run with root privileges (sudo)."
    exit 1
fi

# ----------------------------------------------------
# 2. Generate and Copy the Service File with Dynamic Path
# ----------------------------------------------------
echo "⚙️ Generating and installing $SERVICE_FILE with project path: $PROJECT_DIR_PATH"

SOURCE_FILE="$PROJECT_DIR_PATH/$DAEMON_DIR/$SERVICE_FILE"
if [ ! -f "$SOURCE_FILE" ]; then
    echo "❌ Error: Service file not found at $SOURCE_FILE."
    exit 1
fi

# Use sed to replace the %PROJECT_DIR% with the actual project
# directory path where the project is located to maintain portability
sed "s|%PROJECT_DIR%|$PROJECT_DIR_PATH|g" "$SOURCE_FILE" > "$DEST_PATH/$SERVICE_FILE"

if [ $? -ne 0 ]; then
    echo "❌ Error generating and copying the service file."
    exit 1
fi
echo "✅ Service file ($SERVICE_FILE) installed in $DEST_PATH."

# ----------------------------------------------------
# 3. Reload and Enable
# ----------------------------------------------------
echo "🔄 Reloading Systemd configuration..."
systemctl daemon-reload

echo "🔌 Enabling $SERVICE_FILE service for automatic startup..."
systemctl enable "$SERVICE_FILE"

echo "▶️ Starting $SERVICE_FILE service now..."
systemctl start "$SERVICE_FILE"

echo "🎉 Installation complete. Check status with: sudo systemctl status $SERVICE_FILE"

exit 0

