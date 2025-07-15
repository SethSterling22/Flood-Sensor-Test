#!/bin/bash

# Flood Sensor Runner Script
# This script runs the main.py flood sensor application

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to the script directory
cd "$SCRIPT_DIR"

# Check if main.py exists
if [ ! -f "main.py" ]; then
    echo "Error: main.py not found in $SCRIPT_DIR"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found in $SCRIPT_DIR"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed or not in PATH"
    exit 1
fi

# Check if required Python packages are installed
echo "Checking Python dependencies..."
python3 -c "import tapipy, dotenv, requests, RPi.GPIO" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Error: Required Python packages are missing."
    echo "Please install them with: pip3 install tapipy python-dotenv requests RPi.GPIO"
    exit 1
fi

# Set up logging
LOG_FILE="$SCRIPT_DIR/flood_sensor.log"
echo "Starting Flood Sensor at $(date)" | tee -a "$LOG_FILE"

# Run the main Python script
echo "Running flood sensor application..."
python3 main.py 2>&1 #!/bin/bash

# Flood Sensor Daemon Runner Script
# This script can run the flood sensor as a background service

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/flood_sensor.pid"
LOG_FILE="$SCRIPT_DIR/flood_sensor.log"

start_sensor() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Flood sensor is already running (PID: $PID)"
            return 1
        else
            echo "Removing stale PID file"
            rm -f "$PID_FILE"
        fi
    fi
    
    echo "Starting flood sensor daemon..."
    cd "$SCRIPT_DIR"
    
    # Start the Python script in background
    nohup python3 main.py >> "$LOG_FILE" 2>&1 &
    PID=$!
    
    # Save PID to file
    echo $PID > "$PID_FILE"
    echo "Flood sensor started with PID: $PID"
    echo "Logs are being written to: $LOG_FILE"
}

stop_sensor() {
    if [ ! -f "$PID_FILE" ]; then
        echo "Flood sensor is not running (no PID file found)"
        return 1
    fi
    
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Stopping flood sensor (PID: $PID)..."
        kill "$PID"
        
        # Wait for process to stop
        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        
        # Force kill if still running
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Force killing flood sensor..."
            kill -9 "$PID"
        fi
        
        rm -f "$PID_FILE"
        echo "Flood sensor stopped"
    else
        echo "Flood sensor is not running (PID $PID not found)"
        rm -f "$PID_FILE"
    fi
}

status_sensor() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Flood sensor is running (PID: $PID)"
            return 0
        else
            echo "Flood sensor is not running (stale PID file)"
            return 1
        fi
    else
        echo "Flood sensor is not running"
        return 1
    fi
}

case "$1" in
    start)
        start_sensor
        ;;
    stop)
        stop_sensor
        ;;
    restart)
        stop_sensor
        sleep 2
        start_sensor
        ;;
    status)
        status_sensor
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the flood sensor daemon"
        echo "  stop    - Stop the flood sensor daemon"
        echo "  restart - Restart the flood sensor daemon"
        echo "  status  - Check if the flood sensor is running"
        exit 1
        ;;
esac
| tee -a "$LOG_FILE"

# Capture exit code
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Flood sensor application completed successfully" | tee -a "$LOG_FILE"
else
    echo "Flood sensor application exited with error code: $EXIT_CODE" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
