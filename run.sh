#!/bin/bash

# ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃  Flood Sensor Daemon Script                 ┃
# ┃  Runs main.py, raingauge.py, and            ┃
# ┃                                             ┃
# ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
# Should use UTF-8 for the icons and special characters

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# PID files
PID_MAIN="$SCRIPT_DIR/PID/flood_sensor.pid"
PID_RAINGAUGE="$SCRIPT_DIR/PID/raingauge.pid"
PID_UPLOADER="$SCRIPT_DIR/PID/rain_gauge_uploader.pid"

# Log files
LOG_MAIN="$SCRIPT_DIR/Logs/flood_sensor.log"
LOG_RAINGAUGE="$SCRIPT_DIR/Logs/rain_gauge.log"

# Check required files exist
for file in main.py raingauge.py .env .env.public; do
    if [ ! -f "$SCRIPT_DIR/$file" ]; then
        echo "❌ Error: $file not found in $SCRIPT_DIR"
        exit 1
    fi
done


# Activate virtual environment
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
else
    echo "❌ Error: Virtual environment not found at venv/"
    exit 1
fi


# Check Python and dependencies
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed or not in PATH"
    exit 1
fi


echo "🔍 Checking Python dependencies..."
python3 -c "import tapipy, dotenv, requests, RPi.GPIO" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Error: Required Python packages are missing."
    echo "📦 Install with: pip3 install tapipy python-dotenv requests RPi.GPIO"
    echo "      Or with: pip3 install -r requirements.txt"
    exit 1
fi

check_pid() {
    [ -f "$1" ] && ps -p "$(cat "$1")" > /dev/null 2>&1
}


start_sensor() {
    echo "🚀 Starting flood sensor components..."

    if check_pid "$PID_MAIN" || check_pid "$PID_RAINGAUGE" || check_pid "$PID_UPLOADER"; then
        echo "⚠️ One or more components are already running."
        return 1
    fi

    nohup python3 "$SCRIPT_DIR/main.py" >> "$LOG_MAIN" 2>&1 &
    echo $! > "$PID_MAIN"
    echo $1
    nohup sudo python3 "$SCRIPT_DIR/raingauge.py" >> "$LOG_RAINGAUGE" 2>&1 &
    echo $! > "$PID_RAINGAUGE"

    echo "✅ Started all components."
}


stop_sensor() {
    echo "🛑 Stopping flood sensor components..."

    for pid_file in "$PID_MAIN" "$PID_RAINGAUGE" "$PID_UPLOADER"; do
        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            if ps -p "$PID" > /dev/null 2>&1; then
                sudo skill "$PID"
                sleep 2
                if ps -p "$PID" > /dev/null 2>&1; then
                    echo "⛔ Force killing PID $PID"
                    kill -9 "$PID"
                fi
            else
                echo "⚠️ Process $PID not running"
            fi
            rm -f "$pid_file"
        else
            echo "⚠️ PID file $pid_file not found"
        fi
    done

    echo "🧼 All stopped."
}


status_sensor() {
    echo "📊 Status report:"
    for pid_file in "$PID_MAIN" "$PID_RAINGAUGE" "$PID_UPLOADER"; do
        SCRIPT_NAME=$(basename "$pid_file" .pid)
        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            if ps -p "$PID" > /dev/null 2>&1; then
                echo "✅ $SCRIPT_NAME.py is running (PID: $PID)"
            else
                echo "❌ $SCRIPT_NAME.py is NOT running but PID file exists"
            fi
        else
            echo "❌ $SCRIPT_NAME.py PID file missing"
        fi
    done
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
        echo "Usage: $0 {start | stop | restart | status}"
        echo ""
        echo "Commands:"
        echo "  start   - Start all flood sensor components"
        echo "  stop    - Stop all flood sensor components"
        echo "  restart - Restart all flood sensor components"
        echo "  status  - Show running status"
        exit 1
        ;;
esac
