#!/bin/bash

# ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃  Flood Sensor Daemon Script                 ┃
# ┃  Runs main.py, rain_gauge.py,               ┃
# ┃  flood_sensor and metrics_receiver.py       ┃
# ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
# Should use UTF-8 for the icons and special characters


# === CONFIGURE GLOBAL VARIABLES ===
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Set script directory
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python" # Set venv Python path

# == PID files ==
PID_MAIN="$SCRIPT_DIR/PID/main.pid"
# PID_RAINGAUGE="$SCRIPT_DIR/PID/rain_gauge.pid"
PID_RECEIVER="$SCRIPT_DIR/PID/metrics_receiver.pid" # The receiver will log what it sends

# == Log files ==
LOG_MAIN="$SCRIPT_DIR/Logs/main.log"
# LOG_RAINGAUGE="$SCRIPT_DIR/Logs/rain_gauge.log"
LOG_RECEIVER="$SCRIPT_DIR/Logs/metrics_receiver.log"


# === Check required files exist ===
for file in main.py rain_gauge.py flood_sensor.py metrics_receiver.py Env/.env Env/.env.public Env/.env.config; do
    if [ ! -f "$SCRIPT_DIR/$file" ]; then
        echo "❌ Error: $file not found in $SCRIPT_DIR"
        exit 1
    fi
done


# === ACTIVATE VIRTUAL ENVIRONMENT ===
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
else
    echo "❌ Error: Virtual environment not found at venv/"
    echo "📦 Can be created with: python -m venv --system-site-packages venv"
    exit 1
fi


# === CHECK PYTHON AND ITS DEPENDENCIES ===
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


# === CHECK IF THE PROCESS IS RUNNING IN BACKGROUND ===
check_pid() {
    [ -f "$1" ] && ps -p "$(cat "$1")" > /dev/null 2>&1
}


# === PROCESSES MANAGER ===
start_component() {
    local script_name="$1"
    local pid_file="$2"
    local log_file="$3"
    local use_sudo="$4"
    shift 4 
    local extra_args="$@"  

    if check_pid "$pid_file"; then
        echo "⚠️ Component $script_name is already running."
        return 0
    fi

    echo -n "🚀 Starting $script_name... "
    if [ "$use_sudo" = "true" ]; then
        # which python !!!
        nohup sudo $VENV_PYTHON "$SCRIPT_DIR/$script_name"  $extra_args >> "$log_file" 2>&1 &
    else
        
        nohup $VENV_PYTHON "$SCRIPT_DIR/$script_name"  $extra_args >> "$log_file" 2>&1 &
    fi
    echo $! > "$pid_file"
    echo "Done (PID: $(cat "$pid_file"))."
}


start_sensor() {
    echo "🚀 Starting flood sensor components..."
    local mode="$1" # 'Server', 'Node', 'ExitNode'
    
    # Check if any component is already running
    case "$mode" in
        Server)
            echo "⚙️ Mode: Server, starting metrics_receiver.py ...)"
            start_component "metrics_receiver.py" "$PID_RECEIVER" "$LOG_RECEIVER" "false"
            ;;
        Node)
            echo "⚙️ Mode: Node starting main.py ..."
            start_component "main.py" "$PID_MAIN" "$LOG_MAIN" "false"
            ;;
        ExitNode)
            echo "⚙️ Mode: ExitNode starting main.py and metrics_receiver.py ..."
            start_component "main.py" "$PID_MAIN" "$LOG_MAIN" "false" "ExitNode"
            start_component "metrics_receiver.py" "$PID_RECEIVER" "$LOG_RECEIVER" "false"
            ;;
        *)
            echo "⛔ ERROR: Invalid mode for start command: '$mode'"
            return 1
            ;;
    esac

    echo "✅ Components for mode '$mode' started."
}


stop_sensor() {
    echo "🛑 Stopping flood sensor components..."

    for pid_file in "$PID_MAIN" "$PID_RECEIVER"; do
        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            if ps -p "$PID" > /dev/null 2>&1; then
                # Change skill to kill for standard
                kill -9 "$PID" 
                wait "$PID" 2>/dev/null
                sleep 2
                if ps -p "$PID" > /dev/null 2>&1; then
                    echo "⛔ Force killing PID $PID"
                    kill -9 "$PID"
                    wait "$PID" 2>/dev/null
                fi
            else
                echo "⚠️ Process $PID not running"
            fi
            rm -f "$pid_file"
        else
            # Warning if PID file doesn't exist
            echo "⚠️ PID file $pid_file not found"
        fi
    done

    echo "🧼 All stopped."
}


status_sensor() {
    echo "📊 Status report:"

    for pid_file in "$PID_MAIN" "$PID_RECEIVER"; do
        case "$pid_file" in
            *$PID_MAIN) SCRIPT_NAME="main";;
            *$PID_RECEIVER) SCRIPT_NAME="metrics_receiver";;
            *) SCRIPT_NAME=$(basename "$pid_file" .pid);;
        esac

        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            if ps -p "$PID" > /dev/null 2>&1; then
                echo "✅ $SCRIPT_NAME.py is running (PID: $PID)"
            else
                echo "❌ $SCRIPT_NAME.py is NOT running but PID file exists (Stale PID: $PID)"
            fi
        else
            echo "❌ $SCRIPT_NAME.py PID file missing"
        fi
    done
}


# === EXECUTION MENU ===
case "$1" in
    start)
        # Check if a "Mode" parameter was given
        case "$2" in
            Server|Node|ExitNode)
                start_sensor "$2"
                ;;
            "")
                echo "⛔ ERROR: Missing start mode."
                echo "Usage: $0 start {Server | Node | ExitNode}"
                exit 1
                ;;
            *)
                echo "⛔ ERROR: Invalid start mode '$2'."
                exit 1
                ;;
        esac
        ;;
    stop|status|restart)
        "$1"_sensor
        ;;
    *)
        # --help general usage
        echo "Usage: $0 {start | stop | restart | status}"
        echo ""
        echo "Commands:"
        echo "  start {Server | Node | ExitNode} - Start components based on role."
        echo "    Server: metrics_receiver.py only"
        echo "    Node: main.py"
        echo "    ExitNode: main.py and metrics_receiver.py"
        echo "  stop    - Stop all flood sensor components"
        echo "  restart - Restart all flood sensor components"
        echo "  status  - Show running status"
        exit 1
        ;;
esac


