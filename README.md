# Flood Sensor Application

A Raspberry Pi-based flood monitoring system that integrates with USGS streamflow data and MINT (Model Integration) API to detect and respond to flood conditions in real-time.

## Overview

This application monitors a physical flood sensor connected to a Raspberry Pi and automatically triggers flood modeling when water levels exceed predefined thresholds. It fetches real-time streamflow data from USGS and submits flood prediction models through the MINT platform.

## Features

- **Real-time Flood Detection**: Monitors GPIO pin for physical flood sensor input
- **USGS Integration**: Fetches live streamflow data from USGS water services
- **MINT API Integration**: Automatically configures and submits flood models
- **Continuous Monitoring**: Runs as a daemon with configurable check intervals
- **Comprehensive Logging**: Detailed logging to both file and console
- **Daemon Management**: Start, stop, restart, and status checking capabilities

## Hardware Requirements

- Raspberry Pi (any model with GPIO support)
- Flood sensor connected to GPIO pin 13
- Internet connection for API access

## Software Requirements

- Python 3.6 or higher
- Required Python packages (see Installation section)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/In-For-Disaster-Analytics/Flood-Sensor.git
   cd Flood-Sensor
   ```

2. **Install Python dependencies:**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip3 install -r requirements.txt
   ```

3. **Create environment configuration:**
   Create a `.env` file in the project root with your credentials:
   ```env
   userid=your_tapis_username
   password=your_tapis_password
   ```

4. **Make the run script executable:**
   ```bash
   chmod +x run.sh
   ```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

| Variable | Description | Required |
|----------|-------------|----------|
| `userid` | Your Tapis username | Yes |
| `password` | Your Tapis password | Yes |

### Hardware Configuration

- **Sensor Pin**: GPIO pin 13 (BCM mode)
- **Sensor Logic**: 
  - HIGH = No flood detected
  - LOW = Potential flood condition

### Model Configuration

The application is pre-configured with:
- **MINT Base URL**: `https://ensemble-manager.mint.tacc.utexas.edu/v1`
- **Problem Statement ID**: `IDYnqZpBGvZpL4GPLRcg`
- **Task ID**: `dwDiJ0dymXPd93kvlF9S`
- **Sub-task ID**: `qwiUq7XqNK9bp6crSDj6`

## Usage

### Running the Application

#### Method 1: Direct Execution
```bash
python3 main.py
```

#### Method 2: Using the Run Script
```bash
./run.sh
```

#### Method 3: Daemon Mode

**Start the daemon:**
```bash
./run.sh start
```

**Check status:**
```bash
./run.sh status
```

**Stop the daemon:**
```bash
./run.sh stop
```

**Restart the daemon:**
```bash
./run.sh restart
```

## How It Works

1. **Sensor Monitoring**: Continuously monitors GPIO pin 13 for flood sensor input
2. **Threshold Check**: When sensor detects water (LOW signal):
   - Fetches current streamflow data from USGS
   - Compares with previous readings
   - Converts flow rate from ft³/s to m³/s
3. **Model Execution**: If streamflow exceeds threshold:
   - Authenticates with Tapis API
   - Configures flood model parameters
   - Submits model execution request to MINT
4. **Logging**: All activities are logged with timestamps
5. **Wait Cycle**: Sleeps for 6 minutes (360 seconds) between checks

## Logging

The application creates detailed logs in:
- **File**: `flood_sensor.log` (in project directory)
- **Console**: Real-time output during execution

Log levels include:
- `INFO`: General operational information
- `DEBUG`: Detailed debugging information
- `ERROR`: Error conditions and failures

## File Structure

```
Flood-Sensor/
├── main.py              # Main application logic
├── run.sh               # Execution and daemon management script
├── utils.py             # Utility functions (not shown)
├── .env                 # Environment variables (create this)
├── flood_sensor.log     # Application logs (auto-generated)
├── flood_sensor.pid     # Process ID file (auto-generated)
└── README.md           # This file
```

## API Integration

### USGS Water Services
- Fetches real-time streamflow data
- Converts units from ft³/s to m³/s
- Used for threshold comparison

### MINT API
- Model configuration and parameter setting
- Flood model execution submission
- Integration with Tapis authentication


### Log Analysis

Check the log file for detailed error information:
```bash
tail -f flood_sensor.log
```

## Development

### Adding New Features

1. Modify sensor pin in `main.py`:
   ```python
   sensor_pin = 13  # Change to your desired pin
   ```

2. Adjust check interval:
   ```python
   time.sleep(360)  # Change sleep duration (seconds)
   ```

3. Modify streamflow threshold logic in the main loop

### Testing

Test the sensor without hardware:
```python
# Comment out GPIO lines for testing
# GPIO.setmode(GPIO.BCM)
# GPIO.setup(sensor_pin, GPIO.IN)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

For issues and questions:
- Create an issue on GitHub
- Check the logs for detailed error information
- Verify hardware connections and API credentials

## Acknowledgments

- USGS for providing real-time water data
- MINT platform for flood modeling capabilities
- Tapis API for authentication services
