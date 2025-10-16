# Flood Sensor Application

A Raspberry Pi-based flood monitoring system that integrates with USGS streamflow data and MINT (Model Integration) API to detect and respond to flood conditions in real-time.

## Overview

This application monitors a physical flood sensor connected to a Raspberry Pi and automatically triggers flood modeling when water levels exceed predefined thresholds. It fetches real-time streamflow data from USGS and submits flood prediction models through the MINT platform.

## Features

- **Real-time Flood Detection**: Monitors GPIO pin for physical flood sensor input
- **Real-time Rainfall Measurement**: Calculates the volume of rainfall in a time interval
- **USGS Integration**: Fetches live streamflow data from USGS water services
- **MINT API Integration**: Automatically configures and submits flood models
- **Continuous Monitoring**: Runs as a daemon with configurable check intervals
- **Comprehensive Logging**: Detailed logging to both file and console
- **Daemon Management**: Start, stop, restart, and status checking capabilities

## Requirements

### Hardware Requirements

- Raspberry Pi (any model with GPIO support)
- Flood sensor connected to GPIO pin 13
- Rainfall Sensor connected to GPIO pin 6
- Internet connection for API access
- SE DEBEN AGREGAR LOS DIFERENTES SENSORES !!!

### Software Requirements

- Python 3.6 or higher
- Required Python packages (see Installation section)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/In-For-Disaster-Analytics/Flood-Sensor.git
   cd Flood-Sensor
   ```

2. **Install Python dependencies (Be ensure your local packages are as clean as possible):**
   ```bash
   python -m venv --system-site-packages venv
   source venv/bin/activate
   pip3 install -r Setup/requirements.txt
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

Create a `.env` file with the following variables for MINT:

|  Variable  | Description | Required |
|------------|-------------|----------|
|  `userid`  | Your Tapis username | Yes |
| `password` | Your Tapis password | Yes |

### Hardware Configuration

- **Liquid Sensor Pin**: GPIO pin 13 (BCM mode)
- **Sensor Logic**: 
  - HIGH = No flood detected
  - LOW = Potential flood condition

- **Rainfall Sensor Pin**: GPIO pin 6
- **Sensor Logic**:
  - Work like a button: give a signal when is pressed 

### Model Configuration

The application is pre-configured with:
- **MINT Base URL**: `https://ensemble-manager.mint.tacc.utexas.edu/v1`
- **Problem Statement ID**: `IDYnqZpBGvZpL4GPLRcg`
- **Task ID**: `dwDiJ0dymXPd93kvlF9S`
- **Sub-task ID**: `qwiUq7XqNK9bp6crSDj6`

## Usage

### Running the Application

HAY QUE REVISARLO !!!

#### Method 1: Direct Execution
```bash
python3 main.py
``` 

#### Method 2: Daemon Mode

**Start the daemon:**
```bash
./run.sh start  { Server | Node  | ExitNode }
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

REVISAR LA NUEVA ESTRUCTURA !!!

```
Flood-Sensor/
├── Env/                            # Environment configuration files
│   ├── .env.config                 # Private environment variables (API keys, secrets)
│   ├── .env.public                 # Public/shared environment variables
|   └── .env.                       # Private environment variables (Must be created)
|
├── Logs/                           # System and application log files (Created automaticaly)
├── PID/                            # Process ID files for daemon management (Created/deleted automaticaly)
├── Sensor_Tests/                   # Hardware validation test suites
│   ├── flood_sensor_test.py        # Flood sensor calibration/validation tests
│   └── rainfall_sensor_test.py     # Rain gauge tests
|
├── Services/                       # Systemd service configurations
│   ├── flood-sensor.service        # Flood monitoring daemon service
│   └── rain_gauge_uploader.service # Data telemetry service 
|                                                                    CREO QUE DEBO HACER CONSIDERACIONES EN SERVICES!!!
├── Setup/                          # Dependency management
│   ├── constraints.txt             # Version-pinned package constraints
│   ├── requirements.txt            # Python package requirements
|   └── campaign_manager.py         # Monitoring campaign scheduler
|
├── .gitattributes                  # Git file handling rules
├── .gitignore                      # Git excluded files/patterns
├── flood_sensor.py                 # Core flood detection logic
├── LICENSE                         # Project license (MIT)
├── main.py                         # Primary application logic
├── metrics_receiver.py             # Listener metrics server
├── metrics_uploader.py             # Data export to Upstream-dso
├── rain_gauge.py                   # Precipitation measurement module
├── README.md                       # Project documentation
├── run.sh                          # bash execution script
└── utils.py                        # Shared utility functions
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
# Comment out GPIO lines for testing:

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

## Extra Information: 

- [Liquid Sensor](https://www.mouser.com/datasheet/2/737/3397_datasheet_actual-1228633.pdf?srsltid=AfmBOoomA1sX4nExoP1tFe5z0GlJ6zAp_ayNQhGoQWl9QLpZI74N1I5b "Liquid Sensor Link")
- [Rainfall Sensor:](https://wiki.dfrobot.com/SKU_SEN0575_Gravity_Rainfall_Sensor "Rainfall Sensor Link")

