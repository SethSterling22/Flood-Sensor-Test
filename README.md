# Flood Sensor Application

A Raspberry Pi-based flood monitoring system that integrates with USGS streamflow data and MINT (Model Integration) API to detect and respond to flood conditions in real-time.

## Overview

This application monitors a physical flood sensor connected to a Raspberry Pi and automatically triggers flood modeling when water levels exceed predefined thresholds. It fetches real-time streamflow data from USGS and submits flood prediction models through the MINT platform.

## Features

- **Real-time Flood Detection**: Monitors GPIO pin for physical flood sensor input
- **Real-time Rainfall Measurement**: Calculates the volume of rainfall in a time interval
- **Real-time Temperature and Humidty Measurement**: Provides the temperature in Celsius or Farenheit degrees and Humidity percentage
- **Client-Server Communication**: A Network of Sensors can be deployed in different locations and the data will be synchronized and storage
- **Upload Weather Data to Upstream**: The data collected by the sensors is sent to: https://upstream.pods.tacc.tapis.io
- **USGS Integration**: Fetches live streamflow data from USGS water services
- **MINT API Integration**: Automatically configures and submits flood models
- **Continuous Monitoring**: Runs as a daemon with configurable check intervals
- **Comprehensive Logging**: Detailed logging to both file and console
- **Daemon Management**: Start, stop, restart, and status checking capabilities

## Requirements

### Hardware Requirements

- Raspberry Pi (any model with GPIO support)
- Flood Sensor connected to GPIO pin 13 (3v)
- Rainfall Sensor connected to GPIO pin 6
- Temperature and Humidity connected to GPIO 4 (3v)
- Internet connection for API access

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

|  Variable  |     Description     | Required |
|------------|---------------------|----------|
|  `userid`  | Your Tapis username |    Yes   |
| `password` | Your Tapis password |    Yes   |

### Environment Configuration

The file `.env.config` in the ./Env directory must be configured independently by each client/node. An example of the variables and their values ​​is provided, but these must be fully configured.

|       Variable        |                                        Description                                         | Required |
|-----------------------|--------------------------------------------------------------------------------------------|----------|
| `FLOOD_SENSOR`        | GPIO pin number assigned to the flood sensor	                                             |    Yes   |
| `RAINFALL_SENSOR`     | GPIO pin number assigned to the rain sensor (rain gauge)	                                 |    Yes   |
| `BUCKET_SIZE`         | Size of the rain gauge scoop or bucket (volume of precipitation per turn, typically in mm) |    Yes   |
| `TEMP_&_HUMID_SENSOR` | GPIO Pin Number for the temperature and humidity sensor	                                 |    Yes   |
| `RECEIVER_HOST`       | IP address or hostname of the receiving server (metrics_receiver.py)	                     |    Yes   |
| `RECEIVER_PORT`       | Network port on which the receiving server is listening	                                 |    Yes   |
| `NODE_ID`             | Unique identifier assigned to this device or sensor node	                                 |    Yes   |
| `CAMPAIGN_ID`         | Campaign ID to which the station belongs within the Tapis system                           |    Yes   |
| `STATION_ID`          | Station ID (this node) within the specific Campaign                                        |    Yes   |
| `GPS_LAT`             | Latitude of the physical location of the node                                              |    Yes   |
| `GPS_LON`             | Longitude of the physical location of the node                                             |    Yes   |

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

The use of sudo is mandatory due to the use of GPIO pins 
and the different dependencies.

#### Method 1: Direct Execution

## Client/Node:
Get the Python Path
```bash
which python 
``` 
using the previous path
```bash
# To run as a Node pointing to an external server
sudo path/to/python3 main.py
```

```bash
# To run as an ExitNode pointing to localhost
sudo path/to/python3 main.py {any_arg}
```

## Server:
Get the Python Path
```bash
which python 
``` 
using the previous path
```bash
# Will be waiting for a Client/Node connection
sudo path/to/python3 metrics_receiver.py
```

## Uploader:
Get the Python Path
```bash
which python 
``` 
using the previous path
```bash
# Will search and upload the given file to Upstream (files must be in ./Logs/Water_data)
sudo path/to/python3 metrics_uploader.py {file_path}
```


#### Method 2: Daemon Mode

**Start the daemon:**
```bash
sudo ./run.sh start  { Server | Node  | ExitNode | Uploader }
```

**Check status:**
```bash
sudo ./run.sh status
```

**Stop the daemon:**
```bash
sudo ./run.sh stop
```

**Restart the daemon:**
```bash
sudo ./run.sh restart
```

## How It Works:

1. **Sensor Monitoring**: Continuously monitors different GPIO pins for the sensors input

2. **Threshold Check**: When sensor detects water (LOW signal):
   - Fetches current streamflow data from USGS
   - Compares with previous readings
   - Converts flow rate from ft³/s to m³/s

3. **Model Execution**: If streamflow exceeds threshold:
   - Authenticates with Tapis API
   - Configures flood model parameters
   - Submits model execution request to MINT

4. **Logging**: All activities are logged with timestamps

5. **Life Cycle**: Save data from sensors each minute, to be uploaded each hour.


## Logging

The application creates detailed in:
- **File**: `Logs/` (in project directory)
- **Console**: Real-time output during execution

Log levels include:
- `INFO`: General operational information
- `DEBUG`: Detailed debugging information
- `ERROR`: Error conditions and failures

## File Structure

```
Flood-Sensor/
|
├── Daemon_Services/                # Systemd service configurations
│   ├── flood-sensor.service        # Flood monitoring daemon service
│   └── rain_gauge_uploader.service # Data telemetry service 
|
├── Env/                            # Environment configuration files 
│   ├── .env                        # Private environment variables (Must be created)
│   ├── .env.config                 # Private environment variables (API keys, secrets)
|   └── .env.public                 # Public/shared environment variables
|
├── Logs/                           # System and application log files (Created automaticaly)
|   └── Water_data/                 # Directory where all the metrics are storaged
|
├── PID/                            # Process ID files for daemon management (Created/deleted automaticaly)
|
├── Sensor_Tests/                   # Hardware validation test suites
│   ├── flood_sensor_test.py        # Flood sensor calibration/validation tests
│   └── rainfall_sensor_test.py     # Rain gauge tests
|                                                                    
├── Sensors/                        # Sensors folder
│   ├── flood_sensor.py             # Core flood detection logic used as thread
│   ├── rain_gauge.py               # Precipitation measurement module used as thread
|   └── temp_and_humid_sensor.py    # Temperature and Humidity sensor, used as thread
|
├── Setup/                          # Dependency management
│   ├── campaign_manager.py         # Monitoring campaign scheduler
│   ├── requirements.txt            # Python package requirements
|   └── constraints.txt             # Version-pinned package constraints
|
├── Tests/                          # Testing folder
|   ├── Test_Nodes/
|   |   ├── Logs/
|   |   ├── dummy_manager.py        # Used for test, replica of "main.py"
|   |   └── dummy_node.py           # Generates dummy data for testing
│   ├── flood_sensor_test.py        # Used to test the flood sensor independently
│   └── rainfall_sensor_test.py     # Used to test the rainfall sensor independently
|
├── venv/                           # Must be created
|
├── .gitattributes                  # Git file handling rules
├── .gitignore                      # Git excluded files/patterns
├── LICENSE                         # Project license (MIT)
├── main.py                         # Primary application logic
├── metrics_receiver.py             # Listener metrics server
├── metrics_uploader.py             # Data export to Upstream-dso
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
tail -f ./Logs/metrics_receiver.log
```

## Development

### Adding New Features

1. Modify sensor pin in `./Env/.env.config`:
   ```python
   # Change to your desired pin
   FLOOD_SENSOR = 13
   RAINFALL_SENSOR = 6
   TEMP_&_HUMID_SENSOR = 4
   ```

3. Modify streamflow threshold logic in the main loop

### Testing

Test the sensor without hardware (RaspberryPi and Sensors):
- You can use the `metrics_receiver.py` as normal
- For Nodes, `Tests/Test_Nodes/dummy_manager.py` must be used


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
- [Rainfall Sensor](https://wiki.dfrobot.com/SKU_SEN0575_Gravity_Rainfall_Sensor "Rainfall Sensor Link")
- [Temperature and humidity Sensor](https://docs.sunfounder.com/projects/umsk/en/latest/05_raspberry_pi/pi_lesson19_dht11.html "Temperature and humidity Sensor Sensor Link")

