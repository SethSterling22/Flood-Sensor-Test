import os
import csv
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from upstream.client import UpstreamClient

# === ENVIRONMENT  VARIABLES ===
load_dotenv(".env.public")  # Public env variables
load_dotenv(".env")         # Tapis credentials

# === CONFIGURATION ===
LOG_DIR = "./Logs/rain_logs"  # absolute path for safety

SENSOR_FILE = os.path.join(LOG_DIR, "sensors.csv")
USERNAME = os.getenv("userid")
PASSWORD = os.getenv("password")
BASE_URL = os.getenv('BASE_URL')
CKAN_URL = os.getenv('CKAN_URL')
CKAN_ORG = os.getenv('CKAN_ORG')
CAMPAIGN_ID = os.getenv('CAMPAIGN_ID')
STATION_ID = os.getenv('STATION_ID')


# === LOGGING ===
PID_FILE = "./PID/rain_gauge_uploader.pid"


with open(PID_FILE, "w") as f:
    f.write(str(os.getpid()))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler( 'rain_gauge_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def init_sensor_file():
    if not os.path.exists(SENSOR_FILE):
        try:
            with open(SENSOR_FILE, "w", newline="") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerow(["alias,variablename,postprocess,units,datatype"])
                writer.writerow(["precipitation,precipitation,,mm,float"])
            logger.info(f"✅ Created sensor file at {SENSOR_FILE}")
        except Exception as e:
            logger.error(f"Failed to create sensor file: {e}")
    else:
        logger.info(f"Sensor file exists at {SENSOR_FILE}")


def get_previous_hour_file():
    """Return path for previous hour's CSV file."""
    now = datetime.now()
    prev_hour = now - timedelta(hours=1)
    filename = os.path.join(LOG_DIR, f"rain_{prev_hour.strftime('%Y%m%d_%H')}.csv")
    return  filename


def is_time_to_upload():
    """Check if current time is between HH:05:00 and HH:05:59 UTC."""
    now = datetime.datetime.now()
    # return now.minute == 5
    return True


def submit_file_to_upstream(file_path):
    logger.info(f"🚀 Uploading {os.path.basename(file_path)} to Upstream...")
    try:
        client = UpstreamClient(
            username=USERNAME,
            password=PASSWORD,
            base_url=BASE_URL,
            ckan_url=CKAN_URL,
            ckan_organization=CKAN_ORG
        )
    except Exception as e:
        logger.error(f"❌ Upload failed for Upstream Client {e}")
        return False
        
    try:
        client.upload_csv_data(
            measurements_file=file_path,
            sensors_file=SENSOR_FILE,
            campaign_id=CAMPAIGN_ID,
            station_id=STATION_ID
        )
        logger.info(f"✅ Successfully uploaded {file_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Upload failed for {file_path}: {e}")
        return False


def main():
    logger.info("📡 Starting rain gauge uploader...")
    init_sensor_file()

    if not is_time_to_upload():
        logger.info("⏳ Not time to upload yet. Run this script at 5 minutes past the hour.")
        return

    file_to_upload = get_previous_hour_file()
    if not os.path.exists(file_to_upload):
        logger.warning(f"⚠️ No data file to upload for previous hour: {file_to_upload}")
        return

    success = submit_file_to_upstream(file_to_upload)
    if success:
        try:
            os.remove(file_to_upload)
            logger.info(f"🧹 Removed uploaded file {os.path.basename(file_to_upload)}")
        except Exception as e:
            logger.error(f"Failed to remove uploaded file: {e}")

if __name__ == "__main__":
    main()
