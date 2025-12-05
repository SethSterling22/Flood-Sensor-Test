"""
This program is used to upload the recollected data
from the Client-Server application to Upstream-dso
"""



################################ IMPORT MODULES AND LIBRARIES ####################################
##################################################################################################
import os
import sys
import csv
import logging
import tempfile
import pandas as pd
from dotenv import load_dotenv
from upstream.client import UpstreamClient
##################################################################################################



############################### GLOBAL VARIABLES INITIALIZATION ##################################
##################################################################################################
# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env")         # Tapis credentials
load_dotenv("./Env/.env.public")  # Public env variables


# === CONFIGURATION ===
LOG_DIR = "./Logs/"
os.makedirs(LOG_DIR, exist_ok=True)


USERNAME = os.getenv("userid")
PASSWORD = os.getenv("password")
BASE_URL = os.getenv('BASE_URL')
CKAN_URL = os.getenv('CKAN_URL')
CKAN_ORG = os.getenv('CKAN_ORG')
STATION_ID = int(os.getenv('STATION_ID'))
CAMPAIGN_ID = int(os.getenv('CAMPAIGN_ID'))
CSV_DIR = os.path.join(LOG_DIR,"Water_data/")
SENSOR_FILE = os.path.join(CSV_DIR, "metrics_template.csv")


# ====== LOGGING SETUP ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'metrics_receiver.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
##################################################################################################



################################### FILE TEMPLATE CREATION #######################################
##################################################################################################
def init_sensor_file():
    """
    Create the template CSV with the variables for Upstream
    """
    try:
        if not os.path.exists(SENSOR_FILE):
            with open(SENSOR_FILE, "w", newline="") as file:
                writer = csv.writer(file, delimiter="\t")
                writer.writerow(["alias,variablename,postprocess,units,datatype"]) # Default fields

                # Fields to upload
                # writer.writerow(["Metrics,Metrics,true,string,float"])
                writer.writerow(["Precipitation,Precipitation,False,mm,float"])
                writer.writerow(["Temperature,Temperature,False,Celsius,float"])
                writer.writerow(["Humidity,Humidity,False,Percentage,float"])
                writer.writerow(["Flooding,Flooding,False,Boolean,integer"])

            logger.info("✅ Created sensor file at %s", SENSOR_FILE)
        else:
            logger.info("Sensor file exists at %s", SENSOR_FILE)
        return True
    except Exception as e:
        logger.error("Failed to create sensor file: %s", e)
        return False
##################################################################################################



######################### CREATE UPSTREAM SESION AND UPLOAD THE DATA #############################
##################################################################################################
def submit_file_to_upstream(file_path):
    """
    It connects to Upstream and attempts to upload 
    the sensor data, dividing it by Station_Id
    """
    logger.info("🚀 Processing %s for multiple stations...", file_path)

    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        if 'Station_Id' not in df.columns:
            logger.error("❌ Column 'Station_Id' not found in %s", file_path)
            return False

        # Group by Station_Id
        grouped = df.groupby('Station_Id')

        # Client initialization
        client = UpstreamClient(
            username=USERNAME,
            password=PASSWORD,
            base_url=BASE_URL,
            ckan_url=CKAN_URL,
            ckan_organization=CKAN_ORG
        )

        success_count = 0
        total_stations = len(grouped)

        for station_id, group in grouped:
            try:
                # Create a temp file for each station and iterate it
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
                    group.to_csv(tmp_file.name, index=False)
                    logger.info("📤 Uploading %s records for station %s...", len(group), station_id)

                    client.upload_csv_data(
                        measurements_file=tmp_file.name,
                        sensors_file=SENSOR_FILE,
                        campaign_id=CAMPAIGN_ID,
                        station_id=station_id
                    )
                    success_count += 1
                    logger.info("✅ Successfully uploaded data for station %s", station_id)

            except Exception as e:
                logger.error("❌ Failed to upload data for station %s: %s", station_id, e)
            finally:
                # Delete a temp file if exists
                if os.path.exists(tmp_file.name):
                    os.unlink(tmp_file.name)

        logger.info("📊 Successfully uploaded %s of %s stations", success_count, total_stations)
        return success_count == total_stations

    except Exception as e:
        logger.error("❌ Critical error processing %s: %s", file_path, e)
        return False
##################################################################################################



#################################### PROGRAM EXECUTION ###########################################
##################################################################################################
def run_uploader(file_to_upload):
    """
    Principal function, called by metrics_receiver.py
    Process, upload and delete the RAW file if the upload is sucessful
    """

    logger.info("📡 Starting Metrics uploader...")
    created_template = init_sensor_file()
    if created_template:

        # file_to_upload = get_previous_hour_file()
        if not os.path.exists(file_to_upload):
            logger.warning("⚠️ No data file to upload for previous hour: %s", file_to_upload)
            return False

        success = submit_file_to_upstream(file_to_upload)
        if success:
            try:
                os.remove(file_to_upload) # File DELETE
                logger.info("🧹 Removed uploaded file %s", file_to_upload)
            except Exception as e:
                logger.error("❌ Failed to remove uploaded file: %s", e)
        else:
            logger.warning("❌ Upload failed for %s. RAW file preserved.", file_to_upload)

        return True

    logger.warning("❌ Error creating template file")
    return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(f"Executing manual upload with: {sys.argv[1]}")
        run_uploader(sys.argv[1])
    else:
        print("No command-line argument provided (CSV File Path)")
##################################################################################################
