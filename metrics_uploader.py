"""

"""



import os
import sys
import csv
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from upstream.client import UpstreamClient



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
STATION_ID = os.getenv('STATION_ID')
CAMPAIGN_ID = os.getenv('CAMPAIGN_ID')
SENSOR_FILE = os.path.join(LOG_DIR, "Water_data/metrics_template.csv")


# === LOGGING ===
PID_FILE = "./PID/metrics_uploader.pid"


# Register PID
with open(PID_FILE, "w") as f:
    f.write(str(os.getpid()))


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



def init_sensor_file(sensor_file):
    try:
        if not os.path.exists(sensor_file):
            with open(sensor_file, "w", newline="", encoding='utf-8-sig') as file:
                writer = csv.writer(file, delimiter="\t")

                writer.writerow(["alias, variablename, postprocess, units, datatype"]) # Default fields

                # Fields to upload
                writer.writerow(["Metrics", "Metrics", "", "string", "string"])
                # writer.writerow(["Rain Gauge", "Rain_Gauge_Metrics", "", "mm", "float"])
                # writer.writerow(["Flood Sensor", "Flood_Sensor_Metrics", "", "cm", "float"])
                # writer.writerow(["Temperature and Humidity", "Temp_and_Humid_Sensor_Metrics", "", "cm", "float"])
                # writer.writerow(["precipitation,precipitation,,mm,float"])
            logger.info(f"✅ Created sensor file at {sensor_file}")
        else:
            logger.info(f"Sensor file exists at {sensor_file}")
    except Exception as e:
        logger.error(f"Failed to create sensor file: {e}")




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


# def submit_file_to_upstream(file_path):
#     """
#     Reads the raw CSV file, transforms the JSON into CSV files
#     separated by sensor, and uploads them individually. 
#     Returns True if all individual uploads are successful, False otherwise.
#     """


#     logger.info("📋 Starting upload transformation for: %s", os.path.basename(file_path))

#     sensor_buffers = {"Rain Gauge": [], "Flood Sensor": []}
#     all_uploads_successful = True
#     temp_files_to_clean = []

#     TEMP_DIR = os.path.join(LOG_DIR, "temp")
#     os.makedirs(TEMP_DIR, exist_ok=True)

#     # 1. Read and transform data
#     try:
#         with open(file_path, mode='r', newline='', encoding='utf-8-sig') as infile:
#             reader = csv.reader(infile)
#             try:
#                 next(reader) # jump header
#             except StopIteration:
#                 logger.warning("⚠️ RAW file empty: %s", file_path)
#                 return True # Take "empty" for delete

#             for row in reader:
#                 if len(row) < 3: continue 
                
#                 node_id = row[0]
#                 timestamp = row[1]
#                 raw_json = row[2]
                
#                 try:
#                     # Lógica de limpieza de comillas si fue escrito con json.dumps
#                     if raw_json.startswith('"') and raw_json.endswith('"'):
#                         raw_json = raw_json[1:-1].replace('""', '"')

#                     data = json.loads(raw_json)
#                     sensor_name = data.get("Sensor")
#                     metric_value = data.get("Value", {}).get("Metrics")
#                     metric_value = data.get("value", {}).get("Metrics")
                    
#                     if sensor_name in sensor_buffers and metric_value is not None:
#                         sensor_buffers[sensor_name].append([node_id, timestamp, metric_value])
                        
#                 except json.JSONDecodeError as e:
#                     logger.error("❌ Error JSON: %s - Datos RAW: %s", e, raw_json)
#                 except Exception as e:
#                     logger.error("❌ Error processing row: %s", e)

#     except Exception as e:
#         logger.error("❌ Error general al leer archivo %s: %s", file_path, e)
#         return False

#     # 2. Write temporary data and Upload
#     try:
#         client = UpstreamClient(
#             username=USERNAME, 
#             password=PASSWORD,
#             base_url=BASE_URL,
#             ckan_url=CKAN_URL,
#             ckan_organization=CKAN_ORG
#         )
#     except Exception as e:
#         logger.error("❌ Error initializing Upstream Client %s", e)
#         return False

#     for sensor_name, data in sensor_buffers.items():
#         if not data: continue

#         base_name = os.path.basename(file_path).replace('.csv', '')
#         temp_filename = os.path.join(TEMP_DIR, f"{base_name}_{sensor_name.replace(' ', '_')}_metrics.csv")
#         temp_files_to_clean.append(temp_filename)
        
#         header_row = ["Node_ID", "Timestamp", f"{sensor_name.replace(' ', '_')}_Metrics"]

#         try:
#             with open(temp_filename, mode='w', newline='', encoding='utf-8-sig') as outfile:
#                 writer = csv.writer(outfile)
#                 writer.writerow(header_row)
#                 writer.writerows(data)
            
#             client.upload_csv_data(
#                 measurements_file=temp_filename,
#                 sensors_file=SENSOR_FILE,
#                 campaign_id=CAMPAIGN_ID,
#                 station_id=STATION_ID
#             )
#             logger.info("✅ Metrics Upload successful for %s", sensor_name)

#         except Exception as e:
#             logger.error(f"❌ Subida fallida para {sensor_name} ({os.path.basename(temp_filename)}): {e}")
#             logger.error(f"❌ Upload failed for Upstream Client {e}")
#             all_uploads_successful = False
            
#     # 3. Clean temporary files
#     for temp_file in temp_files_to_clean:
#         try:
#             if os.path.exists(temp_file):
#                 os.remove(temp_file)
#         except Exception as e:
#             logger.error(f"Fallo al eliminar archivo temporal {temp_file}: {e}")

#     return all_uploads_successful




def run_uploader(file_to_upload):
    """
    Principal function, called by metrics_receiver.py
    Process, upload and delete the RAW file if the upload is sucessful
    """
    logger.info("📡 Starting Metrics uploader...")
    init_sensor_file(SENSOR_FILE)

    # if not is_time_to_upload():
    #     logger.info("⏳ Not time to upload yet. Run this script at 5 minutes past the hour.")
    #     return

    # file_to_upload = get_previous_hour_file()
    if not os.path.exists(file_to_upload):
        logger.warning("⚠️ No data file to upload for previous hour: %s", file_to_upload)
        return

    success = submit_file_to_upstream(file_to_upload)
    if success:
        try:
            os.remove(file_to_upload)
            logger.info("🧹 Removed uploaded file %s", os.path.basename(file_to_upload))
        except Exception as e:
            logger.error("❌ Failed to remove uploaded file: %s", e)
    else:
        logger.warning("❌ Upload failed for fallida para %s. RAW file preserved.", os.path.basename(file_to_upload))

    return success


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(f"Executing manual upload with: {sys.argv[1]}")
        run_uploader(sys.argv[1])
    else:
        print("No command-line argument provided (CSV File Path)")





# def get_previous_hour_file():
#     """Return path for previous hour's CSV file."""
#     now = datetime.now()
#     prev_hour = now - timedelta(hours=1)
#     filename = os.path.join(LOG_DIR, f"rain_{prev_hour.strftime('%Y%m%d_%H')}.csv")
#     return  filename


# def is_time_to_upload():
#     """Check if current time is between HH:05:00 and HH:05:59 UTC."""
#     now = datetime.datetime.now()
#     # return now.minute == 5
#     return True


# def submit_file_to_upstream(file_path):
#     logger.info(f"🚀 Uploading {os.path.basename(file_path)} to Upstream...")
#     try:
#         client = UpstreamClient(
#             username=USERNAME,
#             password=PASSWORD,
#             base_url=BASE_URL,
#             ckan_url=CKAN_URL,
#             ckan_organization=CKAN_ORG
#         )
#     except Exception as e:
#         logger.error(f"❌ Upload failed for Upstream Client {e}")
#         return False
        
#     try:
#         client.upload_csv_data(
#             measurements_file=file_path,
#             sensors_file=SENSOR_FILE,
#             campaign_id=CAMPAIGN_ID,
#             station_id=STATION_ID
#         )
#         logger.info(f"✅ Successfully uploaded {file_path}")
#         return True
#     except Exception as e:
#         logger.error(f"❌ Upload failed for {file_path}: {e}")
#         return False



