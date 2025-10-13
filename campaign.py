from upstream.client import UpstreamClient
from upstream_api_client.models import CampaignsIn, StationCreate
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys
import os

# === ENVIRONMENT  VARIABLES ===
load_dotenv("./Env/.env.public")  # Public env variables
load_dotenv("./Env/.env")         # Tapis credentials

# Initialize client with CKAN integration
try:
    client = UpstreamClient(
        username=os.getenv('userid'),
        password=os.getenv('password'),
        base_url=os.getenv('BASE_URL'),
        ckan_url=os.getenv('CKAN_URL'),
        ckan_organization="dso-internal"
    )

    if client.authenticate():
        print("✅ Connected successfully!")

except Exception as e:
    print(f"\n❌ Authentication Error or Not Allowed!\n\n{str(e)}")
    sys.exit(0)

# Create campaign
# campaign_data = CampaignsIn(
#     name="Alaska 2025",
#     description="Rain Gauge Test Low cost sensor for Alaska 2025",
#     contact_name="Dr. Kasey Faust",
#     contact_email="",
#     allocation="PT2050-DataX",
#     start_date=datetime.now(),
#     end_date=datetime.now() + timedelta(days=365)
# )
# campaign = client.create_campaign(campaign_data)

# # Create monitoring station
# station_data = StationCreate(
#     name="Bethel Rain Gauge",
#     description="Single sensor rain gauge, captures data every minute",
#     contact_name="Dr. William Mobley",
#     contact_email="wmobley@tacc.utexas.edu",
#     start_date=datetime.now()
# )
# station = client.create_station(campaign.id, station_data)


###############################################################################
# Create campaign
campaign_data = CampaignsIn(
    name="Test 2025",
    description="Rain Gauge and Flood Sensor Test",
    contact_name="Sebastian Hernandez Sterling",
    contact_email="",
    # Project Charge Code from TACC dashboard
    allocation="PT2050-DataX",
    start_date=datetime.now(),
    end_date=datetime.now() + timedelta(days=365)
)
campaign = client.create_campaign(campaign_data)
print(f"Campaign created with ID: {campaign.id}")

# Create monitoring station
station_data = StationCreate(
    name="test - Sebastian",
    description="Just a test 01",
    contact_name="Sebastian Hernandez Sterling",
    contact_email="sebastian@gmail.com",
    start_date=datetime.now()
)
station = client.create_station(campaign.id, station_data)
print(f"Station created with ID: {station.id}")
###############################################################################
