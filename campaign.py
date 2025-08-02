from upstream.client import UpstreamClient
from upstream_api_client.models import CampaignsIn, StationCreate
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

# Initialize client with CKAN integration
client = UpstreamClient(
    username=os.getenv('userid'),
    password=os.getenv('password'),
    base_url="https://upstream-dso.tacc.utexas.edu",
    ckan_url="https://ckan.tacc.utexas.edu",
    ckan_organization="dso-internal"
)

# Create campaign
campaign_data = CampaignsIn(
    name="Alaska 2025",
    description="Rain Gauge Test Low cost sensor for Alaska 2025",
    contact_name="Dr. Kasey Faust",
    contact_email="",
    allocation="PT2050-DataX",
    start_date=datetime.now(),
    end_date=datetime.now() + timedelta(days=365)
)
campaign = client.create_campaign(campaign_data)

# Create monitoring station
station_data = StationCreate(
    name="Bethel Rain Gauge",
    description="Single sensor rain gauge, captures data every minute",
    contact_name="Dr. William Mobley",
    contact_email="wmobley@tacc.utexas.edu",
    start_date=datetime.now()
)
station = client.create_station(campaign.id, station_data)
