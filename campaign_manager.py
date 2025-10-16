from upstream.client import UpstreamClient
from upstream_api_client.models import CampaignsIn, StationCreate
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Optional

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
        ckan_organization=os.getenv('CKAN_ORG'),
    )

    if client.authenticate():
        print("✅ Connected to Upstream Successfully!\n")

except Exception as e:
    print(f"\n❌ Authentication Error or Not Allowed!\n\n{str(e)}")
    sys.exit(0)


# Clases modelo (simuladas para el ejemplo)
class CampaignsIn:
    def __init__(self, name: str, description: str, contact_name: str, 
                contact_email: str, allocation: str, 
                start_date: datetime, end_date: datetime):
        self.name = name
        self.description = description
        self.contact_name = contact_name
        self.contact_email = contact_email
        self.allocation = allocation
        self.start_date = start_date
        self.end_date = end_date

class StationCreate:
    def __init__(self, name: str, description: str, contact_name: str, 
                contact_email: str, start_date: datetime):
        self.name = name
        self.description = description
        self.contact_name = contact_name
        self.contact_email = contact_email
        self.start_date = start_date


# ##################### For Testing purposes #####################
class MockClient:
    def create_campaign(self, campaign_data: CampaignsIn):
        print("\nCreating Campaign...")
        # Simulación de creación en base de datos
        campaign = type('', (), {'id': 'camp-12345'})()
        return campaign
    
    def create_station(self, campaign_id: str, station_data: StationCreate):
        print("\nCreating Station...")
        # Simulación de creación en base de datos
        station = type('', (), {'id': 'stat-67890'})()
        return station
##################################################################


def get_input(prompt: str, required: bool = True, default: Optional[str] = None) -> str:
    while True:
        value = input(prompt)
        if not value and required and not default:
            print("This field is requires!.")
            continue
        return value or default


def use_template():
    """Preconfigured campaign (You can Edit it here if it's easier)"""
    ###############################################################################

    # Create campaign
    # campaign_data = CampaignsIn(
    #     name="Alaska 2025",
    #     description="Rain Gauge Test Low cost sensor for Alaska 2025",
    #     contact_name="Dr. Kasey Faust",
    #     contact_email="",
    #     # Project Charge Code from TACC dashboard
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


    # Create campaign
    campaign_data = CampaignsIn(
        name="Flood Sensor Test 2025",
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
        description="Just a test 02",
        contact_name="Sebastian Hernandez Sterling",
        contact_email="sebastian@gmail.com",
        start_date=datetime.now()
    )
    # station = client.create_station(campaign.id, station_data)
    # print(f"Station created with ID: {station.id}")
    ###############################################################################
    return campaign_data, station_data


def manual_input():
    """Register the data manually"""
    print("\n===📢 Enter the data for the Campaign  📢===")
    campaign_data = CampaignsIn(
        name=get_input("Campaign name: "),
        description=get_input("Description: "),
        contact_name=get_input("Contact name: "),
        contact_email=get_input("Contact Email (optional): ", required=False, default=""),
        allocation=get_input("Project Code (Allocation): "),
        start_date=datetime.now(),
        end_date=datetime.now() + timedelta(days=365)
    )
    
    print("\n===📢 Enter the data for the Station 📢===")
    station_data = StationCreate(
        name=get_input("Station name: "),
        description=get_input("Description: "),
        contact_name=get_input("Contact name: "),
        contact_email=get_input("Contact Email: "),
        start_date=datetime.now()
    )
    return campaign_data, station_data


def main():
    # For Testing
    # client = MockClient()
    
    while True:

        print("===📢 Campaign and Station Manager  📢===\n")
        print("1. 📄 Use Preconfigured Campaign (In File ⚠️)")
        print("2. 📝 New Campaign (Register Manually)")
        print("3. Exit  ➡️🚪")

        choice = input("\nSelect an Option (1-3): ")
        
        if choice == '1':
            campaign_data, station_data = use_template()
        elif choice == '2':
            campaign_data, station_data = manual_input()
        elif choice == '3':
            print("Exiting the program ➡️🚪...")
            return
        else:
            print("Invalid option, please select between: 1, 2 or 3.")
            continue
        
        # Show data before create
        print("\n=== Data Summary ===")
        print("Campaign:")
        print(f"  Name: {campaign_data.name}")
        print(f"  Description: {campaign_data.description}")
        print(f"  Contact: {campaign_data.contact_name}")
        print(f"  Contact: {campaign_data.allocation}")
        print("\nStation:")
        print(f"  Name: {station_data.name}")
        print(f"  Description: {station_data.description}")
        print(f"  Contact: {station_data.contact_name}")
        print(f"  Contact: {station_data.contact_email}")
        
        confirm = input("\n⚠️ Confirm Creation? (y/n): ").lower()
        if confirm != 'y':
            print("❌ Creation cancelled.\n")
            continue
        
        try:
            # Create the resources
            campaign = client.create_campaign(campaign_data)
            print(f"Campaign created with ID: {campaign.id}")
            
            station = client.create_station(campaign.id, station_data)
            print(f"Station created with ID: {station.id}")
            
            print("\n✅ Operation successfully completed!\n")

        except Exception as e:
            print(f"\n❌ An error has occurred: \n\n{str(e)}")
            sys.exit(0)


if __name__ == "__main__":
    main()
