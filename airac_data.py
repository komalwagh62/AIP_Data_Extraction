from model import AiracData, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
from shapely import wkt
import os
import re
import uuid
from datetime import datetime
from url_extraction import find_eaip_url, fetch_and_parse_frameset

# Function to check if the URL is already in the processed URLs file
def is_url_processed(url, processed_urls_file):
    if os.path.exists(processed_urls_file):
        with open(processed_urls_file, 'r') as file:
            processed_urls = file.readlines()
            # Check if the URL is already in the processed list
            if url + '\n' in processed_urls:
                return True
    return False

# Function to update the previous record status to False
def update_previous_record_status():
    try:
        # Find the most recent active record (status=True)
        last_active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
        if last_active_record:
            # Update its status to False
            last_active_record.status = False
            session.commit()
            print(f"Updated status of previous record to False, Process Name: {last_active_record.process_name}")
    except Exception as e:
        session.rollback()  # In case of an error, rollback the session
        print(f"Error updating previous record status: {e}")

# Function to insert airac data into the database
def insert_airac_data(eaip_url, processed_urls_file):
    try:
        # Check if URL has already been processed
        if is_url_processed(eaip_url, processed_urls_file):
            print(f"URL {eaip_url} has already been processed. Skipping insertion.")
            return

        # Extract year and date from the URL (you might need to adjust this based on the format of your URL)
        match = re.search(r'v2-(\d{2})-(\d{4})', eaip_url)  # Assuming the URL format contains v2-<month>-<year>
        if match:
            month = match.group(1)
            year = match.group(2)
            valid_from_date = f"{year}-{month}-01"  # Default to the 1st of the month
            process_name = f"airac_{year}"  # Format process name as airac_<year>
            valid_from_obj = datetime.strptime(valid_from_date, "%Y-%m-%d")
        else:
            print("No date found in the URL.")
            return

        # Generate a unique process ID
        process_id = str(uuid.uuid4())

        # Update previous record's status to False
        update_previous_record_status()

        # Insert data into the database for the new record
        new_airac_data = AiracData(
            process_name=process_name,
            created_At=datetime.now(),
            valid_from=valid_from_obj,
            status=True  # Set the status to True (active) for the new record
        )
        session.add(new_airac_data)
        session.commit()
        print(f"Inserted new record with Process Name: {process_name}, Valid From: {valid_from_obj}, Status: Active")

        # Write the processed URL to the file to avoid future duplication
        with open(processed_urls_file, 'a') as file:
            file.write(eaip_url + '\n')
        print(f"Processed URL {eaip_url} added to the processed list.")

    except IntegrityError:
        session.rollback()  # Rollback in case of a database integrity error (e.g., duplicate URL)
        print("IntegrityError: The URL is already present in the database.")
    except Exception as e:
        session.rollback()  # In case of other errors, rollback the session
        print(f"Error inserting data: {e}")

# Main function to drive the URL extraction and processing flow
def main():
    processed_urls_file = "airac_data.url.txt"
    
    # Find the eAIP URL
    eaip_url = find_eaip_url()
    print(f"Found eAIP link: {eaip_url}")
    
    if eaip_url:
        insert_airac_data(eaip_url, processed_urls_file)

if __name__ == "__main__":
    main()
