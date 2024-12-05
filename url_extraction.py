import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import time
import os

from model import session, AirportData  # Import session and any necessary models
def fetch_url_with_retries(url, retries=3, delay=5):
    for attempt in range(retries):
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, verify=False)
        if response.status_code == 200:
            return response
        else:
            # print(f"Attempt {attempt + 1} failed with status code {response.status_code}. Retrying in {delay} seconds...")
            time.sleep(delay)
    return None

def find_eaip_url(max_retries=3):
    url = "https://aim-india.aai.aero/aip-supplements?page=1"
    for attempt in range(max_retries):
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for li_tag in soup.find_all('li', class_="leaf new"):
                a_tag = li_tag.find('a', href=True)
                if a_tag and a_tag.text.strip().startswith("eAIP India"):
                    href = a_tag['href']
                    print(f"Found eAIP link: {href}")
                    return href
            print("No eAIP URL found.")
            return None
        else:
            # print(f"Attempt {attempt + 1} failed with status code {response.status_code}. Retrying in 5 seconds...")
            time.sleep(5)
    print("Failed to fetch the AIP supplements page after multiple attempts.")
    return None

def fetch_and_parse_frameset(eaip_url):
    response = fetch_url_with_retries(eaip_url)
    if response is None:
        print("Failed to fetch the EAIP frameset after multiple attempts.")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    frameset = soup.find('frameset', attrs={'onload': 'openTarget()'})
    if frameset:
        frame = frameset.find('frame', {'name': 'eAISNavigationBase'})
        if frame and 'src' in frame.attrs:
            frame_url = frame['src']
            return f"{eaip_url.rsplit('/', 1)[0]}/{frame_url}"
    print("No frame found with the name 'eAISNavigationBase'.")
    return None

def fetch_and_parse_navigation_frame(base_frame_url):
    response = fetch_url_with_retries(base_frame_url)
    if response is None:
        print("Failed to fetch the base frame page after multiple attempts.")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    frame = soup.find('frame', {'name': 'eAISNavigation'})
    if frame and 'src' in frame.attrs:
        frame_url = frame['src']
        return f"{base_frame_url.rsplit('/', 1)[0]}/{frame_url}"
    print("No frame found with the name 'eAISNavigation'.")
    return None

def get_base_url(navigation_url):
    parsed_url = urlparse(navigation_url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path.rsplit('/', 1)[0]}/"

def load_processed_urls(file_path):
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r') as f:
        return {line.strip() for line in f}

def save_processed_url(file_path, url):
    with open(file_path, 'a') as f:
        f.write(f"{url}\n")

# Search for ENR 3.1 and 3.2 links and print them if not already processed
def search_and_print_enr_links(navigation_url, processed_urls_file):
    processed_urls = load_processed_urls(processed_urls_file)
    script_name = os.path.basename(__file__)  # Get the current script name

    print(f"Checking if Navigation URL '{navigation_url}' has already been processed by '{script_name}'...")

    # Check if the navigation URL has already been processed by this script
    if f"{script_name}:{navigation_url}" in processed_urls:
        print(f"Navigation URL '{navigation_url}' already processed by '{script_name}'.")
        return [], []

    base_url = get_base_url(navigation_url)
    print(f"Base URL: {base_url}")

    response = fetch_url_with_retries(navigation_url)
    if response is None:
        print("Failed to fetch the navigation content after multiple attempts.")
        return [], []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    enr_3_1_urls = []
    enr_3_2_urls = []

    print("ENR 3.1 links:")
    for link in links:
        link_id = link.get('id', 'None')  # Default to 'None' to avoid KeyError
        href = link['href']

        # Skip if the URL is already processed by this script
        if f"{script_name}:{href}" in processed_urls:
            print(f"Skipping already processed URL: {href}")
            continue

        # Match ENR 3.1 links
        if re.match(r'^ENR 3\.1', link_id):
            modified_url = f"{base_url}{href.split('/')[-1]}"
            enr_3_1_urls.append(modified_url)
            print(f"ID: {link_id}, Modified URL: {modified_url}")
            save_processed_url(processed_urls_file, modified_url)  # Save the URL immediately

        # Match ENR 3.2 links
        elif re.match(r'^ENR 3\.2', link_id):
            modified_url = f"{base_url}{href.split('/')[-1]}"
            enr_3_2_urls.append(modified_url)
            print(f"ID: {link_id}, Modified URL: {modified_url}")
            save_processed_url(processed_urls_file, modified_url)  # Save the URL immediately

    # Save the navigation URL after processing all links
    save_processed_url(processed_urls_file, navigation_url)
    print(f"Saved Navigation URL '{navigation_url}' as processed by '{script_name}'.")

    return enr_3_1_urls, enr_3_2_urls


# Search for AD 2 airprot links and print them if not already processed
def fetch_and_print_airports(navigation_url, processed_file="airportData_urls.txt"):
    # Load already processed URLs from file
    processed_urls = set()
    if os.path.exists(processed_file):
        with open(processed_file, "r") as file:
            processed_urls = set(line.strip() for line in file)

    response = requests.get(navigation_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the navigation content. Status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    aerodromes = soup.find_all('a', href=True, id=re.compile(r'^AD 2\.1V[A-Z]+'))
    base_url = navigation_url.rsplit('/', 1)[0]
    new_urls = []  # Collect URLs that are new and unprocessed

    for aerodrome in aerodromes:
        aerodrome_href = aerodrome['href']
        airport_url = f"{base_url}/{aerodrome_href}"
        
        # Only add URLs that haven't been processed
        if airport_url not in processed_urls:
            new_urls.append(airport_url)

    # Append new URLs to the processed file
    if new_urls:
        with open(processed_file, "a") as file:
            for url in new_urls:
                file.write(f"{url}\n")

    return new_urls  # Return only new URLs


# Search for ENR 4.4 link and print them if not already processed
def search_and_print_waypoint_links(navigation_url, processed_waypoint_file):
    processed_urls = load_processed_urls(processed_waypoint_file)
    script_name = os.path.basename(__file__)

    print(f"Checking if Navigation URL '{navigation_url}' has already been processed by '{script_name}'...")

    # Skip processing if this navigation URL was already handled by this script
    if f"{script_name}:{navigation_url}" in processed_urls:
        print(f"Navigation URL '{navigation_url}' already processed by '{script_name}'.")
        return []

    base_url = get_base_url(navigation_url)
    print(f"Base URL: {base_url}")

    response = fetch_url_with_retries(navigation_url)
    if response is None:
        print("Failed to fetch the navigation content after multiple attempts.")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    enr_4_4_urls = []

    for link in links:
        link_id = link.get('id', 'None')
        href = link['href']
        modified_url = f"{base_url}{href.split('/')[-1]}"  # Create the final URL

        # Skip if the full URL is already processed by this script
        if f"{script_name}:{modified_url}" in processed_urls:
            print(f"Skipping already processed URL: {modified_url}")
            continue

        # Match ENR 4.4 links
        if re.match(r'^ENR 4\.4', link_id):
            enr_4_4_urls.append(modified_url)
            print(f"ID: {link_id}, Modified URL: {modified_url}")
            save_processed_url(processed_waypoint_file, f"{script_name}:{modified_url}")  # Save URL immediately

    # Mark the main navigation URL as processed
    save_processed_url(processed_waypoint_file, f"{script_name}:{navigation_url}")
    print(f"Saved Navigation URL '{navigation_url}' as processed by '{script_name}'.")

    return enr_4_4_urls


# Search for Aerdromes AD airport link and print them if not already processed
def extract_airport_details(icao_code):
    """
    Function to extract airport details based on the ICAO code from the airportData table.
    """
 
    try:
        # Query for airport details using the ICAO code
        airport = session.query(AirportData).filter_by(ICAOCode=icao_code).first()

        # Check if airport data is found
        if airport:
            airport_details = (
                f"ICAO Code: {airport.ICAOCode}\n"
            )
            return airport_details
        else:
            print(f"No airport found for ICAO code: {icao_code}")
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        session.close()



def fetch_and_print_ad_data(navigation_url, processed_file="AD_urls.txt"):
    # Load valid ICAO codes from the database
    valid_icao_codes = {airport.ICAOCode for airport in session.query(AirportData.ICAOCode).all()}

    # Load already processed URLs from file
    processed_urls = set()
    if os.path.exists(processed_file):
        with open(processed_file, "r") as file:
            processed_urls = set(line.strip() for line in file)

    response = requests.get(navigation_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the navigation content. Status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    aerodromes = soup.find_all('a', href=True, id=re.compile(r'^AD 2\.1V[A-Z]+'))
    base_url = navigation_url.rsplit('/', 1)[0]
    new_urls = []

    for aerodrome in aerodromes:
        aerodrome_href = aerodrome['href']
        airport_url = f"{base_url}/{aerodrome_href}"

        # Extract ICAO code from the URL or href
        icao_code_match = re.search(r"AD 2\.1(V[A-Z]+)", aerodrome_href)
        if icao_code_match:
            icao_code = icao_code_match.group(1)
            if icao_code in valid_icao_codes and airport_url not in processed_urls:
                new_urls.append(airport_url)

    # Append new URLs to the processed file
    if new_urls:
        with open(processed_file, "a") as file:
            for url in new_urls:
                file.write(f"{url}\n")

    return new_urls


# Search for ENR 5.1 link and print them if not already processed
def search_and_print_restricted_links(navigation_url, processed_restricted_file):
    processed_urls = load_processed_urls(processed_restricted_file)
    script_name = os.path.basename(__file__)

    print(f"Checking if Navigation URL '{navigation_url}' has already been processed by '{script_name}'...")

    # Skip processing if this navigation URL was already handled by this script
    if f"{script_name}:{navigation_url}" in processed_urls:
        print(f"Navigation URL '{navigation_url}' already processed by '{script_name}'.")
        return []

    base_url = get_base_url(navigation_url)
    print(f"Base URL: {base_url}")

    response = fetch_url_with_retries(navigation_url)
    if response is None:
        print("Failed to fetch the navigation content after multiple attempts.")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    enr_5_1_urls = []

    for link in links:
        link_id = link.get('id', 'None')
        href = link['href']
        modified_url = f"{base_url}{href.split('/')[-1]}"  # Create the final URL

        # Skip if the full URL is already processed by this script
        if f"{script_name}:{modified_url}" in processed_urls:
            print(f"Skipping already processed URL: {modified_url}")
            continue

        # Match ENR 4.4 links
        if re.match(r'^ENR 5\.1', link_id):
            enr_5_1_urls.append(modified_url)
            print(f"ID: {link_id}, Modified URL: {modified_url}")
            save_processed_url(processed_restricted_file, f"{script_name}:{modified_url}")  # Save URL immediately

    # Mark the main navigation URL as processed
    save_processed_url(processed_restricted_file, f"{script_name}:{navigation_url}")
    print(f"Saved Navigation URL '{navigation_url}' as processed by '{script_name}'.")

    return enr_5_1_urls



# Search for ENR 2.1 link and print them if not already processed
def search_and_print_controlled_links(navigation_url, processed_controlled_file):
    processed_urls = load_processed_urls(processed_controlled_file)
    script_name = os.path.basename(__file__)

    print(f"Checking if Navigation URL '{navigation_url}' has already been processed by '{script_name}'...")

    # Skip processing if this navigation URL was already handled by this script
    if f"{script_name}:{navigation_url}" in processed_urls:
        print(f"Navigation URL '{navigation_url}' already processed by '{script_name}'.")
        return []

    base_url = get_base_url(navigation_url)
    print(f"Base URL: {base_url}")

    response = fetch_url_with_retries(navigation_url)
    if response is None:
        print("Failed to fetch the navigation content after multiple attempts.")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    enr_2_1_urls = []

    for link in links:
        link_id = link.get('id', 'None')
        href = link['href']
        modified_url = f"{base_url}{href.split('/')[-1]}"  # Create the final URL

        # Skip if the full URL is already processed by this script
        if f"{script_name}:{modified_url}" in processed_urls:
            print(f"Skipping already processed URL: {modified_url}")
            continue

        # Match ENR 4.4 links
        if re.match(r'^ENR 2\.1', link_id):
            enr_2_1_urls.append(modified_url)
            print(f"ID: {link_id}, Modified URL: {modified_url}")
            save_processed_url(processed_controlled_file, f"{script_name}:{modified_url}")  # Save URL immediately

    # Mark the main navigation URL as processed
    save_processed_url(processed_controlled_file, f"{script_name}:{navigation_url}")
    print(f"Saved Navigation URL '{navigation_url}' as processed by '{script_name}'.")

    return enr_2_1_urls
