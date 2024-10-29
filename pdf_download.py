import time
import requests
from bs4 import BeautifulSoup
import re
import os

# Function to find the 'eaip' URL from the base page
def find_eaip_url():
    url = "https://aim-india.aai.aero/aip-supplements?page=1"
    response = requests.get(url, verify=False)

    if response.status_code != 200:
        print(f"Failed to fetch the AIP supplements page. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Look for links containing 'eaip'
    for li_tag in soup.find_all('li', class_="leaf new"):
        a_tag = li_tag.find('a', href=True)
        if a_tag and a_tag.text.strip().startswith("eAIP India"):
            href = a_tag['href']
            print(f"Found eAIP link: {href}")
            return href  # Return the first matching eAIP URL

    print("No EAIP URL found.")
    return None

# Fetch the frame with name "eAISNavigationBase"
def fetch_and_parse_frameset(eaip_url):
    response = requests.get(eaip_url, verify=False)

    if response.status_code != 200:
        print(f"Failed to fetch the EAIP frameset. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    frameset = soup.find('frameset', attrs={'onload': 'openTarget()'})
    
    if frameset:
        frame = frameset.find('frame', {'name': 'eAISNavigationBase'})
        if frame and 'src' in frame.attrs:
            frame_url = frame['src']
            return f"{eaip_url.rsplit('/', 1)[0]}/{frame_url}"  # Return full frame URL

    print("No frame found with the name 'eAISNavigationBase'.")
    return None

# Fetch the second frame with name "eAISNavigation"
def fetch_and_parse_navigation_frame(base_frame_url):
    response = requests.get(base_frame_url, verify=False)

    if response.status_code != 200:
        print(f"Failed to fetch the base frame page. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    frame = soup.find('frame', {'name': 'eAISNavigation'})
    
    if frame and 'src' in frame.attrs:
        frame_url = frame['src']
        return f"{base_frame_url.rsplit('/', 1)[0]}/{frame_url}"  # Return full navigation frame URL

    print("No frame found with the name 'eAISNavigation'.")
    return None

# Function to download PDFs
def download_pdf(pdf_url, directory):
    response = requests.get(pdf_url, verify=False)

    if response.status_code == 200:
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        pdf_file_path = os.path.join(directory, pdf_url.split("/")[-1])
        with open(pdf_file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {pdf_file_path}")
    else:
        print(f"Failed to download PDF from {pdf_url}. Status code: {response.status_code}")

# Function to extract and print airport names and hrefs
def fetch_and_print_airports(navigation_url):
    response = requests.get(navigation_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the navigation content. Status code: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find all relevant <a> tags with aerodrome ids
    aerodromes = soup.find_all('a', href=True, id=re.compile(r'^AD 2\.1V[A-Z]+'))
    base_url = navigation_url.rsplit('/', 1)[0]  # Extract base URL
    # Loop through and print aerodrome names and their hrefs
    for aerodrome in aerodromes:
        aerodrome_name = aerodrome.text.strip()
        aerodrome_href = aerodrome['href']
        print(f"{aerodrome_name}: {aerodrome_href}")
        # Fetch and print the iframe src for each aerodrome
        
        fetch_and_print_iframe_src(aerodrome_href, base_url)
        
# Function to fetch each airport page and extract the IFRAME src
def fetch_and_print_iframe_src(airport_href, base_url):
    airport_url = f"{base_url}/{airport_href}" # Construct the full URL
    response = requests.get(airport_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the airport page. Status code: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find the iframe and extract the 'src' attribute
    iframe = soup.find('iframe', {'id': 'IncludeFileBottom'})
    if iframe and 'src' in iframe.attrs:
        iframe_src = iframe['src']
        full_iframe_url = f"{airport_url.rsplit('/', 1)[0]}/{iframe_src}"  # Full URL for iframe src
        print(f"Iframe source: {full_iframe_url}")
        # Now check the contents of the iframe page for PDF links
        check_iframe_content_for_pdfs(full_iframe_url, airport_href)
    else:
        print("No iframe found with the ID 'IncludeFileBottom'.")

# Function to check the iframe content for PDF links
def check_iframe_content_for_pdfs(iframe_url, airport_href):
    response = requests.get(iframe_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the iframe content. Status code: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find all PDF links in the iframe content
    pdf_links = soup.find_all('a', href=True)
    directory_name = f"downloads/{airport_href.split('/')[-1].replace('.html', '')}"
    for link in pdf_links:
        pdf_url = link['href']
        if pdf_url.endswith('.pdf'):
            # Construct the full PDF URL
            full_pdf_url = pdf_url if pdf_url.startswith("http") else f"{iframe_url.rsplit('/', 1)[0]}/{pdf_url}"
            download_pdf(full_pdf_url, directory_name)

def main():
    eaip_url = find_eaip_url()
    if eaip_url:
        # Fetch and process the frame content
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if base_frame_url:
            navigation_frame_url = fetch_and_parse_navigation_frame(base_frame_url)
            if navigation_frame_url:
                
                # Fetch and print the airport data (name and href)
                fetch_and_print_airports(navigation_frame_url)
    else:
        print("No EAIP URL found.")

if __name__ == "__main__":
    starttime = time.time()
    main()
    endtime = time.time()
    print(endtime-starttime,"time")
