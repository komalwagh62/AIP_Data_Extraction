from model import MilitaryControlZones, session
from sqlalchemy.exc import IntegrityError
from bs4 import BeautifulSoup
import requests

# Define the URL of the webpage containing the data
url = 'https://aim-india.aai.aero/eaip-v2-02-2024/eAIP/IN-ENR%202.1-en-GB.html'

# Send an HTTP GET request to the URL
response = requests.get(url, verify=False)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the HTML content of the webpage
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all table elements
    tables = soup.find_all('table', class_='AmdtTable')

    # Flag to indicate whether to print the contents
    print_content = False

    # Iterate through the tables
    for table in tables:
        # Find all rows in the table
        rows = table.find_all('tr')

        # Iterate through the rows
        for row in rows[1:]:
            # Check if the current row contains the desired text
            if "2.1.4.7" in row.get_text():
                # Set the flag to indicate to start printing the content
                print_content = True
            elif print_content:
                # Extract data from the cells
              cells = row.find_all('td')
              print(cells)
              if len(cells) >= 4:
                 
                # Extract name and lateral limits from cells
                name = cells[0].get_text(strip=True)
              
                unit_service = cells[1].get_text(strip=True)
                call_sign = cells[2].get_text(strip=True)
                frequency = cells[3].get_text(strip=True)
                remarks = cells[4].get_text(strip=True)
                
                # Uncomment the following lines to save the data to the database
                military_control_zone = MilitaryControlZones(
                    name=name,
                    unit_service=unit_service,
                    call_sign=call_sign,
                    frequency=frequency,
                    remarks=remarks,
                )
                session.add(military_control_zone)
        
    session.commit()
