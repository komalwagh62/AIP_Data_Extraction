import time
from model import RestrictedArea, Restricted, RestrictedAirspace, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    search_and_print_restricted_links
)

def process_restrictedDatas(urls):
 for url in urls:
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
        # Check if the current table has the desired structure
        if table.find('h6') and '5.1' in table.get_text():
            print_content = True

        # Print the contents if the flag is True
        if print_content:
            rows = table.find_all('tr')
            
            # Iterate through the rows
            for row in rows[1:]:
                # Find all cells in the row
                cells = row.find_all('td')
                
                # Extract upper and lower limits from the cells
                limit_text = cells[2].get_text(strip=True)
                if '/' in limit_text:
                    upper_limits, lower_limits = limit_text.split('/', 1)
                else:
                    upper_limits = lower_limits = limit_text.strip()
                
                # Extract identification and split into parts
                identification = cells[0].get_text(strip=True)
                
                # Extract the content of the first and second <p> tags
                first_p_content = cells[0].find_all('p')[0].get_text(strip=True)
                second_p_content = cells[0].find_all('p')[1].get_text(strip=True)

                # Split the first_p_content to extract prefix, number, and designator
                fir = first_p_content[:2].strip()
                type = first_p_content[2:3].strip()
                designation = first_p_content[3:].strip()
                designation = designation.replace('(', '').replace(')', '').replace('-', '').strip()
                print(designation)

                # Extract name from the content of the second <p> tag
                name = second_p_content.strip('[]')

                lateral_limits = cells[1].get_text(strip=True)
                remarks = cells[3].get_text(strip=True)
                # print(name)
                
                # Initialize variables to None
                restricted_airspace_entry = None
                restricted_entry = None

                # Query the Restricted table for matching name
                
                
                restricted_airspace_entry = session.query(RestrictedAirspace).filter((RestrictedAirspace.RestrictiveAirspaceName == name) & (RestrictedAirspace.RestrictiveAirspaceDesignation == designation)).first()

                # Determine the geom attribute based on available entries
                geom = None
                geometry=None
                if restricted_airspace_entry:
                    geom = restricted_airspace_entry.UR_restrictivePoligon
                    geometry=restricted_airspace_entry.eometry
                    # print(geometry)
                    restricted_area = RestrictedArea(
                        designation=designation,
                        name=name,
                        fir=fir,
                        type=type,
                        lateral_limits=lateral_limits,
                        upper_limit=upper_limits,
                        lower_limit=lower_limits,
                        geom=geom,
                        geometry=geometry,
                        remarks=remarks
                    )
                    session.add(restricted_area)
                
    # Commit changes outside the loop
    session.commit()


def process_restrictedDatas1(urls):
 for url in urls:
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
        # Check if the current table has the desired structure
        if table.find('h6') and '5.1' in table.get_text():
            print_content = True

        # Print the contents if the flag is True
        if print_content:
            rows = table.find_all('tr')
            
            # Iterate through the rows
            for row in rows[1:]:
                # Find all cells in the row
                cells = row.find_all('td')
                
                # Extract upper and lower limits from the cells
                limit_text = cells[2].get_text(strip=True)
                if '/' in limit_text:
                    upper_limits, lower_limits = limit_text.split('/', 1)
                else:
                    upper_limits = lower_limits = limit_text.strip()
                
                # Extract identification and split into parts
                identification = cells[0].get_text(strip=True)
                
                # Extract the content of the first and second <p> tags
                first_p_content = cells[0].find_all('p')[0].get_text(strip=True)
                second_p_content = cells[0].find_all('p')[1].get_text(strip=True)

                # Split the first_p_content to extract prefix, number, and designator
                fir = first_p_content[:2].strip()
                type = first_p_content[2:3].strip()
                designation = first_p_content[3:].strip()
                designation = designation.replace('(', '').replace(')', '').replace('-', '').strip()
                print(designation)

                # Extract name from the content of the second <p> tag
                name = second_p_content.strip('[]')

                lateral_limits = cells[1].get_text(strip=True)
                remarks = cells[3].get_text(strip=True)
                # print(name)
                
                # Initialize variables to None
                restricted_airspace_entry = None
                restricted_entry = None

                # Query the Restricted table for matching name
                restricted_entry = session.query(Restricted).filter((Restricted.Airspace_name == name) & (Restricted.designation == designation)).first()
                if restricted_entry:
                    geom = restricted_entry.geometry
                    geometry=restricted_entry.geom
                    print(geometry)
                    restricted_area = RestrictedArea(
                        designation=designation,
                        name=name,
                        fir=fir,
                        type=type,
                        lateral_limits=lateral_limits,
                        upper_limit=upper_limits,
                        lower_limit=lower_limits,
                        geom=geom,
                        geometry=geometry,
                        remarks=remarks
                    )
                    session.add(restricted_area)
                   
                
    # Commit changes outside the loop
    session.commit()


def main():
    processed_urls_file = "restricted_processed_urls.txt"
    eaip_url = find_eaip_url()
    if eaip_url:
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if base_frame_url:
            navigation_url = fetch_and_parse_navigation_frame(base_frame_url)
            if navigation_url:
                enr_5_1_urls = search_and_print_restricted_links(navigation_url, processed_urls_file)
                # Here you would process ENR 5.1
                process_restrictedDatas(enr_5_1_urls)
                process_restrictedDatas1(enr_5_1_urls)
                
if __name__ == "__main__":
    starttime = time.time()
    main()
    endtime = time.time()
    print(f"Execution time: {endtime - starttime:.2f} seconds")
        
            

