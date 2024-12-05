import re
from model import RestrictedArea,Restrict,Restricted, session
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup

# Define the URL of the webpage containing the data
url = 'https://aim-india.aai.aero/eaip-v2-02-2024/eAIP/IN-ENR%205.2-en-GB.html'

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
        if table.find('h6', class_='Numbering-left') and '5.2.1' in table.get_text():
            print_content = True

        # Print the contents if the flag is True
        if print_content:
            rows = table.find_all('tr')
            
            # Iterate through the rows
            for row in rows[6:]:
                # Find all cells in the row
                cells = row.find_all('td')
                
                # Extract data from the first cell (name column)
                
                # print(name)
                first_p_content = cells[0].find_all('p')[0].get_text(strip=True)
                

                # Split the first_p_content to extract prefix, number, and designator
                fir = first_p_content[:2].strip()
                type = first_p_content[2:3].strip()
                designation = first_p_content[3:].strip()
                stop_chars = ['[']
                stop_pos = min([designation.find(char) for char in stop_chars if char in designation] + [len(designation)])
                designation = designation[:stop_pos].strip()
                designation = designation.replace('(', '').replace(')', '').replace('-', '').strip()
                # print(designation)
                bracket_contents = re.findall(r'\[(.*?)\]', first_p_content)

# If there are any matches, take the first one and clean it
                
                
                name = bracket_contents[0].replace('[', '').replace(']', '').strip()
                print(name)
                print(designation)
                
                
                lateral_limits = cells[0].find_all('p')[1].get_text(strip=True)
                upper_limits, lower_limits = cells[1].get_text(strip=True).split('/', 1)
                remarks = cells[2].get_text(strip=True)
                restricted_entry = None
                
                restricted_entry = session.query(Restrict).filter((Restrict.RestrictiveAirspaceName == name) & (Restrict.RestrictiveAirspaceDesignation == designation)).first()
                print(restricted_entry)
                if restricted_entry:
                    geom = restricted_entry.UR_restrictivePoligon
                    geometry=restricted_entry.eometry
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
                
                # restricted_entry = session.query(Restricted).filter((Restricted.Airspace_name == name) & (Restricted.designation == designation)).first()
                # if restricted_entry:
                #     geom = restricted_entry.geometry
                #     geometry=restricted_entry.geom
                #     print(geometry)
                #     restricted_area = RestrictedArea(
                #         designation=designation,
                #         name=name,
                #         fir=fir,
                #         type=type,
                #         lateral_limits=lateral_limits,
                #         upper_limit=upper_limits,
                #         lower_limit=lower_limits,
                #         geom=geom,
                #         geometry=geometry,
                #         remarks=remarks
                #     )
                #     session.add(restricted_area)
        # Check if the current table is the ending point
        if table.find('h6', class_='Numbering-left') and ' 5.2.2' in table.get_text():
            break
    session.commit()
