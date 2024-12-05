import requests
from bs4 import BeautifulSoup
from model import TerminalControlArea, session

# Define the URL of the webpage containing the data
url = 'https://aim-india.aai.aero/eaip-v2-07-2024/eAIP/IN-ENR%202.1-en-GB.html'

# Send an HTTP GET request to the URL
response = requests.get(url, verify=False)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    tables = soup.find_all('table', class_='AmdtTable')

    ### Terminal Control Area ##########
    print_content = False
    seen_fir = set()  # To store FIR names and avoid duplicates
    printed_p_tags = set()  # To track printed p_tag texts

    for table in tables:
        # Check if the table contains the specific title "2.1.3 Terminal Control Area"
        if table.find('p', class_='Undertitle-centered') and '2.1.3 Terminal Control Area' in table.get_text():
            print_content = True

        if print_content:
            # Iterate through rows of the specific table (not the entire document)
            rows = table.find_all('tr')

            for row in rows:
                first_td = row.find_all('td')
                if first_td:  # If there is at least one td in the row
                    # Check if the first p tag within the table has the specific class
                    first_p = row.find('p', class_='Paragraph-text-left')
                    if first_p and first_p.get_text(strip=True) not in printed_p_tags:
                        name = first_p.get_text(strip=True)
                        print("Name:", name)  # Print the name
                        printed_p_tags.add(first_p.get_text(strip=True))  # Mark as printed

                    # Extract all <p> tags with the class 'Paragraph-text-left'
                    p_tags = row.find_all('p', class_='Paragraph-text-left')

                    # Ensure there are at least three <p> tags before accessing the third
                    if len(p_tags) >= 3:
                        print(p_tags)
                        # Start from the third <p> tag
                        for p_tag in p_tags[2:]:
                            # Check if the p_tag is not empty, contains text, and is not a &nbsp;
                            tag_text = p_tag.get_text(strip=True)
                            # print(tag_text)
                            
                    # Check for end of the "Terminal Control Area" section and stop processing
                    if '2.1.4 Military Control Zones' in row.get_text():
                        break

            # If "2.1.4 Military Control Zones" is found, stop processing further tables
            if '2.1.4 Military Control Zones' in table.get_text():
                break
