import re
import requests
from bs4 import BeautifulSoup
from model import RestrictedAreas,Restricted,AiracData,AirportData, session
from sqlalchemy import func
from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    search_and_print_restricted_links
)
# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None
class AirspaceExtractor:
    def __init__(self, aip_base_url):
        self.aip_base_url = aip_base_url

    def find_and_process_enr_5_1(self, tables, heading, restrict_type):
        """
        Search for a table containing the specified heading and process the next table.
        """
        for i, table in enumerate(tables):
            # Check if the current table contains the heading
            heading_tag = table.find("h6")
            if heading_tag and heading_tag.get_text(strip=True) == heading:
                print(f"Found table for: {heading}")
                # Process the next table if it exists
                if i + 1 < len(tables):
                    self.process_table_enr_5_1(tables[i + 1], restrict_type)
                else:
                    print(f"No table found after heading: {heading}")
                return  # Stop after processing the first matching heading
        print(f"Heading '{heading}' not found in any table.")

    def process_table_enr_5_1(self, table, region):
        """
        Extract and print data from the specified table.
        """
        print("Processing table data...")
        process_id = get_active_process_id()
        tr_list = table.find_all("tr")[1:]  # Skip the header row
        for tr in tr_list:
            td_list = tr.find_all("td")
            if len(td_list) < 3:
                print(f"Skipping row due to insufficient columns: {tr}")
                continue

            # Extract Identification and Name
            p_tags = td_list[0].find_all("p")
            identification = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else "N/A"
            code = identification[:2]
            restrictive_type = identification[2:3]
            name = (p_tags[1].get_text(strip=True) if len(p_tags) > 1 else "N/A") + \
                     (p_tags[2].get_text(strip=True) if len(p_tags) > 2 else "") + \
                    (p_tags[3].get_text(strip=True) if len(p_tags) > 3 else "") + \
                    (p_tags[4].get_text(strip=True) if len(p_tags) > 4 else "")

            # Extract Lateral Data
            lateral_data = td_list[1].get_text(strip=True) if len(td_list) > 1 else "None"

            # Extract Upper and Lower Limits
            td_data = td_list[2].get_text(strip=True) if len(td_list) > 2 else "N/A"
            upper_limit, lower_limit = "N/A", "N/A"
            if "/" in td_data:
                parts = td_data.split("/")
                upper_limit = parts[0].strip()
                lower_limit = parts[1].strip() if len(parts) > 1 else "N/A"
            else:
                upper_limit = td_data  # If no '/', treat the entire content as upper_limit

            # Extract Remarks
            remark = td_list[-1].get_text(strip=True) if len(td_list) > 2 else "None"
            # Ensure the name comparison is case-insensitive
            restricted_airspace_entry = session.query(Restricted).filter(
            (func.lower(Restricted.name) == func.lower(name)) & 
            (Restricted.Identi_No == identification)
            ).first()
            # Extract city name safely
            if not name or name.strip() == "":  # Handle empty or None names
                city_name = 'UNKNOWN'
            else:
                try:
                    city_name = re.sub(r'[\[\]]', '', name.split()[0]).strip()  # Remove brackets and leading/trailing spaces
                except IndexError:
                    city_name = 'UNKNOWN'
            
            print(city_name)
            
            airport_record = session.query(AirportData).filter(AirportData.city.ilike(f"%{city_name}%")).first()

            if airport_record:
                icao_code = airport_record.ICAOCode
                print(f"Matching city found: {city_name}")
            else:
                icao_code = None
                print(f"No matching city found for: {city_name}")
            # Create and save database entry
            if restricted_airspace_entry: 
                                       
                geom = restricted_airspace_entry.geom
                geometry = restricted_airspace_entry.geometry
                airspace_entry = RestrictedAreas(
                identification=identification,
                code = code,
                restrictive_type = restrictive_type,
                name=name,
                icao_code=icao_code if icao_code else '',  # Default to 'NON-E'
                region=region,
                lateral_limits=lateral_data,
                upper_limits=upper_limit,
                lower_limits=lower_limit,
                geom=geometry,
                geometry=geom,
                remarks=remark,
                process_id=process_id
                )
                session.add(airspace_entry)
                session.commit()

    def extract_and_process_enr_5_1(self):
        process_id = get_active_process_id()
        """
        Fetch the webpage, clean it, and process data for each specified region.
        """
        response = requests.get(self.aip_base_url, verify=False)
        soup = BeautifulSoup(response.text, "html.parser")

        # Remove unwanted tags
        for del_tag in soup.find_all("del"):
            del_tag.decompose()
        for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
            hidden_tag.decompose()

        # Find all tables
        tables = soup.find_all("table")

        # Process each region
        regions = [
            ("Prohibited, Restricted and Danger Airspace - Chennai Region", "Chennai Region"),
            ("Prohibited, Restricted and Danger Airspace - Mumbai Region", "Mumbai Region"),
            ("Prohibited, Restricted and Danger Airspace - Delhi Region", "Delhi Region"),
            ("Prohibited, Restricted and Danger Airspace - Kolkata Region", "Kolkata Region"),
        ]
        for heading, restrict_type in regions:
            self.find_and_process_enr_5_1(tables, heading, restrict_type)

    def find_and_process_enr_5_2(self, tables, heading):
        """
        Search for a table containing the specified heading and process the next table.
        """
        for i, table in enumerate(tables):
            # Check if the current table contains the heading
            heading_tag = table.find("h6")
            if heading_tag and heading_tag.get_text(strip=True) == heading:
                # print(f"Found table for: {heading}")
                # Process the next table if it exists
                if i + 1 < len(tables):
                    self.process_table_enr_5_2(tables[i + 1])
                else:
                    print(f"No table found after heading: {heading}")
                return  # Stop after processing the first matching heading
       

    def process_table_enr_5_2(self, table):
        process_id = get_active_process_id()
        """
        Extract and print data from the specified table.
        """
        print("Processing table data...")
        tr_list = table.find_all("tr")[1:]  # Skip the header row
        for tr in tr_list:
            td_list = tr.find_all("td")
            if len(td_list) < 3:
                print(f"Skipping row due to insufficient columns: {tr}")
                continue

            # Extract Identification and Name
            p_tags = td_list[0].find_all("p")
            identification = p_tags[0].get_text(strip=True).split('[')[0].strip() if '[' in p_tags[0].get_text(strip=True) else p_tags[0].get_text(strip=True)
            if identification[:3] == 'TRA':
                region = 'Temporary Reserved Area (TRA)'
            elif identification[:3] == 'TSA':
                region = 'Temporary Segregated Area (TSA)'
            name = re.search(r'\[([^\]]+)\]', p_tags[0].get_text(strip=True))
            name = name.group(0) if name else "N/A"
            # Extract Lateral Data
            lateral_data = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else "N/A"
            # Extract Upper and Lower Limits
            td_data = td_list[1].get_text(strip=True) if len(td_list) > 2 else "N/A"
            upper_limit, lower_limit = "N/A", "N/A"
            if "/" in td_data:
                parts = td_data.split("/")
                upper_limit = parts[0].strip()
                lower_limit = parts[1].strip() if len(parts) > 1 else "N/A"
            else:
                upper_limit = td_data  # If no '/', treat the entire content as upper_limit
            # Extract Remarks
            remark = td_list[-1].get_text(strip=True) if len(td_list) > 2 else "None"
            # Ensure the name comparison is case-insensitive
            restricted_airspace_entry = session.query(Restricted).filter(
            (func.lower(Restricted.name) == func.lower(name)) & 
            (Restricted.Identi_No == identification)
            ).first()
            # Check if city name matches any entry in airportData table
            # Clean up the name to remove square brackets
            city_name = re.sub(r'[\[\]]', '', name).strip()  # Remove only the square brackets

            print(city_name)
            # city_name = name.split()[0]  # Extract first string as city name
            airport_record = session.query(AirportData).filter(AirportData.city.ilike(f"%{city_name}%")).first()

            if airport_record:
                icao_code = airport_record.ICAOCode
                print(f"Matching city found: {city_name}")
            else:
                icao_code = None
                print(f"No matching city found for: {city_name}")
            # Create and save database entry
            # Create and save database 
            if restricted_airspace_entry:                        
                geom = restricted_airspace_entry.geom
                geometry = restricted_airspace_entry.geometry
                airspace_entry = RestrictedAreas(
                identification=identification,
                name=name,
                icao_code=icao_code if icao_code else '',  # Default to 'NON-E'
                code = "",
                restrictive_type = "",
                region=region,
                lateral_limits=lateral_data,
                upper_limits=upper_limit,
                lower_limits=lower_limit,
                geom=geometry,
                geometry=geom,
                remarks=remark,
                process_id=process_id
                )
                session.add(airspace_entry)
                session.commit()

   
    def extract_and_process_enr_5_2(self):
        """
        Fetch the webpage, clean it, and process data for each specified region.
        """
        response = requests.get(self.aip_base_url, verify=False)
        soup = BeautifulSoup(response.text, "html.parser")

        # Remove unwanted tags
        for del_tag in soup.find_all("del"):
            del_tag.decompose()
        for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
            hidden_tag.decompose()

        # Find all tables
        tables = soup.find_all("table")

        # Process the required section (5.2.1 and 5.2.2)
        regions = [
            ("5.2.1"),
            
        ]
        for heading in regions:
            self.find_and_process_enr_5_2(tables, heading)


def main():
    processed_urls_file = "restricted_processed_urls.txt"
    eaip_url = find_eaip_url()
    if eaip_url:
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if base_frame_url:
            navigation_url = fetch_and_parse_navigation_frame(base_frame_url)
            if navigation_url:
                enr_5_1_urls,enr_5_2_urls = search_and_print_restricted_links(navigation_url, processed_urls_file)
                # Select a valid URL from the list
                
                extractor = AirspaceExtractor(enr_5_1_urls[0])  # Pick first URL
                extractor.extract_and_process_enr_5_1()
                extractor = AirspaceExtractor(enr_5_2_urls[0])  # Pick first URL
                extractor.extract_and_process_enr_5_2()

# def main():
#     aip_base_url = 'https://aim-india.aai.aero/eaip-v2-07-2024/eAIP/IN-ENR%205.2-en-GB.html'
#     extractor = AirspaceExtractor(aip_base_url)
#     extractor.extract_and_process_enr_5_2()


if __name__ == "__main__":
    main()
