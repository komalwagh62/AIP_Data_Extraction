import re
import requests
from bs4 import BeautifulSoup
from model import FlightInformationRegion,AiracData,ControlAirspace,Controlled,AirportData, session
from sqlalchemy import func
from url_extraction import (
    find_eaip_url,
    fetch_and_parse_frameset,
    fetch_and_parse_navigation_frame,
    search_and_print_controlled_links
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
        
 def find_and_process(self, tables, heading, airspace_type):
    process_id = get_active_process_id()
    for i, t in enumerate(tables):
        if t.p.get_text(strip=True) == heading:
            table = tables[i + 1]
            break

    tr_list = table.find_all("tr")[1:]
    main_info_td, remarks_td = None, None
    airspace = None
    for tr in tr_list:
        td_list = tr.find_all("td")
        if len(td_list) == 5:
            # For an airspace only the first row(which has length 5) has data in first and last column of the source table in UI
            main_info_td = td_list.pop(0)
            remarks_td = td_list.pop(-1)
            # print(remarks_td)
            data_list = []
            p_list = main_info_td.find_all("p")
            # print(p_list[-1])
            airspace_class = ""
            i = len(p_list) - 1
            while True:
                p_text = p_list[i].get_text(strip=True)
                if p_text:
                    if re.match(r"[A-Z]$", p_text):
                        airspace_class = p_text
                        
                        p_list.pop(i)
                    break
                i -= 1
            s = ""
            for p in p_list:
                p_text = p.get_text(strip=True)
                if p_text == "":
                    if s != "":
                        data_list.append(s.strip(" \n"))
                        s = ""
                    continue
                s += p_text + "\n"
            if s != "":
                data_list.append(s.strip(" \n"))
            if airspace_class:
                data_list.append("")
            while len(data_list) < 4:
                data_list.append("")

            if airspace_class:
                print("")
            else:
                i = -1
                data = data_list[i]
                if data:
                    data_list[i] = ""

            i = -2
            vertical_limits = None
            data = data_list[-2]
            
            if data and data.find("/") > -1 and data.find("\n") == -1:
                x = r"FL\s*\d+|\d+\s*FT\s*AMSL"
                re_str = r"(?P<ul>UNL|" + x + r")\s*/\s*(?P<ll>GND|" + x + r")"
                
                if m := re.search(re_str, data):
                    vertical_limits = data
                   
                    data = data[: m.start()] + data[m.end() :]
                    data = data.strip()
                    data_list[i] = data
                    

            remark = remarks_td.get_text().strip(" \n").replace("\r", "")


            unit_td = td_list[0]
            unit_providing_service = unit_td.get_text(strip=True)
            
            call_sign_td = td_list[1]
            p_list = call_sign_td.find_all("p")
            call_sign = ""
            language = ""
            area = ""

            for i, p_tag in enumerate(p_list):
             text = p_tag.get_text(strip=True)

             if "English" in text:
                language = text
                if i + 1 < len(p_list):
                    area = p_list[i + 1].get_text(strip=True)
                break
             call_sign += text + " "
        
        frequencies = ", ".join([p.get_text(strip=True) for p in td_list[2].find_all("p")])
        # Query for matching controlled airspace entry
        controlled_airspace_entry = session.query(Controlled).filter(
            (func.lower(Controlled.name) == func.lower(data_list[0])) 
            ).first()
        controlled_airspace_entry = session.query(Controlled).filter(Controlled.name == data_list[0]).first()
        # print(controlled_airspace_entry)
        
        if airspace_type == "FIR":
                airspace_entry = FlightInformationRegion(
                        fir_name=data_list[0],
                        lateral_limits=f"{data_list[1]} {data_list[2]}",
                        vertical_limits=vertical_limits,
                        unit_process_service=unit_providing_service,
                        call_sign=call_sign,
                        language=language,
                        area=area,
                        frequency=frequencies,
                        remarks=remark,
                        process_id=process_id
                    )
        else:
            # print(controlled_airspace_entry)
            # lateral_limits=f"{data_list}"
        
            # # text = lateral_limits.get_text(strip=True) 
        
            # print(lateral_limits)
            
            # Check if city name matches any entry in airportData table
            city_name = data_list[0].split()[0]  # Extract first string as city name
            airport_record = session.query(AirportData).filter(AirportData.city.ilike(f"%{city_name}%")).first()

            if airport_record:
                icao_code = airport_record.ICAOCode
                print(f"Matching city found: {city_name}")
            else:
                icao_code = None
                print(f"No matching city found for: {city_name}")
            if controlled_airspace_entry:                        
                        geom = controlled_airspace_entry.geom
                        geometry = controlled_airspace_entry.geometry
                        airspace_entry = ControlAirspace(
                        name=data_list[0],
                        icao_code=icao_code if icao_code else '',  # Default to 'NON-E'
                        type=airspace_type,
                        AirspaceClassification = airspace_class,
                        lateral_limits=f"{data_list[1]} {data_list[2]}",
                        vertical_limits=vertical_limits,
                        unit_process_service=unit_providing_service,
                        call_sign=call_sign,
                        language=language,
                        area=area,
                        frequency=frequencies,
                        geom=geometry,
                        geometry=geom,
                        remarks=remark,
                        process_id=process_id
                    )
            session.add(airspace_entry)
            session.commit()

    

 def extract_and_insert_2_1(self):
        response = requests.get(self.aip_base_url, verify=False)
        soup = BeautifulSoup(response.text, "html.parser")
        for del_tag in soup.find_all("del"):
            del_tag.decompose()
        for hidden_tag in soup.find_all(class_="AmdtDeletedAIRAC"):
            hidden_tag.decompose()

        tables = soup.find_all("table")
        self.find_and_process(tables, "2.1.1 Flight Information Region", "FIR")
        self.find_and_process(tables, "2.1.2 Control Area", "CTA")
        self.find_and_process(tables, "2.1.3 Terminal Control Area", "TMA")
        self.find_and_process(tables, "2.1.4 Military Control Zones", "MTR")


# def main():
#     processed_urls_file = "controlled_processed_urls.txt"
#     eaip_url = find_eaip_url()
#     if eaip_url:
#         base_frame_url = fetch_and_parse_frameset(eaip_url)
#         if base_frame_url:
#             navigation_url = fetch_and_parse_navigation_frame(base_frame_url)
#             if navigation_url:
#                 enr_2_1_urls = search_and_print_controlled_links(navigation_url, processed_urls_file)
#                 # Select a valid URL from the list
#                 if enr_2_1_urls:
#                     extractor = AirspaceExtractor(enr_2_1_urls[0])  # Pick first URL
#                     extractor.extract_and_insert_2_1()
#                 else:
#                     print("No valid URLs found for ENR 2.1 processing.")


def main():
    aip_base_url = 'https://aim-india.aai.aero/eaip-v2-07-2024/eAIP/IN-ENR%202.1-en-GB.html'
    extractor = AirspaceExtractor(aip_base_url)
    extractor.extract_and_insert_2_1()

if __name__ == "__main__":
    main()
