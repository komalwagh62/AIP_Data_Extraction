from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, AiracData, session
from sqlalchemy import select
import camelot
import os
import re
import fitz  # PyMuPDF

AIRPORT_ICAO = "VOCB"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"
    
def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    new_dir = coord[-1]
    coord = coord[:-1]
    parts = re.split(r"[:.]", coord)
    print(f"Parsing DMS: {coord}, Direction: {new_dir}, Parts: {parts}")
    if len(parts) == 3:
        HH, MM, SS = map(float, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(float, parts)
    else:
        raise ValueError(f"Invalid coordinate format: {coord}")
           
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    print(f"Converted to Decimal Degrees: {decimal_degrees}")
    return decimal_degrees

def get_active_process_id():
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id
    else:
        print("No active AIRAC record found.")
        return None
           
def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True

def format_coordinates(raw_coordinate):
    match = re.search(r"(\d+)°(\d+)'([\d.]+)''([NS])\s+(\d+)°(\d+)'([\d.]+)''([EW])", raw_coordinate)
    if match:
        lat_deg, lat_min, lat_sec, lat_dir, lon_deg, lon_min, lon_sec, lon_dir = match.groups()
        return f"{lat_dir} {lat_deg}:{lat_min}:{lat_sec}{lon_dir} {lon_deg}:{lon_min}:{lon_sec}"
    else:
        raise ValueError(f"Invalid coordinate format: {raw_coordinate}")

def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    waypoint_tables = tables[1:]

    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df.drop(index=[0])
        for _, row in waypoint_df.iterrows():
            row = [x for x in row if x.strip()]  # Remove empty values
            if len(row) < 2:
                continue
            
            waypoint_name = row[0].strip()
            existing_waypoint = session.execute(
                select(Waypoint).where(Waypoint.airport_icao == AIRPORT_ICAO, Waypoint.name == waypoint_name)
            ).fetchone()
            if existing_waypoint:
                continue

            try:
                formatted_coords = format_coordinates(row[1])
                lat_dir, lat_value, lng_dir, lng_value = re.findall(r"([NS]) (\d+:\d+:\d+\.\d+)([EW]) (\d+:\d+:\d+\.\d+)", formatted_coords)[0]

                lat_dd = conversionDMStoDD(lat_value + lat_dir)
                lng_dd = conversionDMStoDD(lng_value + lng_dir)
                coordinates = f"{lat_dd} {lng_dd}"

                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        name=waypoint_name,
                        coordinates_dd=coordinates,
                        geom=f"POINT({lng_dd} {lat_dd})",
                        process_id=process_id
                    )
                )
            except ValueError as e:
                print(f"Skipping waypoint {waypoint_name}: {e}")

    coding_df = tables[0].df.drop(index=[0,1,2])
    procedure_name = re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")

    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
        process_id=process_id
    )
    session.add(procedure_obj)

    sequence_number = 1
    for _, row in coding_df.iterrows():
        row = list(row)
        waypoint_obj = None

        if bool(row[-1].strip()) and is_valid_data(row[2]):
            waypoint_name = row[2].strip().replace("\n", "").replace(" ", "")
            waypoint_obj = session.query(Waypoint).filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name).first()

        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=row[4].replace("\n", "").replace(" ", ""),
            turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time="  ".join(row[5].strip().split()) if is_valid_data(row[5]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
            process_id=process_id
        )

        if is_valid_data(data := row[3]):
            proc_des_obj.fly_over = True if data == "Y" else False

        session.add(proc_des_obj)
        sequence_number += 1    
                                                                                       
                                                                          
def main():
    file_names = os.listdir(FOLDER_PATH)
    apch_coding_file_names = []

    for file_name in file_names:
        if file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)
    
    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all", flavor="lattice")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)
 
    session.commit()
    print("Data insertion complete.")
                                                      
                                                
if __name__ == "__main__":
    main()
    
         
   
                                     
                                        
                                        
                                        
                                        
                                        
                                        
                                        
                                        