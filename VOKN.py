from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select
##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re

AIRPORT_ICAO = "VOKN"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    # Extract direction (N/S/E/W)
    new_dir = coord[-1]
    coord = coord[:-1]
    # Split degrees, minutes, and seconds parts
    parts = re.split(r"[:.]", coord)
    # Handle different number of parts
    if len(parts) == 3:
        HH, MM, SS = map(int, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(int, parts)
    else:
        raise ValueError("Invalid coordinate format")
    # Calculate decimal degrees
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    return decimal_degrees


def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    coding_df = tables[0].df
    coding_df = coding_df.drop(0)
    # print(coding_df)
    procedure_name = (
        re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
    )
    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
        process_id=process_id
    )
    session.add(procedure_obj)
    # Initialize sequence number tracker
    sequence_number = 1
    for _, row in coding_df.iterrows():
         row = list(row)
        
         waypoint_obj = None
         if is_valid_data(row[1]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[1].strip())
                .first()
            )
            
         
         course_angle = row[4].replace("\n", "").replace(" ", " ").replace(" )", ")").replace(" N/A", "").replace("N/A", "")
         angles = course_angle.split()
 
            # Check if we have exactly two angle values
         if len(angles) == 2:
            course_angle = f"{angles[0]}({angles[1]})"
         print(course_angle)
        # Create ProcedureDescription instance
         proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,
            seq_num=int(row[0]),
            
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=course_angle,
            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
            process_id=process_id
        )
         print(proc_des_obj,"dwe")
         session.add(proc_des_obj)
         if is_valid_data(data := row[2]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
         sequence_number += 1
    session.commit()




def main():
    file_names = os.listdir(FOLDER_PATH)
    waypoint_file_names = []
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("CODING") > -1: 
            if file_name.find("RNP") > -1:
                waypoint_file_names.append(file_name)
              
                apch_coding_file_names.append(file_name)
           

    process_id = get_active_process_id()
    for waypoint_file_name in waypoint_file_names:
        
                
                    df = camelot.read_pdf(
                        FOLDER_PATH + waypoint_file_name,
                        pages="all",  # str(table_index + 1),  # Page numbers start from 1
                    )[0].df
                    # print(df)
                    header_row = df.iloc[0].tolist()
                    # print(header_row)
                    if 'Waypoint \nType \nLatitude/Longitude (WGS84) \nIdentifier \n(DD:MM:SS.SS)' in header_row:
                        for _, row in df.iterrows():
                         row = list(row)
                         print(row)
                         row = [x for x in row if x.strip()]
                         if len(row) < 3:
                            continue
                         result_row = session.execute(
                            select(Waypoint).where(
                                Waypoint.airport_icao == AIRPORT_ICAO,
                                Waypoint.name == row[0].strip(),
                            )
                         ).fetchone()
                        
                         if result_row:
                            continue
                         extracted_data1 = [
                            item
                            for match in re.findall(
                                r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[2]
                            )
                            for item in match
                        ]
                         lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
                         lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                         lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                         coordinates = f"{lat1} {lng1}"
                         session.add(
                            Waypoint(
                                airport_icao=AIRPORT_ICAO,
                                name=row[0].strip(),
                                type=row[1].strip(),
                                coordinates_dd = coordinates,
                                geom=f"POINT({lng1} {lat1})",
                                process_id=process_id
                            )
                        )
    
    
    
    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)
     
    session.commit()
    
    print("Data insertion complete.")



if __name__ == "__main__":
    main()