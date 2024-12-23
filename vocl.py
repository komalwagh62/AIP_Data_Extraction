from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select
from pyproj import Proj, transform

utm_proj = Proj(proj='utm', zone=43, ellps='WGS84', south=False)  # south=False for northern hemisphere
wgs84_proj = Proj(proj='latlong', datum='WGS84')  # Latitude/Longitude

##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None

AIRPORT_ICAO = "VOCL"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}

    # Remove non-numeric characters
    coord_numeric = ''.join(char for char in coord if char.isdigit() or char == '.')

    # Check if the coordinate is in decimal format
    try:
        decimal_degrees = float(coord_numeric)
        return decimal_degrees
    except ValueError:
        print(f"Processing coordinate: {coord}")

    # Extract direction (N/S/E/W)
    new_dir = coord[-1]
    
    # Split degrees, minutes, and seconds parts
    parts = re.split(r"[:.]", coord_numeric)
    
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
    # print(coding_df)
    coding_df = coding_df.drop(0)
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
        # if not row[0].strip().isdigit():
        #   continue
        waypoint_obj = None
        if is_valid_data(row[2]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                .first()
            )
       
        # Create ProcedureDescription instance
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number = sequence_number,
            seq_num=int(row[0]),
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=row[4].replace("\n", "").replace(" ", "").replace(" )", ")"),
            turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[5].strip() if is_valid_data(row[5]) else None,
            vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
            role_of_the_fix =row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
            process_id=process_id
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[3]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
        sequence_number += 1



def main():
    file_names = os.listdir(FOLDER_PATH)

    apch_coding_file_names = [
        file_name
        for file_name in file_names
        if "CODING" in file_name and "RNP" in file_name
    ]
    waypoint_file_names = [
        file_name
        for file_name in file_names
        if "WAYPOINTS" in file_name and "RNP" in file_name
    ]
    # print(waypoint_file_names)


    process_id = get_active_process_id()
    for waypoint_file_name in waypoint_file_names:
                    
                    df = camelot.read_pdf(
                        FOLDER_PATH + waypoint_file_name,
                        pages="all",  # str(table_index + 1),  # Page numbers start from 1
                    )[0].df
                    # print(df)
    #                 if re.search(r"WAYPOINT INFORMATION-", str(df[1]), re.I):
                    df = df.drop(0)
                    # print(df)
                    for _, row in df.iterrows():
                        row = list(row)
                        row = [x for x in row if x.strip()]
                        if len(row) < 2:
                            continue
                        result_row = session.execute(
                            select(Waypoint).where(
                                Waypoint.airport_icao == AIRPORT_ICAO,
                                Waypoint.name == row[0].strip(),
                            )
                        ).fetchone()
                        if result_row:
                            continue
                        
                        lat1 = conversionDMStoDD(row[2])
                       
                        lng1 = conversionDMStoDD(row[3])
                        
                        longitude, latitude = transform(utm_proj, wgs84_proj, lat1, lng1)
                        coordinates = f"{latitude} {longitude}"
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