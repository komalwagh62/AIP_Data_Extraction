
import pdfplumber
import os
import re
from sqlalchemy import select
from model import Waypoint, Procedure, ProcedureDescription, AiracData, session

AIRPORT_ICAO = "VAKE"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    new_dir = coord[-1]
    coord = coord[:-1]
    parts = re.split(r"[:.]", coord)
    if len(parts) == 3:
        HH, MM, SS = map(float, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(float, parts)
    else:
        raise ValueError("Invalid coordinate format")
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    return decimal_degrees

def get_active_process_id():
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    return active_record.id if active_record else None

def is_valid_data(data):
    return bool(data and not re.match(r"(\s+|\s*-\s*)$", data))

def extract_tables_from_pdf(file_path):
    tables = []
    try:
        with pdfplumber.open(file_path) as pdf:
            print(f"Extracting tables from {file_path}")
            for page in pdf.pages:
                # Extract tables from each page
                tables.extend(page.extract_tables())
            print(f"Extracted {len(tables)} tables from {file_path}")
    except Exception as e:
        print(f"Error extracting tables from {file_path}: {e}")
    return tables

def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    print(f"Extracted tables: {tables}")
    print(len(tables))
    if not process_id:
        print("No active AIRAC record found.")
        return

    waypoint_tables = tables[1:]  # Assuming waypoints are in table[1] onward
    print(f"Processing {len(waypoint_tables)} waypoint tables for {file_name}")
    

    for table in waypoint_tables:
        # Assuming table is a list of rows where each row is a list of columns
        for row in table:
            print(f"Raw row: {row}")  # Debugging line

            # Ensure the row contains only string elements before applying strip
            row = [str(x).strip() if x else "" for x in row]  # Convert to string before strip
            
            if len(row) < 2:
                print("Skipping row due to insufficient data.")
                continue
            
            print(f"Processed row: {row}")  # Debugging line
            existing_waypoint = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0],
                )
            ).fetchone()
            if existing_waypoint:
                continue
            
            extracted_data1 = [
                item
                for match in re.findall(
                    r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]
                )
                for item in match
            ]

            if len(extracted_data1) != 4:
                continue

            lat_dir, lat_value, lng_dir, lng_value = extracted_data1
            lat = conversionDMStoDD(lat_value + lat_dir)
            lng = conversionDMStoDD(lng_value + lng_dir)
            coordinates = f"{lat} {lng}"

            session.add(Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=row[0],
                coordinates_dd=coordinates,
                geom=f"POINT({lng} {lat})",
                process_id=process_id
            ))

    procedure_name_match = re.search(r"(RNP.+)-TABLE", file_name)
    procedure_name = procedure_name_match.groups()[0].replace("-", " ") if procedure_name_match else "Unknown Procedure"

    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
        process_id=process_id
    )
    session.add(procedure_obj)

    coding_df = tables[0][2:]
    print(coding_df)
    sequence_number = 1

    for row in coding_df:
        # Convert to string before checking and stripping
        row = [str(x).strip() if x else "" for x in row]
        if not row or not row[-1].strip():
            continue

        waypoint_obj = None
        if is_valid_data(row[2]):
            waypoint_name = row[2].replace("\n", "").replace(" ", "")
            waypoint_obj = session.query(Waypoint).filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name).first()
        
        course_angle = row[4].replace("\n", "").replace("Mag", "").replace("True", "").replace("/", " ").strip()
        angles = course_angle.split()

        if len(angles) == 2:
            course_angle = f"{angles[0]}({angles[1]})"

        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=course_angle,
            turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[5].strip() if is_valid_data(row[5]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
            process_id=process_id
        )
        if is_valid_data(row[7]):
            altitudes = row[7].strip().split()  # Split on spaces or other delimiters
            proc_des_obj.altitude_ll = "  ".join(altitudes) 

        session.add(proc_des_obj)

        if is_valid_data(row[3]):
            proc_des_obj.fly_over = True if row[3] == "Y" else False
        
        sequence_number += 1

def main():
    file_names = [f for f in os.listdir(FOLDER_PATH) if "TABLE" in f and "RNP" in f]

    for file_name in file_names:
        file_path = os.path.join(FOLDER_PATH, file_name)
        tables = extract_tables_from_pdf(file_path)
        rwy_dir_match = re.search(r"RWY-(\d+[A-Z]?)", file_name)
        rwy_dir = rwy_dir_match.groups()[0] if rwy_dir_match else "Unknown RWY"

        extract_insert_apch(file_name, rwy_dir, tables)

    session.commit()
    print("Data insertion complete.")

if __name__ == "__main__":
    main()
