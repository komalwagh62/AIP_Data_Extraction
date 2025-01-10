import camelot
import re
import os
import fitz  # PyMuPDF
from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription,TerminalHolding
##################
# EXTRACTOR CODE #
##################

AIRPORT_ICAO = "VABP"
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
        process_id = process_id
    )
    session.add(procedure_obj)
    sequence_number = 1
    for _, row in coding_df.iloc[1:].iterrows():
        row = list(row)
        if bool(row[-1].strip()):
         waypoint_obj = None
         if is_valid_data(row[2]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                .first()
            )
         course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / "," ")
         angles = course_angle.split()
         if len(angles) == 2:  # Ensure there are two angle values
                            course_angle = f"{angles[0]}({angles[1]})"


         # Create ProcedureDescription instance
         proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            
            seq_num=(row[0]),
            sequence_number = sequence_number,
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
         session.add(proc_des_obj)
         if is_valid_data(data := row[3]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
         sequence_number += 1
        else:
            data_parts = row[0].split("\n")
            if data_parts[-1].endswith(" True"):
             data_to_insert = data_parts[0] + "" + data_parts[-1]
             data_parts.insert(4, data_to_insert)
             data_parts.pop(0)
             data_parts.pop(-1)
             data_parts[3],data_parts[4] = data_parts[4],data_parts[3]
             data_parts.insert(4, data_parts[5])
             data_parts.pop(6)
             data_parts.pop(0)
            else:
                # Concatenate data_parts[0] and data_parts[-1]
                data_to_insert = data_parts[0].strip() + " " + data_parts[-1].strip()

                # Insert data_to_insert at the 7th position
                data_parts.insert(7, data_to_insert)

                # Remove the 0th and last indexes
                data_parts.pop(0)  # Remove 0th index
                data_parts.pop(-1)  # Remove last index

                # Ensure data_parts has enough elements before proceeding
                if len(data_parts) >= 8:  # Check length after removals
                    # print(data_parts)
                    # Insert elements at required positions
                    # data_parts.pop(0)  # Remove 0th index
                    data_parts.insert(4, data_parts[-4])  # Insert 6th at 3rd position
                    data_parts.insert(5, data_parts[-5])  # Insert 7th at 4th position

                    # # Remove the old positions (adjusted after inserts)
                    data_parts.pop(-4)  # Remove the old 7th
                    data_parts.pop(-4)  # Remove the old 6th
                    data_parts.pop(0)  # Remove 0th index
                else:
                    print(f"Skipping row due to insufficient length: {data_parts}")
                    continue  # Skip invalid rows

                    # Print the final processed parts for debugging
            waypoint_name = data_parts[2].replace(" ","")
            if is_valid_data(waypoint_name):
                print(waypoint_name,"de")
                print(f"AIRPORT_ICAO: '{AIRPORT_ICAO}', waypoint_name: '{waypoint_name}'")

                waypoint_obj = (
                                session.query(Waypoint)
                                .filter_by(
                                    airport_icao=AIRPORT_ICAO,
                                    name=waypoint_name,
                                )
                                .first()
                            )
                print(waypoint_obj,"Dr")
                    
            course_angle = data_parts[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / "," ")
            angles = course_angle.split()
            if len(angles) == 2:  # Ensure there are two angle values
                course_angle = f"{angles[0]}({angles[1]})"
            print(waypoint_obj)
            proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    sequence_number=sequence_number,
                    seq_num=data_parts[0].strip(),
                    waypoint=waypoint_obj,
                    path_descriptor=data_parts[1].strip(),
                    course_angle=course_angle,
                    turn_dir=data_parts[6].strip()
                    if is_valid_data(data_parts[6])
                    else None,
                    altitude_ll=data_parts[7].strip()
                    if is_valid_data(data_parts[7])
                    else None,
                    speed_limit=data_parts[8].strip()
                    if is_valid_data(data_parts[8])
                    else None,
                    dst_time=data_parts[5].strip()
                    if is_valid_data(data_parts[5])
                    else None,
                    vpa_tch=data_parts[9].strip()
                    if is_valid_data(data_parts[9])
                    else None,
                    nav_spec=data_parts[10].strip()
                    if is_valid_data(data_parts[10])
                    else None,
                    process_id=process_id
                )
                
            session.add(proc_des_obj)
            if is_valid_data(data := data_parts[3]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1
        session.commit()


                
                # print(data_parts)
             

           



def main():
    file_names = os.listdir(FOLDER_PATH)
    waypoint_file_names = []
    apch_coding_file_names = []

    for file_name in file_names:
        if file_name.find("CODING") > -1:
         if file_name.find("RNP") > -1:
            waypoint_file_names.append(file_name)
            apch_coding_file_names.append(file_name)

    for waypoint_file_name in waypoint_file_names:
        process_id = get_active_process_id()
        with open(FOLDER_PATH + waypoint_file_name, "rb") as f:
            pdf = fitz.open(f)
            if len(pdf) >= 1:
                # if re.search(r"WAYPOINT INFORMATION", pdf[0], re.I):
                    df = camelot.read_pdf(
                        FOLDER_PATH + waypoint_file_name,
                        pages="all",  # str(table_index + 1),  # Page numbers start from 1
                    )[1].df
                    
                    df = df.drop([0])
                        
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
                        extracted_data1 = [
                            item
                            for match in re.findall(
                                r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[1]
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
                                coordinates_dd = coordinates,
                                geom=f"POINT({lng1} {lat1})",
                                process_id=process_id
                            )
                        )
    # session.commit()
    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)

    
    print("Data insertion complete.")


if __name__ == "__main__":
    main()
