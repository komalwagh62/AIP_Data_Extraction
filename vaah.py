from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select


##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re
import fitz  # PyMuPDF

AIRPORT_ICAO = "VAAH"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    # Extract direction (N/S/E/W)
    new_dir = coord[-1]
    coord = coord[:-1]
    # Split degrees, minutes, and seconds parts
    parts = re.split(r"[:.]", coord)
    # Handle different number of parts
    if len(parts) == 3:
        HH, MM, SS = map(float, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(float, parts)
    else:
        raise ValueError("Invalid coordinate format")
    # Calculate decimal degrees
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    return decimal_degrees

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None
    
def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    coding_df = tables[0].df
    coding_df = coding_df.drop(0)  # Drop the first row if it's a header or irrelevant data
    # print(coding_df)
    # Extract the procedure name from the file name
    match = re.search(r"(RNP.+)-TABLE", file_name) or re.search(r"(RNP.+)-CODING", file_name)
    if not match:
        raise ValueError(f"Unable to extract procedure name from file: {file_name}")
    
    procedure_name = match.groups()[0].replace("-", " ")
    
    # Create a Procedure object and add to the session
    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
        process_id=process_id
    )
    session.add(procedure_obj)
    # print(f"Added procedure: {procedure_obj}")
    
    # Initialize sequence number tracker
    sequence_number = 1
    
    # Iterate over the rows of the DataFrame
    for _, row in coding_df.iterrows():
        row = list(row)
        # print(row)
        
        
        # Fetch Waypoint object if valid data is present
        waypoint_obj = None
        if row[-1].strip() == 'RNP APCH':
         if is_valid_data(row[2]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                .first()
            )
        
         # Process the course angle format
         course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / ", "")
         angles = course_angle.split()
         if len(angles) == 2:  # Ensure there are two angle values
            course_angle = f"{angles[0]}{angles[1]}"
        
         # Create a ProcedureDescription object
         proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,  # Assign sequence number based on iteration
            seq_num=int(row[0]),
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
         # print(f"Added procedure description: {proc_des_obj}")
        
         # Handle the 'fly_over' field based on data
         if is_valid_data(data := row[3]):
            proc_des_obj.fly_over = data == "Y"
        
         # Increment sequence number for the next description
         sequence_number += 1
        
        else:
                        data_parts = row[0].split(" \n")
                        # print(data_parts)
                # # print(data_parts)
                # if data_parts[0].isdigit() or data_parts[0].endswith("Mag") or data_parts[-1] == 'RNP APCH':
                        if len(data_parts) > 2:  # Check if it's not an empty string
    
                         if data_parts[-3] == 'RNP APCH':  # Check if it's a float
                            data_parts.insert(-3, data_parts[-1])
                            data_parts.insert(2, data_parts[-2])
                            data_parts.pop(-1)
                            data_parts.pop(-1)
                        #  data_parts.pop(-2)
                        #  data_parts.insert(-2, data_parts[-4])
                        #  data_parts.pop(4)
                            data_parts.insert(-3, (data_parts[0] +" "+ data_parts[-2]))
                            data_parts.pop(0)
                            data_parts.pop(-2)
                        #  print(data_parts)
                            if data_parts[0] == '-':
                                data_parts[0], data_parts[2] = data_parts[2], data_parts[0]
                            if data_parts[2] == '-':
                                data_parts[2], data_parts[3] = data_parts[3], data_parts[2]
                            data_parts[1], data_parts[2] = data_parts[2], data_parts[1]
                            data_parts[-3], data_parts[-4] = data_parts[-4], data_parts[-3]
                         else:
                            data_to_insert = data_parts[0] + " " + data_parts[-1]
                            data_parts.insert(4, data_to_insert)
                            data_parts.pop(0)
                            data_parts.pop(-1)
                            if data_parts[-1] == 'RNP APCH':
                                data_parts[0], data_parts[2] = data_parts[2], data_parts[0]
                                data_parts[4], data_parts[1] = data_parts[1], data_parts[4]
                                data_parts.insert(3, data_parts[-2])
                                data_parts.pop(-2)
                                data_parts.insert(5, data_parts[-4])
                                data_parts.pop(-4)
                                data_parts.insert(3, data_parts[-4])
                                # data_parts.pop(-4)
                                data_parts.insert(4, data_parts[-2])
                                data_parts.insert(-5, data_parts[-3])
                                data_parts.pop(-3)
                                data_parts.pop(-3)
                                # data_parts.insert(5, data_parts[-2])
                                # data_parts.pop(4)
                                data_parts.insert(4, data_parts[-2])
                                data_parts.pop(4)
                                data_parts.pop(4)
                                data_parts.insert(-1, data_parts[4])
                                data_parts.pop(4)
                                # data_parts.insert(3, "-")
                                # print(data_parts)
                            else:
                                data_parts.insert(2, data_parts[-2])
                                data_parts.pop(-2)
                                data_parts.insert(-4, data_parts[-1])
                                data_parts.pop(-1)
                                if data_parts[0] == '-':
                                 data_parts.insert(0, data_parts[3])
                                 data_parts.pop(4)
                                 data_parts.insert(1, data_parts[5])
                                 data_parts.pop(6)
                                 data_parts.insert(2, data_parts[4])
                                 data_parts.pop(5)
                                 data_parts[4], data_parts[5] = data_parts[5], data_parts[4]
                                else:
                                 data_parts.insert(5, data_parts[0])
                                 data_parts.pop(0)
                                 data_parts[1], data_parts[2] = data_parts[2], data_parts[1]
                                 data_parts.pop(-6)
                                 data_parts.insert(3, "-")
                        
                         waypoint_name = data_parts[2]
                         if is_valid_data(waypoint_name):
                            waypoint_obj = (
                                session.query(Waypoint)
                                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                                .first()
                            )
                        #  print(data_parts[4])
                         course_angle = data_parts[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "")
                        #  print(course_angle)
                         angles = course_angle.split()
                         if len(angles) == 2:  # Ensure there are two angle values
                            course_angle = f"{angles[0]}{angles[1]}"
                            # print(course_angle,"wdef")
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
                

                            
                        
                    
                    
    
    # # Commit the session to save all changes
    # session.commit()
    # print("All data has been successfully inserted into the database.")



def main():
    file_names = os.listdir(FOLDER_PATH)
    waypoint_file_names = []
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("TABLE") > -1: 
            if file_name.find("RNP") > -1:
                waypoint_file_names.append(file_name)
                apch_coding_file_names.append(file_name)
           

    for waypoint_file_name in waypoint_file_names:
        process_id = get_active_process_id()
        with open(FOLDER_PATH + waypoint_file_name, "rb") as f:
            pdf = fitz.open(f)

            # pdf = pdftotext.PDF(f)
            if len(pdf) >= 1:
                # if re.search(r"WAYPOINT INFORMATION", pdf[0], re.I):
                    waypoint_df = camelot.read_pdf(
                        FOLDER_PATH + waypoint_file_name,
                        pages="all",  # str(table_index + 1),  # Page numbers start from 1
                    )[1].df
                    # print(df)
    #                 if re.search(r"WAYPOINT INFORMATION-", str(df[1]), re.I):
                    waypoint_df = waypoint_df.drop(index=[0, 1,2])
                    
                    for _, row in waypoint_df.iterrows():
                        row = list(row)
                        # print(row)
                        row = [x for x in row if x.strip()]
                        if len(row) < 2:
                            continue
                       
                        lat, long = row[1].split("   ") or row[1].split("    ")
                        lat1 = conversionDMStoDD(lat)
                        lng1 = conversionDMStoDD(long)
                        coordinates = f"{lat1} {lng1}"
                        session.add(
                            Waypoint(
                            airport_icao=AIRPORT_ICAO,
                            name=row[0].strip(),
                            coordinates_dd = coordinates,
                            geom=f"POINT({lng1} {lat1})",
                            process_id = process_id
                        )
                     )
    
    
    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)
     
    
    session.commit()
    # print("Data insertion complete.")



if __name__ == "__main__":
    main()