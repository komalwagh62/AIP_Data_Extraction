from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select

##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re

AIRPORT_ICAO = "VEPT"
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
    # Map for direction multipliers
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    
    # Extract direction (last character)
    new_dir = coord[-1]  # 'N', 'S', 'E', 'W'
    if new_dir not in direction:
        raise ValueError("Invalid direction in coordinate")

    # Remove direction for parsing numeric parts
    coord = coord[:-1]

    # Match DMS format: e.g., '25°28’30.61ʺ'
    match = re.match(r"(\d+)°(\d+)’([\d.]+)ʺ", coord)
    if not match:
        raise ValueError("Invalid coordinate format")

    # Extract DMS parts
    degrees, minutes, seconds = map(float, match.groups())

    # Calculate decimal degrees
    decimal_degrees = degrees + (minutes / 60) + (seconds / 3600)

    # Apply direction
    return decimal_degrees * direction[new_dir]


def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True


def extract_insert_apch(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        waypoint_df = waypoint_df.drop(index=[0, 1,2])
        for _, row in waypoint_df.iterrows():
            row = list(row)
            if len(row) < 3:
            #  print(row)
             row = [x for x in row if x.strip()]
             # Extract coordinates using regex
             extracted_data1 = re.findall(
    r"(\d+°\d+’\d+\.\d+ʺ)([NS])\s+(\d+°\d+’\d+\.\d+ʺ)\s*([EW])", 
    re.sub(r'\s+', ' ', row[1])  # Normalize spaces
)

             lat_value1, lat_dir1, lng_value1, lng_dir1 = extracted_data1[0]  # Access the first tuple


             # Convert DMS to Decimal Degrees
             lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
             lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
             
             coordinates = f"{lat1} {lng1}"
             session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    # type=row[1].strip(),
                    coordinates_dd = coordinates,
                    # navaid=row[0].strip(),
                    geom=f"POINT({lng1} {lat1})",
                    process_id=process_id
                )
             )
            else:
                row = [x for x in row if x.strip()]
                # print(row)
    
                # Extract coordinates using regex
                extracted_data1 = re.findall(
    r"([NS])\s*(\d+):(\d+):(\d+\.\d+)\s+([EW])\s*(\d+):(\d+):(\d+\.\d+)", 
    re.sub(r'\s+', ' ', row[3])  # Normalize spaces
)


                # print(extracted_data1)
                
                # Extract values
                lat_dir1, lat_deg1, lat_min1, lat_sec1, lng_dir1, lng_deg1, lng_min1, lng_sec1 = extracted_data1[0]

    # Combine DMS for conversion
                lat_dms = f"{lat_deg1}°{lat_min1}’{lat_sec1}ʺ{lat_dir1}"
                lng_dms = f"{lng_deg1}°{lng_min1}’{lng_sec1}ʺ{lng_dir1}"

    # Convert DMS to Decimal Degrees
                lat1 = conversionDMStoDD(lat_dms)
                lng1 = conversionDMStoDD(lng_dms)


                coordinates = f"{lat1} {lng1}"
                session.add(
                     Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=row[2].strip(),
                type=row[1].strip(),
                coordinates_dd=coordinates,
                navaid=row[0].strip(),
                geom=f"POINT({lng1} {lat1})",
                process_id=process_id
            )
        )
    

    coding_df = tables[0].df
    # Print the first row
    # print(coding_df.iloc[0])  # Access the first row using index 0

    # coding_df = coding_df.drop(0)
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
    header_row = coding_df.iloc[0]
    print(len(header_row))
    if len(header_row) == 11:
    
     for _, row in coding_df.iterrows():
        
        row = list(row)
        if row[-1].strip() == 'RNP APCH':
        #   print(row)
          waypoint_obj = None
          if is_valid_data(row[2]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                .first()
            )
          course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace("Mag", "").replace("True", "")
          angles = course_angle.split()
         # Check if we have exactly two angle values
          if len(angles) == 2:
            if not (angles[1].startswith("(") and angles[1].endswith(")")):
        # Add parentheses around the second angle
                course_angle = f"{angles[0]}({angles[1]})"
            else:
        # Keep the original if the second angle already has parentheses
                course_angle = f"{angles[0]}{angles[1]}"

        
          # Create ProcedureDescription instance
          proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number = sequence_number,
            seq_num=int(row[0]),
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=course_angle,
            turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[5].strip() if is_valid_data(row[5]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            # role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
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
            data_parts = row[0].split(" \n")
            if len(data_parts) >=3:
                if data_parts[-1].endswith('°)'):
                    insertData = data_parts[5] +""+data_parts[-1]
                    data_parts.insert(4, insertData)
                    data_parts.pop(-1)
                    data_parts.pop(6)
                    data_parts.insert(-1, data_parts[3])
                    data_parts.pop(3)
                    data_parts.insert(2, data_parts[-1])
                    data_parts.pop(-1)
                    data_parts.insert(3, data_parts[5])
                    data_parts.pop(6)
                    data_parts.insert(-3, data_parts[4])
                    data_parts.pop(4)
                else:
                    data_parts.insert(2, data_parts[-1])
                    data_parts.pop(-1)
                    data_parts.insert(-1, data_parts[4])
                    # data_parts.pop(-1)
                    data_parts.pop(4)
                    data_parts.insert(-2, data_parts[3])
                    # # data_parts.pop(11)
                    
                    
                    # data_parts.insert(-2, data_parts[4])
                    data_parts.pop(3)
                    data_parts.insert(-2, data_parts[3])
                    data_parts.pop(3)
                    data_parts.insert(-2, data_parts[-1])
                    data_parts.pop(-1)
                    # print(data_parts)
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
                        if not (angles[1].startswith("(") and angles[1].endswith(")")):
        # Add parentheses around the second angle
                            course_angle = f"{angles[0]}({angles[1]})"
                        else:
        # Keep the original if the second angle already has parentheses
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
    
    elif len(header_row) == 12:
    
     for _, row in coding_df[2:].iterrows():
          row = list(row)
          print(row)
        
          print(row,"rf")
          waypoint_obj = None
          if is_valid_data(row[2]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[2].strip())
                .first()
            )
          course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace("Mag", "").replace("True", "")

          # Use regex to check and format angles if found in "338.67°338.42°" pattern
          course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', course_angle)

# Split the angles if they are separated by space
          angles = course_angle.split()

# Check if we have exactly two angle values separated by space
          if len(angles) == 2:
            course_angle = f"{angles[0]}({angles[1]})"

            print(course_angle)

          # Create ProcedureDescription instance
          proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number = sequence_number,
            seq_num=(row[0]),
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=course_angle,
            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
            process_id=process_id
        )
          print(proc_des_obj)
          session.add(proc_des_obj)
          if is_valid_data(data := row[1]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
          sequence_number += 1
    #         # print(row)
                


def main():
    file_names = os.listdir(FOLDER_PATH)
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)

    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)

    session.commit()

    print("Data insertion complete.")


if __name__ == "__main__":
    main()
