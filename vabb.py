import camelot
import re
import os

from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription,TerminalHolding
##################
# EXTRACTOR CODE #
##################

AIRPORT_ICAO = "VABB"
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

# Function to get the active process_id from AiracData table
def get_active_process_id():
    # Query the AiracData table for the most recent active record
    active_record = session.query(AiracData).filter(AiracData.status == True).order_by(AiracData.created_At.desc()).first()
    if active_record:
        return active_record.id  # Assuming process_name is the desired process_id
    else:
        print("No active AIRAC record found.")
        return None

def extract_insert_apch1(file_name, tables, rwy_dir):
    process_id = get_active_process_id()
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        header_row = waypoint_df.iloc[0].tolist()
        # print(header_row)
        if len(header_row) > 2:
            waypoint_df = waypoint_df.drop(index=[0])
            for _, row in waypoint_df.iterrows():
                row = list(row)

                row = [x for x in row if x.strip()]

                waypoint_name1 = row[0].strip()
                if len(row) < 3:
                    continue
                result_row = session.execute(
                    select(Waypoint).where(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == row[1].strip(),
                        
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
                # print(extracted_data1)
                lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
                lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                coordinates = f"{lat1} {lng1}"
                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        type=row[0].strip(),
                        name=row[1].strip(),
                        coordinates_dd = coordinates,
                        geom=f"POINT({lng1} {lat1})",
                        process_id = process_id
                    )
                )
        elif len(header_row) == 2:
            waypoint_df = waypoint_df.drop(index=[0, 1])
            for _, row in waypoint_df.iterrows():
                waypoint_name = row[0]
                waypoint_coordinates = row[1]
                row = list(row)
                row = [x for x in row if x.strip()]
                waypoint_name1 = row[0].strip()
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
                        r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", waypoint_coordinates
                    )
                    for item in match
                ]
                lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
                lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
                lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
                coordinates = f"{lat1} {lng1}"
                waypoint = Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=waypoint_name,
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng1} {lat1})",
                    process_id = process_id
                )
                session.add(waypoint)
    
    
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
    coding_df = tables[0].df
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]
    header_row = apch_data_df.iloc[1].tolist()
    sequence_number = 1
    if len(header_row) == 11:
        for _, row in apch_data_df.iloc[2:].iterrows():
            row = list(row)
            waypoint_obj = None
            if bool(row[-1].strip()):
                if is_valid_data(row[2]):
                    waypoint_name = (
                        row[2].strip().strip().replace("\n", "").replace(" ", "")
                    )
                    # print(waypoint_name)
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                        .first()
                    )
                    # print(f"Waypoint name: {waypoint_name}, Waypoint object: {waypoint_obj}")
                course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("N/A","")
                angles = course_angle.split()
                # Check if we have exactly two angle values
                if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
                

                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    sequence_number = sequence_number,
                    seq_num=(row[0]),
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
                # print(proc_des_obj)
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                sequence_number += 1
    else:
        sequence_number  =1
        for _, row in apch_data_df.iloc[2:].iterrows():
            
             
             row = list(row)
             row1 = [""]
             row2 = []
             if len(row) >= 3 and row[-3].strip():
                    
                    row1 = re.split(r'\s{2,}', row[-3])  # This splits by two or more spaces
                    # print("Row1 after split:", row1[-1])  # Print the resulting split list
                    if len(row1) == 2:
                        row.insert(8, row1[0])  
                        row.insert(9, row1[-1]) 
                        row.pop(-3)
                        row.pop(-2)
             if len(row) >= 3 and row[-1].strip():
                    
                    row2 = re.split(r'\s{2,}', row[-1])  # This splits by two or more spaces
                    # print("Row1 after split:", row1[-1])  # Print the resulting split list
                    if len(row2) == 2:
                        
                        row.insert(-2, row2[0]) 
                        row.insert(-1, row2[-1]) 
                        row.pop(-3)
                        # row.pop(-1)
                        # row.insert(9, row1[-1]) 
                        # row.pop(-3)
                        # row.pop(-2)
             waypoint_obj = None
             if bool(row[-1].strip()):
                if is_valid_data(row[2]):
                    waypoint_name = (
                        row[2].strip().strip().replace("\n", "").replace(" ", "")
                    )
                    # print(waypoint_name)
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                        .first()
                    )
                # print(row)
                
                    # data_parts.pop(-3)
                        
                        
                    
                # print(row)
                course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("N/A","")
                angles = course_angle.split()
                # Check if we have exactly two angle values
                if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
                proc_des_obj = ProcedureDescription(
                    procedure=procedure_obj,
                    sequence_number = sequence_number,
                    seq_num=(row[0]),
                    waypoint=waypoint_obj,
                    path_descriptor=row[1].strip(),
                    course_angle=course_angle,
                    turn_dir=row[6].strip() if is_valid_data(row[6]) else None,
                    altitude_ul=row[7].strip() if is_valid_data(row[7]) else None,
                    altitude_ll=row[8].strip() if is_valid_data(row[8]) else None,
                    speed_limit=row[9].strip() if is_valid_data(row[9]) else None,
                    dst_time=row[5].strip() if is_valid_data(row[5]) else None,
                    vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                    nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                    process_id=process_id
                )
                # alt_speed_data = row[9].strip()
                # if alt_speed_data:
                #     alt_speed_values = alt_speed_data.split()
                #     if len(alt_speed_values) == 2:
                #         proc_des_obj.altitude_ll, proc_des_obj.speed_limit = alt_speed_values
                        
                # vpa_tch_nav_spec = row[11].strip()
                # if vpa_tch_nav_spec:
                #     vpa_tch_nav_spec_parts = vpa_tch_nav_spec.split()
                #     if len(vpa_tch_nav_spec_parts) > 1:
                #         last_part = vpa_tch_nav_spec_parts[-1]
                #         if last_part == "APCH" and any(
                #             char.isdigit() for char in vpa_tch_nav_spec_parts[0]
                #         ):
                #             proc_des_obj.vpa_tch = vpa_tch_nav_spec_parts[0]
                #             proc_des_obj.nav_spec = " ".join(vpa_tch_nav_spec_parts[1:])
                session.add(proc_des_obj)
                if is_valid_data(data := row[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                sequence_number += 1
             else:
                data_parts = row[0].split(" \n")
                
                if len(data_parts) >= 13:
                    print(data_parts)
                    if data_parts[0].isdigit() or data_parts[0].endswith("Mag"):
                        # if data_parts[0].isdigit():
                        #     data_parts.insert(7, "")
                        #     data_parts.insert(3, "")
                        #     print(data_parts)
                        if data_parts[0].endswith("Mag"):
                            print(data_parts,"d")
                            data_to_insert = data_parts[0] + " " + data_parts[-1]
                            data_parts.insert(5, data_to_insert)
                            data_parts.pop(0)
                            data_parts.pop(-1)
                            data_parts.pop(0)
                            
                            # data_parts.pop(1)
                            data_parts.insert(3, data_parts[4])
                            data_parts.pop(5)
                            # data_parts.insert(7, data_parts[0])
                            # data_parts.pop(0)
                            data_parts.insert(7, "")
                            # print(data_parts)
                            data_parts.pop(0)
                            data_parts[6],data_parts[7] = data_parts[7],data_parts[6]
                            data_parts[3],data_parts[4] = data_parts[4],data_parts[3]
                            # data_parts.insert(7, data_parts[4])
                            # data_parts.pop(4)
                            # data_parts.insert(3, "")
                    print(data_parts,"rfgt")
                    
                            
                        
                else:
                    data_parts.insert(7, "")  # Insert blank at index 7
                    # print(data_parts)
                    # Initialize data_parts1 with default value
                    data_parts1 = [""]  # Default to an empty list with one blank value
                    
                    # Check if data_parts has enough elements and the 3rd-to-last element is non-empty
                    if len(data_parts) >= 3 and data_parts[-3].strip():  
                            data_parts1 = data_parts[-3].split("  ")
                    # print(data_parts1[-1])
                    data_parts.insert(8, data_parts1[0])  
                    data_parts.insert(9, data_parts1[-1]) 
                    data_parts.pop(-3)
                    
                # print("After Insert:", data_parts)  # Print updated list
                waypoint_name = data_parts[2]
                        # print(waypoint_name)
                if is_valid_data(waypoint_name):
                    waypoint_obj = (
                                session.query(Waypoint)
                                .filter_by(
                                    airport_icao=AIRPORT_ICAO,
                                    name=waypoint_name,
                                )
                                .first()
                            )
                if len(data_parts) > 4:
                #  print(data_parts)
                 course_angle = data_parts[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace("N/A","")
                 angles = course_angle.split()
                        # Check if we have exactly two angle values
                 if len(angles) == 2:
                    course_angle = f"{angles[0]}({angles[1]})"
                        
                 proc_des_obj = ProcedureDescription(
                            procedure=procedure_obj,
                            sequence_number = sequence_number,
                            seq_num=data_parts[0].strip(),
                            waypoint=waypoint_obj,
                            path_descriptor=data_parts[1].strip(),
                            course_angle=course_angle,
                            turn_dir=data_parts[6].strip()
                            if is_valid_data(data_parts[6])
                            else None,
                            altitude_ul=data_parts[7].strip()
                            if is_valid_data(data_parts[7])
                            else None,
                            altitude_ll=data_parts[8].strip()
                            if is_valid_data(data_parts[8])
                            else None,
                            speed_limit=data_parts[9].strip()
                            if is_valid_data(data_parts[9])
                            else None,
                            dst_time=data_parts[5].strip()
                            if is_valid_data(data_parts[5])
                            else None,
                            vpa_tch=data_parts[10].strip()
                            if is_valid_data(data_parts[10])
                            else None,
                            nav_spec=data_parts[11].strip()
                            if is_valid_data(data_parts[11])
                            else None,
                            process_id=process_id
                )
                 session.add(proc_des_obj)
                #  print(proc_des_obj)
                 if is_valid_data(data := data_parts[3]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                 sequence_number += 1


def extract_insert_apch2(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0, 1])
    # print(coding_df)

    # print(apch_data_df)
    if not coding_df.empty and len(coding_df.columns) > 7:
        waypoint_list = coding_df.iloc[
            :, 7
        ].tolist()  # Extract WPT values from second-to-last column
        # print(waypoint_list)
        type_list = coding_df.iloc[:, 3].tolist()
        lat_long_list = coding_df.iloc[
            :, 11
        ].tolist()  # Extract Latitude/Longitude values from seventh column
        # Split the strings using '\n' and filter out empty parts
        waypoint_list = [
            part
            for waypoint in waypoint_list
            for part in waypoint.split("\n")
            if part.strip() != ""
        ]
        # print(waypoint_list)
        type_list = [
            part
            for type in type_list
            for part in type.split("\n")
            if part.strip() != ""
        ]
        # print(type_list)
        lat_long_list = [
            part
            for lat_long in lat_long_list
            for part in lat_long.split("\n")
            if part.strip() != ""
        ]
        # print(lat_long_list)
        # Remove the first index from waypoint_list
        waypoint_list.pop(0)
        # Remove the first two indices from lat_long_list
        lat_long_list = lat_long_list[2:]
        type_list.pop(0)
        # Iterate through both lists and display the data
        for waypoint, type, lat_long in zip(waypoint_list, type_list, lat_long_list):
            # Check if the waypoint exists in the database
            waypoint1 = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint.strip())
                .first()
            )
            lat_dir, lat_value, lng_dir, lng_value = re.findall(
                r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", lat_long
            )[0]
            lat = conversionDMStoDD(lat_value + lat_dir)
            # print(lat)
            lng = conversionDMStoDD(lng_value + lng_dir)
            coordinates = f"{lat} {lng}"
            # If the waypoint doesn't exist, add it to the database
            if not waypoint1:
                new_waypoint = Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=waypoint.strip(),
                    type=type.strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
                )
                session.add(new_waypoint)
                # session.commit()
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
    # print(coding_df)
    apch_data_df = coding_df[coding_df.iloc[:, -3] == "RNP \nAPCH"]
    apch_data_df = apch_data_df.loc[:, (apch_data_df != "").any(axis=0)]
    sequence_number = 1
    for _, row in apch_data_df.iterrows():
        row = list(row)

        if bool(row[-1].strip()):
            #  print(row)
            waypoint_obj = None
            if is_valid_data(row[2]):
                waypoint_name = (
                    row[2].strip().strip().replace("\n", "").replace(" ", "")
                )
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
                # print(f"Waypoint name: {waypoint_name}, Waypoint object: {waypoint_obj}")
            course_angle = row[4].replace("\n", "").replace("  ", " ").replace(" )", ")").replace("N/A", "")
            angles = course_angle.split()

            # Check if we have exactly two angle values
            if len(angles) == 2:
                course_angle = f"{angles[0]}({angles[1]})"
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number = sequence_number,
                seq_num=int(row[0]),
                waypoint=waypoint_obj,
                path_descriptor=row[3].strip(),
                course_angle=course_angle,
                turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
                altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
                nav_spec=row[11].strip().replace("\n", "").replace(" ", ""),
                process_id=process_id
            )
            session.add(proc_des_obj)
            if is_valid_data(data := row[1]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            sequence_number += 1


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
        if len(tables) > 1:
            extract_insert_apch1(file_name, tables, rwy_dir)
        if len(tables) == 1:
            extract_insert_apch2(file_name, rwy_dir, tables)

    session.commit()
    print("Data insertion complete.")


if __name__ == "__main__":
    main()