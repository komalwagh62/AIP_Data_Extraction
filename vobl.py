from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding,AiracData, session
from sqlalchemy import select
 
 
##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re
import pdftotext  # PyMuPDF
 
AIRPORT_ICAO = "VOBL"
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
 
def conversionDMStoDD(coord):  # to convert DMS into Decimal Degrees
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    new_dir = coord[len(coord) - 1 :]
    coord = coord[: len(coord) - 1]
    decimals = coord.split(".")
    decimal = "00"
    if len(decimals) > 1:
        coord, decimal = decimals[0], decimals[1]
    SS = coord[len(coord) - 2 :]
    coord = coord[: len(coord) - 2]
    MM = coord[len(coord) - 2 :]
    coord = coord[: len(coord) - 2]
    HH = coord
    return (
        float(HH) + float(MM) / 60 + float(str(SS) + "." + str(decimal)) / 3600
    ) * direction[new_dir]
 
 
def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True
 
 
def insert_terminal_holdings(df):
    # process_id = get_active_process_id()
    df = df.drop(0)
    for _, row in df.iterrows():
        waypoint_obj = session.execute(
            select(Waypoint).where(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == row[0].strip(),
            )
        ).fetchone()[0]
        course_angle=row[3].replace("\n", "").replace("  ", "").replace(" )", ")")
        formatted_course_angle = re.sub(r"(\d+\.\d+°)(\d+\.\d+°)", r"(\1)\2", course_angle)
        term_hold_obj = TerminalHolding(
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=formatted_course_angle,
            turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
            altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
            # process_id=""
        )
        session.add(term_hold_obj)
        if is_valid_data(data := row[2]):
            if data == "Y":
                term_hold_obj.fly_over = True
            elif data == "N":
                term_hold_obj.fly_over = False
 
 
def extract_insert_sid_star(file_name, type_):
    process_id = get_active_process_id()
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    file_path = FOLDER_PATH + file_name
    print(f"Extracting from: {file_path}")
    procedure_names = None
    with open(file_path, "rb") as f:
        # To get procedure names
        texts = pdftotext.PDF(f)
        procedure_names = re.findall(r"([A-Z]+\s+[A-Z0-9]+)\s*Waypoint", texts[0])
    tables = camelot.read_pdf(
        file_path, pages="all"
    )  # Converting PDF to table DataFrames
    if len(procedure_names) != len(tables):
        # In case there's issue in capturing procedure names
        print("Error: Length of procedures and tables aren't matching")
        exit()
 
    for i in range(len(procedure_names)):
        procedure_name = procedure_names[i]
        if procedure_name == "TERMINAL HOLDINGS":
            insert_terminal_holdings(tables[i].df)
            continue
        procedure_obj = Procedure(
            airport_icao=AIRPORT_ICAO,
            rwy_dir=rwy_dir,
            type=type_,
            process_id=process_id
        )
        session.add(procedure_obj)
        procedure_obj.name, procedure_obj.designator = procedure_name.split()
 
        df = tables[i].df
        nav_spec_col_index = df.shape[1] - 1
        filter_ = df[nav_spec_col_index].apply(
            lambda x: bool(x.strip())
        )  # Filtering out invalid rows caught due to extraction
        df = df[filter_]
        # Initialize sequence number tracker
        sequence_number = 1
        seq_num = 1
        for _, row in df.iterrows():
            waypoint_obj = None
            if is_valid_data(row[0]):
                # Checking if fix is a waypoint or RWY
                waypoint_obj = session.execute(
                    select(Waypoint).where(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == row[0].strip(),
                    )
                ).fetchone()[0]
            course_angle = row[3].replace("\n", "").replace("  ", "").replace(" )", ")").replace("Mag", "").replace("True", "").replace("N/A","").replace(" ","")

# Step 1: Remove any double parentheses first
            course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', course_angle)

# Step 2: Handle cases like "(1.85°)0.02°" and convert to "1.85°(0.02°)"
            course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)

# Step 3: Check for the "1.85°(0.02°)" pattern and keep it as is
            if re.match(r"\d+\.\d+°\(\d+\.\d+°\)", course_angle):
    # If the course_angle matches the pattern, leave it unchanged
                formatted_course_angle = course_angle
            else:
    # Use regex to reformat angles in "338.67°338.42°" pattern
                formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', course_angle)

            # Split the angles if they are separated by space
                angles = formatted_course_angle.split()

    # If two angles exist after splitting, format them
                if len(angles) == 2:
                    formatted_course_angle = f"{angles[0]}({angles[1]})"
                else:
    # Use regex to handle cases like "338.67°338.42°" and "(1.85°)0.02°"
                    formatted_course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)
                    formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', formatted_course_angle)

# Step 3: Final cleanup to remove any remaining double parentheses
                formatted_course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', formatted_course_angle)

                print(formatted_course_angle)
            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=seq_num,
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=formatted_course_angle,
                turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
                altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
                altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
                process_id=process_id
            )
            session.add(proc_des_obj)
            if is_valid_data(data := row[2]):
                if data == "Y":
                    proc_des_obj.fly_over = True
                elif data == "N":
                    proc_des_obj.fly_over = False
            seq_num += 1
            sequence_number += 1
   
 
 
def extract_insert_apch_27L(file_name, rwy_dir):
    process_id = get_active_process_id()
    df = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")[0].df
    df = df.drop(index=[0, 1])
    # Extracting waypoints
    series = df[3]
    all_waypoint_str = series[series != ""].iloc[0]
    waypoint_names = all_waypoint_str.split(" \n")[1:]
    series = df[7]
    all_waypoint_str = series[series != ""].iloc[0]
    waypoint_coords = all_waypoint_str.split(" \n")[2:]
    for name, lat_long in zip(waypoint_names, waypoint_coords):
        result_row = session.execute(
            select(Waypoint).where(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == name,
            )
        ).fetchone()
        if result_row:
            # Not inserting duplicate waypoints
            continue
        lat_long = re.sub(r"[^NEWS\d. ]", "", lat_long).split()
        lat, long = lat_long[1] + lat_long[0], lat_long[3] + lat_long[2]
        lat, long = conversionDMStoDD(lat), conversionDMStoDD(long)
        coordinates = f"{lat} {long}"
        session.add(
            Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=name,
                coordinates_dd = coordinates,
                geom=f"POINT({long} {lat})",
                process_id=process_id
            )
        )
 
    # extracting coding info
    df = df[df[1] != ""]  # targeting only procedure description rows
    df = df.loc[:, (df != "").any(axis=0)]  # deleting empty columns
   
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
    for _, row in df.iterrows():
       
        row = list(row)
        waypoint_obj = None
        if is_valid_data(data := row[2]):
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == data,
                )
            ).fetchone()[0]
        course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace("Mag", "").replace("True", "").replace("N/A","").replace(" ","")

# Step 1: Remove any double parentheses first
        course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', course_angle)

# Step 2: Handle cases like "(1.85°)0.02°" and convert to "1.85°(0.02°)"
        course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)

# Step 3: Check for the "1.85°(0.02°)" pattern and keep it as is
        if re.match(r"\d+\.\d+°\(\d+\.\d+°\)", course_angle):
    # If the course_angle matches the pattern, leave it unchanged
            formatted_course_angle = course_angle
        else:
    # Use regex to reformat angles in "338.67°338.42°" pattern
            formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', course_angle)

    # Split the angles if they are separated by space
            angles = formatted_course_angle.split()

    # If two angles exist after splitting, format them
            if len(angles) == 2:
                formatted_course_angle = f"{angles[0]}({angles[1]})"
            else:
    # Use regex to handle cases like "338.67°338.42°" and "(1.85°)0.02°"
                formatted_course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)
                formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', formatted_course_angle)

# Step 3: Final cleanup to remove any remaining double parentheses
            formatted_course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', formatted_course_angle)

            print(formatted_course_angle)

        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,  # Assign sequence number based on iteration
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=formatted_course_angle,
            turn_dir=row[5].strip() if is_valid_data(row[6]) else None,
            altitude_ul=row[6].strip() if is_valid_data(row[7]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[9].strip() if is_valid_data(row[5]) else None,
            vpa_tch=row[10].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[10]) else None,
            process_id=process_id
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[3]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
        sequence_number += 1  # Increment sequence number
    # session.commit()
 
 
def extract_insert_apch_09R(file_name, rwy_dir):
    
    df = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")[0].df
    df = df.drop(index=[0, 1])
    process_id = get_active_process_id()
    # Extracting waypoints
    waypoint_names = [x for x in df.loc[:, 3].tolist() if x]
    waypoint_names.pop(0)  # removing heading
    waypoint_coords = [x for x in df.loc[:, 9].tolist() if x]
    waypoint_coords.pop(0)  # removing heading
    for name, lat_long in zip(waypoint_names, waypoint_coords):
        result_row = session.execute(
            select(Waypoint).where(
                Waypoint.airport_icao == AIRPORT_ICAO,
                Waypoint.name == name,
            )
        ).fetchone()
        if result_row:
            # Not inserting duplicate waypoints
            continue
        lat_long = re.sub(r"[^NEWS\d. ]", "", lat_long).split()
        lat, long = lat_long[1] + lat_long[0], lat_long[3] + lat_long[2]
        lat, long = conversionDMStoDD(lat), conversionDMStoDD(long)
        coordinates = f"{lat} {long}"
        session.add(
            Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=name,
                coordinates_dd = coordinates,
                geom=f"POINT({long} {lat})",
                process_id=process_id
            )
        )
 
    # extracting coding info
    df = df[df[1] != ""]  # targeting only procedure description rows
    df = df.loc[:, (df != "").any(axis=0)]  # deleting empty columns
 
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
    for _, row in df.iterrows():
        # print(row[0])
        # if not row[0].strip().isdigit():  # Skip rows without valid sequence numbers
        #     continue
       
        row = list(row)
        if not re.match(r"\d+$", row[0]):
            x = row[0]
            row = re.split(r" *\n *", x)
            if row[0][0].isnumeric():
                a = row.pop(0) + "\n" + row.pop(-2)
                row.insert(4, a)
            a = row.pop(0)
            row[-1] = a + " " + row[-1]
            row.insert(0, row.pop(-2))
        waypoint_obj = None
        if is_valid_data(data := row[2]):
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == data,
                )
            ).fetchone()[0]
        course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace("Mag", "").replace("True", "").replace("N/A","").replace(" ","")

# Step 1: Remove any double parentheses first
        course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', course_angle)

# Step 2: Handle cases like "(1.85°)0.02°" and convert to "1.85°(0.02°)"
        course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)

# Step 3: Check for the "1.85°(0.02°)" pattern and keep it as is
        if re.match(r"\d+\.\d+°\(\d+\.\d+°\)", course_angle):
    # If the course_angle matches the pattern, leave it unchanged
            formatted_course_angle = course_angle
        else:
    # Use regex to reformat angles in "338.67°338.42°" pattern
            formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', course_angle)

    # Split the angles if they are separated by space
            angles = formatted_course_angle.split()

    # If two angles exist after splitting, format them
            if len(angles) == 2:
                formatted_course_angle = f"{angles[0]}({angles[1]})"
            else:
    # Use regex to handle cases like "338.67°338.42°" and "(1.85°)0.02°"
             formatted_course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)
             formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', formatted_course_angle)

# Step 3: Final cleanup to remove any remaining double parentheses
            formatted_course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', formatted_course_angle)

            print(formatted_course_angle)

        
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,  # Assign sequence number based on iteration
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[1].strip(),
            course_angle=formatted_course_angle,
            turn_dir=row[5].strip() if is_valid_data(row[6]) else None,
            altitude_ul=row[6].strip() if is_valid_data(row[7]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
            speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
            dst_time=row[9].strip() if is_valid_data(row[5]) else None,
            vpa_tch=row[10].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[10]) else None,
            process_id=process_id
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[3]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
       
        sequence_number += 1  # Increment sequence number
    # session.commit()
       
       
def extract_insert_apch(file_name):
    process_id = get_active_process_id()
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    if rwy_dir == "09R":
        extract_insert_apch_09R(file_name, rwy_dir)
        return
    elif rwy_dir == "27L":
        extract_insert_apch_27L(file_name, rwy_dir)
        return
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        # process_id = get_active_process_id()
        waypoint_df = waypoint_table.df
        if re.search(r"Waypoint", waypoint_df[0][0], re.I):
            waypoint_df = waypoint_df.drop(0)
        for _, row in waypoint_df.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                # Not inserting duplicate waypoints
                continue
            data = row[1]
            data = data.replace(":", "")
            lat, lng = re.findall(r"([A-Z])\s*([\d.]+)", data)
            lat = lat[1] + lat[0]
            lng = lng[1] + lng[0]
            lat, lng = conversionDMStoDD(lat), conversionDMStoDD(lng)
            coordinates = f"{lat} {lng}"
            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
                )
            )
    coding_df = tables[0].df
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
        # if not row[0].strip().isdigit():  # Skip rows without valid sequence numbers
            # continue
        waypoint_obj = None
        if is_valid_data(row[2]):
            # Checking if fix is a waypoint or RWY
            waypoint_obj = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[2].strip(),
                )
            ).fetchone()[0]
        course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace("Mag", "").replace("True", "").replace("N/A","").replace(" ","")

# Step 1: Remove any double parentheses first
        course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', course_angle)

# Step 2: Handle cases like "(1.85°)0.02°" and convert to "1.85°(0.02°)"
        course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)

# Step 3: Check for the "1.85°(0.02°)" pattern and keep it as is
        if re.match(r"\d+\.\d+°\(\d+\.\d+°\)", course_angle):
    # If the course_angle matches the pattern, leave it unchanged
            formatted_course_angle = course_angle
        else:
    # Use regex to reformat angles in "338.67°338.42°" pattern
            formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', course_angle)

    # Split the angles if they are separated by space
            angles = formatted_course_angle.split()

    # If two angles exist after splitting, format them
            if len(angles) == 2:
                formatted_course_angle = f"{angles[0]}({angles[1]})"
            else:
    # Use regex to handle cases like "338.67°338.42°" and "(1.85°)0.02°"
                formatted_course_angle = re.sub(r'\((\d+\.\d+°)\)(\d+\.\d+°)', r'\1(\2)', course_angle)
                formatted_course_angle = re.sub(r'(\d+\.\d+°)(\d+\.\d+°)', r'\1(\2)', formatted_course_angle)

# Step 3: Final cleanup to remove any remaining double parentheses
            formatted_course_angle = re.sub(r'\(\((.*?)\)\)', r'(\1)', formatted_course_angle)

            print(formatted_course_angle)

        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            sequence_number=sequence_number,  # Assign sequence number based on iteration
            seq_num=row[0],
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=formatted_course_angle,
            turn_dir=row[5].strip() if is_valid_data(row[4]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
            process_id=process_id
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[1]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False
        sequence_number += 1  # Increment sequence number
    # session.commit()
 
 
def extract_coding_tables_vobl():
    file_names = os.listdir(FOLDER_PATH)
    # Capturing coding files of all procedures, and waypoint files
    sid_coding_file_names = []
    star_coding_file_names = []
    apch_coding_file_names = []
    waypoint_file_names = []
    for file_name in file_names:
        if file_name.find("WAYPOINTS") > -1:
            waypoint_file_names.append(file_name)
        elif file_name.find("CODING") > -1:
            if file_name.find("SID") > -1:
                sid_coding_file_names.append(file_name)
            elif file_name.find("STAR") > -1:
                star_coding_file_names.append(file_name)
            else:
                apch_coding_file_names.append(file_name)
 
    # Inserting waypoint files first because procedures have dependency on it.
    for waypoint_file_name in waypoint_file_names:
        process_id = get_active_process_id()
        # process_id = get_active_process_id()
        # print("extracting Waypoint file:", waypoint_file_name)
        df = camelot.read_pdf(FOLDER_PATH + waypoint_file_name, pages="1")[0].df
        if re.search(r"WAYPOINT", df[0][0], re.I):
            df = df.drop(0)
        for _, row in df.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]
            result_row = session.execute(
                select(Waypoint).where(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == row[0].strip(),
                )
            ).fetchone()
            if result_row:
                # Not inserting duplicate waypoints
                continue
            data = row[1]
            data = data.replace(":", "")
            lat, lng = re.findall(r"([A-Z])\s*([\d.]+)", data)
            lat = lat[1] + lat[0]
            lng = lng[1] + lng[0]
            lat, lng = conversionDMStoDD(lat), conversionDMStoDD(lng)
            coordinates = f"{lat} {lng}"
            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
                )
            )
    for file_name in sid_coding_file_names:
        extract_insert_sid_star(file_name, "SID")
    for file_name in star_coding_file_names:
        extract_insert_sid_star(file_name, "STAR")
    for file_name in apch_coding_file_names:
        extract_insert_apch(file_name)
    session.commit()
   
 
 
if __name__ == "__main__":
    extract_coding_tables_vobl()
 
 
"""
worksheet = pd.read_excel("/home/ansad/Projects/Skies FPD/Coding Tables Extraction/AIP Supplements Extraction/AIP Supp 74-2023_VIDP RNAV SID & STAR.xlsx", sheet_name=['WPT', 'SID', 'STAR', 'HLDG'])
"""
 