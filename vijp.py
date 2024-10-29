from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, session
from sqlalchemy import select

##################
# EXTRACTOR CODE #
##################

import camelot
import pdftotext
import re
import os
import pandas as pd


AIRPORT_ICAO = "VIJP"
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


def extract_insert_sid(type_):
    pdf_files = [
        file
        for file in os.listdir(FOLDER_PATH)
        if "SID" in file.upper() and "CODING" in file.upper()
    ]
    pdf_files.sort()
    prev_procedure_obj = None
    seq_num = 1
    for pdf_file in pdf_files:
        pdf_path = os.path.join(FOLDER_PATH, pdf_file)
        data_frame = camelot.read_pdf(pdf_path, pages="all")[0].df
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", pdf_file).groups()[0]
        for _, row in data_frame.iterrows():
            row = list(row)
            if re.match(r"^[A-Z]+\s+\d+[A-Z]*$", row[0]):
                procedure_name = row[0]
                name, designator = re.match(
                    r"^([A-Z]+)\s+(\d+[A-Z]*)$", procedure_name
                ).groups()
                procedure_obj = Procedure(
                    airport_icao=AIRPORT_ICAO,
                    rwy_dir=rwy_dir,
                    type=type_,
                    name=name,
                    designator=designator,
                )
                session.add(procedure_obj)
                seq_num = 1
                prev_procedure_obj = procedure_obj
            elif row[0][0].isdigit() and len(row[0].split("\n")) > 1:
                data_parts = row[0].split(" \n")
                course_angle = data_parts.pop(0)
                course_angle = course_angle + " " + data_parts.pop(-1)
                data_parts = [part for part in data_parts if part != ""]
                waypoint_name = data_parts[0]
                # print(waypoint_name,"waypoint name")
                data_parts.insert(3, course_angle)

                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
                proc_des_obj = ProcedureDescription(
                    procedure=prev_procedure_obj,  # Use the previous procedure object
                    seq_num=seq_num,
                    waypoint=waypoint_obj,
                    path_descriptor=data_parts[2].strip(),
                    course_angle=course_angle,
                    turn_dir=data_parts[4].strip()
                    if is_valid_data(data_parts[4])
                    else None,
                    altitude_ul=data_parts[5].strip()
                    if is_valid_data(data_parts[5])
                    else None,
                    altitude_ll=data_parts[6].strip()
                    if is_valid_data(data_parts[6])
                    else None,
                    speed_limit=data_parts[7].strip()
                    if is_valid_data(data_parts[7])
                    else None,
                    dst_time=data_parts[8].strip()
                    if is_valid_data(data_parts[8])
                    else None,
                    vpa_tch=data_parts[9].strip()
                    if is_valid_data(data_parts[9])
                    else None,
                    nav_spec=data_parts[10].strip()
                    if is_valid_data(data_parts[10])
                    else None,
                )
                session.add(proc_des_obj)
                if is_valid_data(data := row[1]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                seq_num += 1  # Increment sequence number for next row
            elif row[-1].strip():
                waypoint_name = row[0].strip()
                # print(waypoint_name,"yh")
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )
                proc_des_obj = ProcedureDescription(
                    procedure=prev_procedure_obj,  # Use the previous procedure object
                    seq_num=seq_num,
                    waypoint=waypoint_obj,
                    path_descriptor=row[2].strip(),
                    course_angle=row[3]
                    .replace("\n", "")
                    .replace("  ", "")
                    .replace(" )", ")"),
                    turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
                    altitude_ul=row[5].strip() if is_valid_data(row[5]) else None,
                    altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
                    speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
                    dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                    vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                    nav_spec=row[10].strip() if is_valid_data(row[10]) else None,
                )
                session.add(proc_des_obj)
                if is_valid_data(data := row[1]):
                    if data == "Y":
                        proc_des_obj.fly_over = True
                    elif data == "N":
                        proc_des_obj.fly_over = False
                seq_num += 1  # Increment sequence number for next row


def extract_insert_star(type_):
    pdf_files = [
        file
        for file in os.listdir(FOLDER_PATH)
        if "STAR" in file.upper() and "CODING" in file.upper()
    ]
    pdf_files.sort()
    prev_procedure_obj = None
    procedure_obj = None
    seq_num = 1  # Initialize sequence number
    for pdf_file in pdf_files:
        pdf_path = os.path.join(FOLDER_PATH, pdf_file)
        data_frame = camelot.read_pdf(pdf_path, pages="all")[0].df
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", pdf_file).groups()[0]
        procedure_obj = None  # Initialize procedure object
        i = 0
        for _, row in data_frame.iterrows():
            procedure_name_match = re.match(r"[A-Z]+\s+\d+[A-Z]*|1[AC]", row[0])
            if procedure_name_match:  # Assuming procedure names are in the first column
                procedure_name_str = row[0].replace("&", ",")
                procedure_names = re.split(r"\s*,\s*", procedure_name_str)
                procedure_names = [
                    name.strip() for name in procedure_names if name.strip()
                ]
                i = 0
                continue  # Move to the next row after extracting procedure names
            if row[2] == "IF":
                procedure_name = procedure_names[i]
                procedure_name = procedure_name.strip()
                i += 1
                # print("Current row", procedure_name)
                procedure_obj = Procedure(
                    airport_icao=AIRPORT_ICAO,
                    rwy_dir=rwy_dir,
                    name=procedure_name,
                    type=type_,
                )
                session.add(procedure_obj)
                prev_procedure_obj = procedure_obj
                seq_num = 1
                procedure_obj.name, procedure_obj.designator = procedure_name.split()
            if prev_procedure_obj:  # checks if a procedure_obj exists
                nav_spec_col_index = (
                    len(row) - 1
                )  # If the navigation specification is not empty...
                if bool(row[nav_spec_col_index].strip()):
                    waypoint_name = row[0].strip()
                    if is_valid_data(waypoint_name):
                        waypoint_obj = session.execute(
                            select(Waypoint).where(
                                Waypoint.airport_icao == AIRPORT_ICAO,
                                Waypoint.name == waypoint_name,
                            )
                        ).fetchone()[0]
                        proc_des_obj = ProcedureDescription(
                            procedure=prev_procedure_obj,
                            seq_num=seq_num,
                            waypoint=waypoint_obj,
                            path_descriptor=row[2].strip(),
                            course_angle=row[3]
                            .replace("\n", "")
                            .replace("  ", "")
                            .replace(" )", ")"),
                            turn_dir=row[4].strip() if is_valid_data(row[4]) else None,
                            altitude_ul=row[5].strip()
                            if is_valid_data(row[5])
                            else None,
                            altitude_ll=row[6].strip()
                            if is_valid_data(row[6])
                            else None,
                            speed_limit=row[7].strip()
                            if is_valid_data(row[7])
                            else None,
                            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
                            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
                            nav_spec=row[10].strip()
                            if is_valid_data(row[10])
                            else None,
                        )
                        session.add(proc_des_obj)
                        if is_valid_data(data := row[1]):
                            if data == "Y":
                                proc_des_obj.fly_over = True
                            elif data == "N":
                                proc_des_obj.fly_over = False
                        seq_num += 1  # Increment sequence number for next row

                elif row[0][0].isdigit() and len(row[0].split("\n")) > 1:
                    data_parts = row[0].split(" \n")
                    course_angle = data_parts.pop(0)
                    course_angle = course_angle + " " + data_parts.pop(-1)
                    data_parts = [part for part in data_parts if part != ""]
                    waypoint_name = data_parts[0]
                    # print("Waypoint Name:", waypoint_name)
                    data_parts.insert(3, course_angle)
                    waypoint_obj = (
                        session.query(Waypoint)
                        .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                        .first()
                    )
                    # print("Waypoint ID:", waypoint_obj.id if waypoint_obj else None)  # Debugging line
                    proc_des_obj = ProcedureDescription(
                        procedure=prev_procedure_obj,
                        seq_num=seq_num,
                        waypoint=waypoint_obj,
                        path_descriptor=data_parts[2].strip(),
                        course_angle=course_angle,
                        turn_dir=data_parts[4].strip()
                        if is_valid_data(data_parts[4])
                        else None,
                        altitude_ul=data_parts[5].strip()
                        if is_valid_data(data_parts[5])
                        else None,
                        altitude_ll=data_parts[6].strip()
                        if is_valid_data(data_parts[6])
                        else None,
                        speed_limit=data_parts[7].strip()
                        if is_valid_data(data_parts[7])
                        else None,
                        dst_time=data_parts[8].strip()
                        if is_valid_data(data_parts[8])
                        else None,
                        vpa_tch=data_parts[9].strip()
                        if is_valid_data(data_parts[9])
                        else None,
                        nav_spec=data_parts[10].strip()
                        if is_valid_data(data_parts[10])
                        else None,
                    )
                    session.add(proc_des_obj)
                    if is_valid_data(data := row[1]):
                        if data == "Y":
                            proc_des_obj.fly_over = True
                        elif data == "N":
                            proc_des_obj.fly_over = False
                    seq_num += 1  # Increment sequence number for next row


def extract_insert_apch1(file_name, tables, rwy_dir):
    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0, 1])
    # print(coding_df)
    apch_data_df = coding_df[coding_df.iloc[:, -2] == "RNP \nAPCH"]
    apch_data_df = apch_data_df.loc[:, (apch_data_df != "").any(axis=0)]
    # print(apch_data_df)
    if not coding_df.empty and len(coding_df.columns) > 7:
        waypoint_list = coding_df.iloc[
            :, 3
        ].tolist()  # Extract WPT values from second-to-last column
        # print(waypoint_list)
        lat_long_list = coding_df.iloc[
            :, 7
        ].tolist()  # Extract Latitude/Longitude values from seventh column
        # Split the strings using '\n' and filter out empty parts
        waypoint_list = [
            part
            for waypoint in waypoint_list
            for part in waypoint.split("\n")
            if part.strip() != ""
        ]
        print(waypoint_list)
        lat_long_list = [
            part
            for lat_long in lat_long_list
            for part in lat_long.split("\n")
            if part.strip() != ""
        ]
        # Remove the first index from waypoint_list
        waypoint_list.pop(0)
        # Remove the first two indices from lat_long_list
        lat_long_list = lat_long_list[2:]
        # Iterate through both lists and display the data
        for waypoint, lat_long in zip(waypoint_list, lat_long_list):
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
            lng = conversionDMStoDD(lng_value + lng_dir)
            # If the waypoint doesn't exist, add it to the database
            if not waypoint1:
                new_waypoint = Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=waypoint.strip(),
                    geom=f"POINT({lng} {lat})",
                )
                session.add(new_waypoint)
                session.commit()
    procedure_name = (
        re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
    )
    procedure_obj = Procedure(
        airport_icao=AIRPORT_ICAO,
        rwy_dir=rwy_dir,
        type="APCH",
        name=procedure_name,
    )
    session.add(procedure_obj)
    for _, row in apch_data_df.iterrows():
        waypoint_obj = None
        if is_valid_data(row[4]):
            waypoint_name = row[4].strip().strip().replace("\n", "").replace(" ", "")
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                .first()
            )
            # print(f"Waypoint name: {waypoint_name}, Waypoint object: {waypoint_obj}")
        proc_des_obj = ProcedureDescription(
            procedure=procedure_obj,
            seq_num=int(row[1]),
            waypoint=waypoint_obj,
            path_descriptor=row[5].strip(),
            course_angle=row[6].replace("\n", "").replace("  ", "").replace(" )", ")"),
            turn_dir=row[8].strip() if is_valid_data(row[8]) else None,
            altitude_ll=row[9].strip() if is_valid_data(row[9]) else None,
            speed_limit=row[10].strip() if is_valid_data(row[10]) else None,
            dst_time=row[11].strip() if is_valid_data(row[11]) else None,
            vpa_tch=row[12].strip() if is_valid_data(row[12]) else None,
            role_of_the_fix=row[13].strip() if is_valid_data(row[13]) else None,
            nav_spec=row[15].strip() if is_valid_data(row[15]) else None,
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[2]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False


def extract_insert_apch2(file_name, rwy_dir, tables):
    waypoint_tables = tables[1:]
    for waypoint_table in waypoint_tables:
        waypoint_df = waypoint_table.df
        # print("Waypoint Table:", waypoint_df)  # Add this line to print the waypoint_df
        if re.search(r"Waypoint List", waypoint_df[0][0], re.I):
            waypoint_df = waypoint_df.drop(0)
        for _, row in waypoint_df.iterrows():
            row = list(row)
            
            row = [x for x in row if x.strip()]
            if len(row) < 2:  # length of the row list is less than 2.
                continue
            waypoint_name1 = row[0].strip()
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
            # These lines query the database to check if a waypoint with the same name and airport ICAO code already exists.
            waypoint1 = (
                session.query(Waypoint)
                .filter(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == waypoint_name1,
                )
                .first()
            )
            # If not, it adds a new Waypoint object to the session with the extracted information and coordinates.
            if not waypoint1:
                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        name=waypoint_name1,
                        geom=f"POINT({lng1} {lat1})",
                    )
                )
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
    )
    session.add(procedure_obj)
    for _, row in coding_df.iterrows():
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
            seq_num=int(row[0]),
            waypoint=waypoint_obj,
            path_descriptor=row[3].strip(),
            course_angle=row[4].replace("\n", "").replace("  ", "").replace(" )", ")"),
            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
            speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
            dst_time=row[8].strip() if is_valid_data(row[8]) else None,
            vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
            role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
            nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
        )
        session.add(proc_des_obj)
        if is_valid_data(data := row[1]):
            if data == "Y":
                proc_des_obj.fly_over = True
            elif data == "N":
                proc_des_obj.fly_over = False


def insert_terminal_holdings(file_name, type_):
    file_path = FOLDER_PATH + file_name
    # print(file_path)
    tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="1")
    # print(tables)
    df = camelot.read_pdf(file_path, pages="all")[0].df
    df = df.iloc[1:]
    df = df.drop(df.index[-1])
    for _, row in df.iterrows():
        waypoint_obj = None
        if is_valid_data(row[1]):
            waypoint_obj = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=row[1].strip())
                .first()
            )
            # print(waypoint_obj)
        term_hold_obj = TerminalHolding(
            waypoint_id=waypoint_obj.id,
            course_angle=row[3].replace("\n", "").replace("  ", "").replace(" )", ")"),
            dst_time=row[4].strip() if is_valid_data(row[4]) else None,
            turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
            altitude_ul=row[6].strip() if is_valid_data(row[6]) else None,
            altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
        )
        session.add(term_hold_obj)


def main():
    file_names = os.listdir(FOLDER_PATH)
    holding_waypoints_file_names = []
    waypoint_file_names = []
    apch_coding_file_names = []
    for file_name in file_names:
        if file_name.find("HOLDING-WAYPOINTS") > -1:
            holding_waypoints_file_names.append(file_name)
        elif file_name.find("WAYPOINTS") > -1:
            waypoint_file_names.append(file_name)
        elif file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)
    # loop iterates through the list of waypoint_file_names
    for waypoint_file_name in waypoint_file_names:
        df = camelot.read_pdf(os.path.join(FOLDER_PATH, waypoint_file_name), pages="1")[
            0
        ].df  # uses camelot to read the first page of the PDF
        if re.search(
            r"WPT", df[0][0], re.I
        ):  # condition checks if the first cell of the df contains the text WPT using a regular expression
            df = df.drop(0)  # drops the first row
        for (
            _,
            row,
        ) in (
            df.iterrows()
        ):  # loop iterates through each row in the DataFrame (df) using the .iterrows() function.
            row = list(row)
            row = [
                x for x in row if x.strip()
            ]  # row containing only non-empty values by removing leading and trailing whitespace using x.strip()
            if len(row) < 2:  # length of the row list is less than 2.
                continue
            waypoint_name1 = row[
                0
            ].strip()  # extracts the waypoint name from the first element of the row list
            # regular expressions to extract latitude and longitude data from the second element of the row list.
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
            # These lines query the database to check if a waypoint with the same name and airport ICAO code already exists.
            waypoint1 = (
                session.query(Waypoint)
                .filter(
                    Waypoint.airport_icao == AIRPORT_ICAO,
                    Waypoint.name == waypoint_name1,
                )
                .first()
            )
            # If not, it adds a new Waypoint object to the session with the extracted information and coordinates.
            if not waypoint1:
                session.add(
                    Waypoint(
                        airport_icao=AIRPORT_ICAO,
                        name=waypoint_name1,
                        geom=f"POINT({lng1} {lat1})",
                    )
                )
            if len(row) > 2:
                waypoint_name2 = row[2].strip()
                extracted_data2 = [
                    item
                    for match in re.findall(
                        r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", row[3]
                    )
                    for item in match
                ]
                lat_dir2, lat_value2, lng_dir2, lng_value2 = extracted_data2
                lat2 = conversionDMStoDD(lat_value2 + lat_dir2)
                lng2 = conversionDMStoDD(lng_value2 + lng_dir2)
                waypoint2 = (
                    session.query(Waypoint)
                    .filter(
                        Waypoint.airport_icao == AIRPORT_ICAO,
                        Waypoint.name == waypoint_name2,
                    )
                    .first()
                )
                if not waypoint2:
                    session.add(
                        Waypoint(
                            airport_icao=AIRPORT_ICAO,
                            name=waypoint_name2,
                            geom=f"POINT({lng2} {lat2})",
                        )
                    )

    extract_insert_sid("SID")
    extract_insert_star("STAR")
    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        if len(tables) == 1:
            extract_insert_apch1(file_name, tables, rwy_dir)
        elif len(tables) > 1:
            extract_insert_apch2(file_name, rwy_dir, tables)
    for file_name in holding_waypoints_file_names:
        insert_terminal_holdings(file_name, "HOLDING-WAYPOINTS")

    # Commit the changes to the database
    session.commit()
    print("Waypoints added successfully to the database.")


if __name__ == "__main__":
    main()
