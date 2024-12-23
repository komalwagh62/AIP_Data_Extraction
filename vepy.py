import camelot
import re
import os

from sqlalchemy import select

from model import AiracData, session, Waypoint, Procedure, ProcedureDescription



##################
# EXTRACTOR CODE #
##################

import fitz  # PyMuPDF

AIRPORT_ICAO = "VEPY"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"

def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}
    print("Direction:", coord[-1])
    new_dir = coord[-1]
    coord = coord[:-1]
    print("Remaining coord:", coord)
    parts = re.split(r"[:.]", coord)
    print("Parts:", parts)

    if len(parts) == 3:
        HH, MM, SS = map(int, parts)
        decimal = 0
    elif len(parts) == 4:
        HH, MM, SS, decimal = map(int, parts)
    else:
        raise ValueError("Invalid coordinate format")
    decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[new_dir]
    print("Latitude before conversion:", decimal_degrees)  # Add this line
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



def extract_insert_sid_star(file_name):
    process_id = get_active_process_id()
    rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
    seq_num = 1
    # Initialize sequence number tracker
    sequence_number = 1
    with open(FOLDER_PATH + file_name, "rb") as f:
        pdf = fitz.open(f)
        if len(pdf) >= 1:
            if re.search(r"TABULAR DESCRIPTION BGD 1 DEPARTURE-VEPY", pdf[0], re.I):
                # Extract procedure name from the file name
                procedure_name = re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")

                # Create a Procedure object and add it to the session
                procedure_obj = Procedure(
                    airport_icao=AIRPORT_ICAO,
                    rwy_dir=rwy_dir,
                    type="SID",
                    name=procedure_name,
                    process_id=process_id
                )
                session.add(procedure_obj)
                
                df = camelot.read_pdf(
                    FOLDER_PATH + file_name,
                    pages="all"  # or specific page number if needed
                )[0].df
                # print(df)
                waypoint_obj = None
                for _, row in df.iterrows():
                    waypoint_obj = None
                    if is_valid_data(row[0]):
                        waypoint_name = row[2].strip()  # Change 2 to the correct index
                        # print(waypoint_name)
                        waypoint_query = session.query(Waypoint).filter(
                            Waypoint.airport_icao == AIRPORT_ICAO,
                            Waypoint.name == waypoint_name,
                        )
                        waypoint_obj = waypoint_query.first()
                        # print(waypoint_obj)
                    proc_des_obj = ProcedureDescription(
                        procedure=procedure_obj,
                        sequence_number=sequence_number,
                        seq_num=seq_num,
                        waypoint=waypoint_obj,
                        path_descriptor=row[3].strip(),
                        course_angle=row[4]
                            .replace("\n", "")
                            .replace(" ", "")
                            .replace(" )", ")"),
                        turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
                        altitude_ul=row[6].strip() if is_valid_data(row[6]) else None,
                        altitude_ll=row[7].strip() if is_valid_data(row[7]) else None,
                        speed_limit=row[8].strip() if is_valid_data(row[8]) else None,
                        dst_time=row[9].strip() if is_valid_data(row[9]) else None,
                        vpa_tch=row[10].strip() if is_valid_data(row[10]) else None,
                        nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
                        process_id=process_id
                    )
                    session.add(proc_des_obj)
                    if is_valid_data(data := row[1]):
                        if data == "Y":
                            proc_des_obj.fly_over = True
                        elif data == "N":
                            proc_des_obj.fly_over = False
                    seq_num += 1
                    sequence_number += 1

                session.commit()

                        
    
def extract_insert_apch(file_name, tables, rwy_dir):
    coding_df = tables[0].df
    coding_df = coding_df.drop(index=[0, 1])
    #print(coding_df)
    apch_data_df = coding_df[coding_df.iloc[:, -2] == "RNP \nAPCH"]
    apch_data_df = apch_data_df.loc[:, (apch_data_df != "").any(axis=0)]
    print(apch_data_df)
    process_id = get_active_process_id()
    if not coding_df.empty and len(coding_df.columns) > 7:
        waypoint_list = coding_df.iloc[
            :, 3
        ].tolist()  # Extract WPT values from second-to-last column
        #print(waypoint_list)
        lat_long_list = coding_df.iloc[
            :, 10
        ].tolist()  # Extract Latitude/Longitude values from seventh column
        # Split the strings using '\n' and filter out empty parts
        waypoint_list = [
            part
            for waypoint in waypoint_list
            for part in waypoint.split("\n")
            if part.strip() != ""
        ]
        lat_long_list = [
            part
            for lat_long in lat_long_list
            for part in lat_long.split("\n")
            if part.strip() != ""
        ]
        #print(waypoint_list)
        #print("lat long is",lat_long_list)
        # Remove the first index from waypoint_list
        waypoint_list.pop(0)
        # Remove the first two indices from lat_long_list
        lat_long_list = lat_long_list[1:]
        lat = 0.0
        lng = 0.0
        # Iterate through both lists and display the data
        
        for waypoint, lat_long in zip(waypoint_list, lat_long_list):
            # Check if the waypoint exists in the database
            waypoint1 = (
                session.query(Waypoint)
                .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint.strip())
                .first()
            )
            matches = re.findall(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", lat_long)
            if matches:
                lat_dir, lat_value, lng_dir, lng_value = matches[0]
                lat = conversionDMStoDD(lat_value + lat_dir)
                lng = conversionDMStoDD(lng_value + lng_dir)
            coordinates = f"{lat} {lng}"
            # If the waypoint doesn't exist, add it to the database
            if not waypoint1:
                new_waypoint = Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=waypoint.strip(),
                    coordinates_dd = coordinates,
                    geom=f"POINT({lng} {lat})",
                    process_id=process_id
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
        process_id=process_id
    )
    session.add(procedure_obj)
    


def main():
    file_names = os.listdir(FOLDER_PATH)
    waypoint_file_names = []
    sid_coding_file_names = []
    apch_coding_file_names=[]

    for file_name in file_names:
        if file_name.find("SID") > -1:
            waypoint_file_names.append(file_name)
            sid_coding_file_names.append(file_name)
        elif file_name.find("CODING") > -1:
            apch_coding_file_names.append(file_name)
            
    for waypoint_file_name in waypoint_file_names:
        with open(FOLDER_PATH + waypoint_file_name, "rb") as f:
            pdf = fitz.open(f)
            process_id = get_active_process_id()
            if len(pdf) >= 1:
                if re.search(r"WAYPOINT INFORMATION", pdf[0], re.I):
                    table_index = 1 
                    df = camelot.read_pdf(
                        FOLDER_PATH + waypoint_file_name,
                        pages="all" #str(table_index + 1),  # Page numbers start from 1
                    )[1].df
                    print(df)
                    if re.search(r"WAYPOINT INFORMATION-", str(df[1]), re.I):
                        df = df.drop(0)
                        print(df)
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
                        data = row[1]
                        # data = data.replace(":", "")
                        match = re.match(r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", data)
                        print(match)
                        # if match:
                        lat, lat_dir, lng, lng_dir = match.groups()
                        print(lat)
                        print(lat_dir)
                        lat = conversionDMStoDD(lat_dir + lat)
                            
                        lng = conversionDMStoDD(lng_dir + lng)
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
        extract_insert_sid_star(file_name)                     
    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        if len(tables) == 1:
            extract_insert_apch(file_name, tables, rwy_dir)
    
    session.commit()
    print("Data insertion complete.")



if __name__ == "__main__":
    main()
