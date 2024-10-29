import camelot
import re
import os

from sqlalchemy import select

from model import session, Waypoint, Procedure, ProcedureDescription,TerminalHolding

##################
# EXTRACTOR CODE #
##################
import camelot
import os
import re


AIRPORT_ICAO = "VILK"
FOLDER_PATH = f"./{AIRPORT_ICAO}/"


def conversionDMStoDD(coord):
    direction = {"N": 1, "S": -1, "E": 1, "W": -1}

    # Remove non-numeric characters
    coord_numeric = "".join(char for char in coord if char.isdigit() or char == ".")

    # Check if the coordinate is in decimal format
    try:
        decimal_degrees = float(coord_numeric)
        return decimal_degrees
    except ValueError:
        pass  # Continue with degrees, minutes, and seconds conversion

    # Check if the coordinate string has at least one character
    if len(coord) > 0:
        # Extract direction (N/S/E/W)
        new_dir = coord[-1]

        # Split degrees, minutes, and seconds parts
        parts = re.split(r"[:.]", coord_numeric)

        # Handle different numbers of parts
        if len(parts) == 3:
            HH, MM, SS = map(int, parts)
            decimal = 0
        elif len(parts) == 4:
            HH, MM, SS, decimal = map(int, parts)
        else:
            raise ValueError("Invalid coordinate format")

        # Calculate decimal degrees
        decimal_degrees = (HH + MM / 60 + (SS + decimal / 100) / 3600) * direction[
            new_dir
        ]
        return decimal_degrees
    else:
        # Handle the case where the coordinate string is empty
        return None


def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True

# def extract_insert_apch(file_name, rwy_dir, tables):
#     coding_df = tables[0].df
    
#     apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]
#     if 'Sequence \nNumber' in apch_data_df.iloc[1][1]:
#         print(apch_data_df)
        
def extract_insert_apch(file_name, rwy_dir, tables):
 coding_df = tables[0].df
 coding_df = coding_df.drop(index=[0])
 apch_data_df = coding_df[coding_df.iloc[:, -2] == "RNP \nAPCH"]
 apch_data_df = apch_data_df.loc[:, (apch_data_df != "").any(axis=0)]

 if coding_df[1][1] == 'Sequence \nNumber':
   
    
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
        # print(waypoint_list)
        lat_long_list = [
            part
            for lat_long in lat_long_list
            for part in lat_long.split("\n")
            if part.strip() != ""
        ]
        # print(lat_long_list)
        # Remove the first index from waypoint_list
    #     waypoint_list.pop(0)
    #     # Remove the first two indices from lat_long_list
    #     lat_long_list = lat_long_list[2:]
    #     # Iterate through both lists and display the data
    #     for waypoint, lat_long in zip(waypoint_list, lat_long_list):
    #         # Check if the waypoint exists in the database
    #         waypoint1 = (
    #             session.query(Waypoint)
    #             .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint.strip())
    #             .first()
    #         )
    #         lat_dir, lat_value, lng_dir, lng_value = re.findall(
    #             r"([NS])\s*([\d:.]+)\s*([EW])\s*([\d:.]+)", lat_long
    #         )[0]
    #         lat = conversionDMStoDD(lat_value + lat_dir)
    #         lng = conversionDMStoDD(lng_value + lng_dir)
    #         # If the waypoint doesn't exist, add it to the database
    #         if not waypoint1:
    #             new_waypoint = Waypoint(
    #                 airport_icao=AIRPORT_ICAO,
    #                 name=waypoint.strip(),
    #                 geom=f"POINT({lng} {lat})",
    #             )
    #             session.add(new_waypoint)
    # procedure_name = (
    #     re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
    # )
    # procedure_obj = Procedure(
    #     airport_icao=AIRPORT_ICAO,
    #     rwy_dir=rwy_dir,
    #     type="APCH",
    #     name=procedure_name,
    # )
    # session.add(procedure_obj)
    # for _, row in apch_data_df.iterrows():
    #     waypoint_obj = None
    #     if is_valid_data(row[4]):
    #         waypoint_name = row[4].strip().strip().replace("\n", "").replace(" ", "")
    #         waypoint_obj = (
    #             session.query(Waypoint)
    #             .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
    #             .first()
    #         )
    #         # print(f"Waypoint name: {waypoint_name}, Waypoint object: {waypoint_obj}")
    #     proc_des_obj = ProcedureDescription(
    #         procedure=procedure_obj,
    #         seq_num=int(row[1]),
    #         waypoint=waypoint_obj,
    #         path_descriptor=row[5].strip(),
    #         course_angle=row[6].replace("\n", "").replace("  ", "").replace(" )", ")"),
    #         turn_dir=row[8].strip() if is_valid_data(row[8]) else None,
    #         altitude_ll=row[9].strip() if is_valid_data(row[9]) else None,
    #         speed_limit=row[10].strip() if is_valid_data(row[10]) else None,
    #         dst_time=row[11].strip() if is_valid_data(row[11]) else None,
    #         vpa_tch=row[12].strip() if is_valid_data(row[12]) else None,
    #         role_of_the_fix=row[13].strip() if is_valid_data(row[13]) else None,
    #         nav_spec=row[15].strip() if is_valid_data(row[15]) else None,
    #     )
    #     session.add(proc_des_obj)
    #     if is_valid_data(data := row[2]):
    #         if data == "Y":
    #             proc_des_obj.fly_over = True
    #         elif data == "N":
    #             proc_des_obj.fly_over = False


               
    
 else:
    # extract waypoint data
    coding_df = tables[0].df
    apch_data_df = coding_df.loc[:, (coding_df != "").any(axis=0)]

    # Search for "WAYPOINT LIST" anywhere in the DataFrame
    waypoint_list_index = None
    for i, row in apch_data_df.iterrows():
        if "WAYPOINT LIST" in ' '.join(row.astype(str)):
            waypoint_list_index = i
            break

    if waypoint_list_index is not None:
        # Extract rows after the "WAYPOINT LIST" row
        apch_data_df_waypoint = apch_data_df.iloc[waypoint_list_index + 2:]
        apch_data_df_waypoint = apch_data_df_waypoint.dropna()  # Remove rows with any NaN values

        # Print the waypoint data
        for _, row in apch_data_df_waypoint.iterrows():
            for_row = [val for val in row if val != ""]  # Skip empty values

            latitude = conversionDMStoDD(for_row[2])
            longitude = conversionDMStoDD(for_row[3])
            waypoint_obj = Waypoint(
                airport_icao=AIRPORT_ICAO,
                name=for_row[0],
                type=for_row[1],
                geom=f"POINT({longitude} {latitude})",
            )
            session.add(waypoint_obj)
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

    # extract terminal holding data
    # Search for "Holding" anywhere in the DataFrame
    holding_index = None
    for i, row in apch_data_df.iterrows():
        if "Holding" in ' '.join(row.astype(str)):
            holding_index = i
            break

    if holding_index is not None:
        # Extract rows after the "Holding" row until an empty row is found
        apch_data_df_holding = apch_data_df.iloc[holding_index + 1:]
        apch_data_df_holding = apch_data_df_holding.dropna()  # Remove rows with any NaN values

        # Print the holding data until an empty row is found
        for _, row in apch_data_df_holding.iterrows():
            row_values = list(row)
            if not any(row_values):  # Check if the row is empty
                break  # Exit the loop if an empty row is found
            print(row_values)
            data_parts = row[0].split("\n")
            print(data_parts)

    # for type_, name, lat_long in zip(waypoint_types, waypoint_names, waypoint_coords):
    #     result_row = session.execute(
    #         select(Waypoint).where(
    #             Waypoint.airport_icao == AIRPORT_ICAO,
    #             Waypoint.name == name,  # TODO: Add column for type_
    #         )
    #     ).fetchone()
    #     if result_row:
    #         # Not inserting duplicate waypoints
    #         continue
    #     lat_long = re.sub(r"[^NEWS\d. ]", "", lat_long).split()
    #     lat, long = lat_long[1] + lat_long[0], lat_long[3] + lat_long[2]
    #     lat, long = conversionDMStoDD(lat), conversionDMStoDD(long)
    #     session.add(
    #         Waypoint(
    #             airport_icao=AIRPORT_ICAO,
    #             name=name,
    #             geom=f"POINT({long} {lat})",
    #         )
    #     )

    # # procedure extraction
    # df = df[df[1] != ""]  # targeting only procedure description rows
    # df = df.loc[:, (df != "").any(axis=0)]  # deleting empty columns
    # procedure_name = (
    #     re.search(r"(RNP.+)-CODING", file_name).groups()[0].replace("-", " ")
    # )
    # procedure_obj = Procedure(
    #     airport_icao=AIRPORT_ICAO,
    #     rwy_dir=rwy_dir,
    #     type="APCH",
    #     name=procedure_name,
    # )
    # session.add(procedure_obj)
    # for i, row in df.iterrows():
    #     row = list(row)
    #     if not re.match(r"\d+$", row[0]):
    #         x = row[0]
    #         if i == 1:
    #             l = re.split(r"\n\s*\n", x)
    #             row = re.split(r"\s*\n\s*", l[1])
    #             nav_spec = re.split(r"\s*\n\s*", l[0])[-1].strip()
    #             row[-1] = f"{nav_spec} " + row[-1]
    #             row.insert(0, "")
    #             row.insert(0, row.pop(-2))
    #         else:
    #             row = re.split(r"\s*\n\s*", x)
    #             course_angle = row.pop(0) + "\n" + row.pop(-2)
    #             row[-1] = row.pop(0) + " " + row[-1]
    #             row.insert(0, row.pop(-2))
    #             row.insert(4, course_angle)
    #     waypoint_obj = None
    #     if is_valid_data(data := row[2]):
    #         waypoint_obj = session.execute(
    #             select(Waypoint).where(
    #                 Waypoint.airport_icao == AIRPORT_ICAO,
    #                 Waypoint.name == data,
    #             )
    #         ).fetchone()[0]
    #     proc_des_obj = ProcedureDescription(
    #         procedure=procedure_obj,
    #         seq_num=row[0],
    #         waypoint=waypoint_obj,
    #         path_descriptor=row[3].strip(),
    #         course_angle=row[4].replace("\n", "").replace("  ", "").replace(" )", ")"),
    #         turn_dir=row[5].strip() if is_valid_data(row[5]) else None,
    #         altitude_ll=row[6].strip() if is_valid_data(row[6]) else None,
    #         speed_limit=row[7].strip() if is_valid_data(row[7]) else None,
    #         dst_time=row[8].strip() if is_valid_data(row[8]) else None,
    #         vpa_tch=row[9].strip() if is_valid_data(row[9]) else None,
    #         role_of_the_fix=row[10].strip() if is_valid_data(row[10]) else None,
    #         nav_spec=row[11].strip() if is_valid_data(row[11]) else None,
    #     )
    #     session.add(proc_des_obj)
    #     if is_valid_data(data := row[1]):
    #         if data == "Y":
    #             proc_des_obj.fly_over = True
    #         elif data == "N":
    #             proc_des_obj.fly_over = False
    
     
    
 


def main():
    file_names = os.listdir(FOLDER_PATH)
    apch_coding_file_names = []

    for file_name in file_names:
        if file_name.find("CODING") > -1:
            if file_name.find("RNP") > -1:
                apch_coding_file_names.append(file_name)

    for file_name in apch_coding_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        print(tables)
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_insert_apch(file_name, rwy_dir, tables)

       
    session.commit()
    print("Data insertion complete.")


if __name__ == "__main__":
    main()
