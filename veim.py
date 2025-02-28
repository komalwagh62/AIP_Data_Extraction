import pdfplumber
from model import Waypoint, Procedure, ProcedureDescription, TerminalHolding, AiracData, session
from sqlalchemy import select
import camelot
import os
import re
import fitz  # PyMuPDF

AIRPORT_ICAO = "VEIM"
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
    if active_record:
        return active_record.id
    else:
        print("No active AIRAC record found.")
        return None

def is_valid_data(data):
    if not data:
        return False
    if re.match(r"(\s+|\s*-\s*)$", data):
        return False
    return True



def extract_sid_data(file_name, rwy_dir, tables):
    process_id = get_active_process_id()
    
    coding_tables = []
    waypoint_table = None

    # Classify tables as coding or waypoint tables
    for table in tables:
        df = table.df
        if df.shape[1] > 2:  # If table has more than 2 columns, it's a coding_df table
            coding_tables.append(df)
        else:  
            waypoint_table = df

    if waypoint_table is not None:
        for _, row in waypoint_table.iterrows():
            row = list(row)
            row = [x for x in row if x.strip()]  # Remove empty entries
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
            print(extracted_data1)

            if len(extracted_data1) != 4:
                continue

            lat_dir1, lat_value1, lng_dir1, lng_value1 = extracted_data1
            print(lat_dir1, lat_value1, lng_dir1, lng_value1)
            lat1 = conversionDMStoDD(lat_value1 + lat_dir1)
            lng1 = conversionDMStoDD(lng_value1 + lng_dir1)
            coordinates = f"{lat1} {lng1}"

            session.add(
                Waypoint(
                    airport_icao=AIRPORT_ICAO,
                    name=row[0].strip(),
                    coordinates_dd=coordinates,
                    geom=f"POINT({lng1} {lat1})",
                    process_id=process_id
                )
            )

    for coding_df in coding_tables:
        coding_df = coding_df.drop(index=[0,1])
        procedure_name = coding_df.iloc[0, 0].strip().replace("-", " ")

        procedure_obj = Procedure(
            airport_icao=AIRPORT_ICAO,
            rwy_dir=rwy_dir,
            type="SID",
            name=procedure_name,
            process_id=process_id
        )
        session.add(procedure_obj)
        sequence_number = 1

        for _, row in coding_df.iterrows():
            row = list(row)
            

            waypoint_obj = None
            if is_valid_data(row[2]):
                waypoint_name = row[2].strip().replace("\n", "").replace(" ", "")
                waypoint_obj = (
                    session.query(Waypoint)
                    .filter_by(airport_icao=AIRPORT_ICAO, name=waypoint_name)
                    .first()
                )

            course_angle = row[4].replace("\n", "").replace("  ", "").replace(" )", ")").replace(" Mag", "").replace(" True", "").replace(" / ", "")

            proc_des_obj = ProcedureDescription(
                procedure=procedure_obj,
                sequence_number=sequence_number,
                seq_num=row[0].strip(),
                waypoint=waypoint_obj,
                path_descriptor=row[1].strip(),
                course_angle=course_angle,
                turn_dir=row[6].strip() if len(row) > 6 and is_valid_data(row[6]) else None,
                altitude_ll=row[7].strip() if len(row) > 7 and is_valid_data(row[7]) else None,
                speed_limit=row[8].strip() if len(row) > 8 and is_valid_data(row[8]) else None,
                dst_time=row[5].strip() if len(row) > 5 and is_valid_data(row[5]) else None,
                vpa_tch=row[9].strip() if len(row) > 9 and is_valid_data(row[9]) else None,
                nav_spec=row[10].strip() if len(row) > 10 and is_valid_data(row[10]) else None,
                process_id=process_id
            )
            session.add(proc_des_obj)

            if len(row) > 3 and is_valid_data(row[3]):  
                proc_des_obj.fly_over = row[3] == "Y"

            sequence_number += 1


def main():

    file_names = os.listdir(FOLDER_PATH)
    sid_file_names = []

    for file_name in file_names:
        if file_name.find("TABLE") > -1:
            if file_name.find("SID") > -1:
                sid_file_names.append(file_name)
                        
    for file_name in sid_file_names:
        tables = camelot.read_pdf(FOLDER_PATH + file_name, pages="all")
        print(tables)
        rwy_dir = re.search(r"RWY-(\d+[A-Z]?)", file_name).groups()[0]
        extract_sid_data(file_name, rwy_dir,tables)


    session.commit()
    print("Data insertion complete.")  
   


if __name__ == "__main__":
    main()



