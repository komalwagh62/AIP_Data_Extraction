import pandas as pd
from sqlalchemy import text
from model import session, Waypoint

EXCEL_FILE = r"C:\Users\LENOVO\Desktop\ANS_Register_Extraction\ans_regist\VOBL_Procedure 1.xlsx"
AIRPORT_ICAO = "VOBL"

def draw_line_between_coordinates(session, coord1, coord2):
    query = text(
        f"SELECT ST_MakeLine(ST_GeomFromText('POINT({coord1[1]} {coord1[0]})'), ST_GeomFromText('POINT({coord2[1]} {coord2[0]})'))"
    )
    result = session.execute(query)
    line_geometry = result.scalar()
    return line_geometry

def convert_coordinate(coord):
    """
    Convert coordinate from string to decimal degrees if necessary.
    This assumes coordinates are in the format 'DDMMSSN' for latitude and 'DDDMMSSW' for longitude.
    """
    if isinstance(coord, str):
        if 'N' in coord or 'S' in coord:
            degrees = int(coord[:2])
            minutes = int(coord[2:4])
            seconds = float(coord[4:-1])
            decimal = degrees + minutes / 60 + seconds / 3600
            return decimal if 'N' in coord else -decimal
        if 'E' in coord or 'W' in coord:
            degrees = int(coord[:3])
            minutes = int(coord[3:5])
            seconds = float(coord[5:-1])
            decimal = degrees + minutes / 60 + seconds / 3600
            return decimal if 'E' in coord else -decimal
    return float(coord)

def main():
    df_wpt = pd.read_excel(EXCEL_FILE, sheet_name="STAR27R")
    
    # Explicitly setting the first coordinate as previous_coordinates
    first_row = df_wpt.iloc[0]
    previous_coordinates = (convert_coordinate(first_row["Lat"]), float(first_row["Long"]))

    for index, row in df_wpt.iterrows():
        waypoint_name = row["Name"]
        lat = convert_coordinate(row["Lat"])
        lon = convert_coordinate(row["Long"])

        lon_lat = (lat, lon)

        # Inserting Waypoint into the database
        new_waypoint = Waypoint(
            airport_icao=AIRPORT_ICAO,
            name=waypoint_name,
        )

        # Adding the point geometry for the waypoint itself
        point_geometry = text(
            f"SELECT ST_GeomFromText('POINT({lon} {lat})')"
        )
        result = session.execute(point_geometry)
        point_geom = result.scalar()
        new_waypoint.geom = point_geom

        if previous_coordinates:
            # Calculate line geometry between previous coordinates and current coordinates
            line_geometry = draw_line_between_coordinates(session, previous_coordinates, lon_lat)
            new_waypoint.geom1 = line_geometry  # Assign the line geometry to geom1 column
            print(f"Line from {previous_coordinates} to {lon_lat}: {line_geometry}")

        # Adding waypoint to the session
        session.add(new_waypoint)

        # Update previous_coordinates to the current coordinates
        previous_coordinates = lon_lat

    # Committing the session to persist changes
    session.commit()

if __name__ == "__main__":
    main()
