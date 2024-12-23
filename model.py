import uuid
from sqlalchemy import (
    JSON,
    UUID,
    VARCHAR,
    create_engine,
    Column,
    Integer,
    String,
    ForeignKey,
    Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from geoalchemy2 import Geometry
from geoalchemy2.types import Geometry as GeoAlchemyGeometry
from geoalchemy2.types import Geometry
from geoalchemy2 import Geometry


# Define the database connection string
db_connection = "postgresql://obstacle_database:obst1234@localhost:5432/aip_data"
engine = create_engine(db_connection, echo=False)

# Create a session class
Session = sessionmaker(engine)
# Create a session
session = Session()
Base = declarative_base()

# airac data
class AiracData(Base):
    __tablename__ = "airac_data"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    process_name = Column(VARCHAR(500), nullable=False)
    created_At = Column(VARCHAR(500), nullable=False)
    valid_from = Column(VARCHAR(500), nullable=False)
    status = Column(Boolean, default=False)
    


# Waypoint Data Extarct from PDFs
class Waypoint(Base):
    __tablename__ = "waypoint"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(VARCHAR(4), nullable=False)
    name = Column(VARCHAR(50), nullable=False)
    coordinates_dd = Column(VARCHAR(500), nullable=False)
    geom = Column(Geometry(geometry_type="POINT"), nullable=False)
    type = Column(VARCHAR(255) ,nullable=True)
    navaid = Column(VARCHAR(50) ,nullable=True)
    # year = Column(VARCHAR(255),nullable=True)
    # supp_number = Column(VARCHAR(1000), nullable=True)
    process_id = Column(VARCHAR(500), nullable=False) 
    # geom1 = Column(Geometry("LINESTRING", srid=4326))

# Procedure Data Extarct from PDFs
class Procedure(Base):
    __tablename__ = "procedure"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(VARCHAR(4), nullable=False)
    rwy_dir = Column(VARCHAR(20), nullable=False)
    name = Column(VARCHAR(50), nullable=False)
    designator = Column(VARCHAR(20), nullable=True)
    type = Column(VARCHAR(50), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 

# Procedure Description Data Extarct from PDFs
class ProcedureDescription(Base):
    __tablename__ = "procedure_description"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    procedure_id = Column(ForeignKey("procedure.id"))
    sequence_number = Column(Integer, nullable=False)
    seq_num = Column(VARCHAR(255), nullable=True)
    waypoint_id = Column(ForeignKey("waypoint.id"))
    path_descriptor = Column(VARCHAR(50), nullable=True)
    fly_over = Column(Boolean, default=False)
    course_angle = Column(VARCHAR(255), nullable=True)
    turn_dir = Column(VARCHAR(50), nullable=True)
    altitude_ul = Column(VARCHAR(50), nullable=True)
    altitude_ll = Column(VARCHAR(50), nullable=True)
    speed_limit = Column(VARCHAR(70), nullable=True)
    dst_time = Column(VARCHAR(50), nullable=True)
    vpa_tch = Column(VARCHAR(50), nullable=True)
    nav_spec = Column(VARCHAR(50), nullable=True)
    role_of_the_fix = Column(VARCHAR(20), nullable=True)
    iap_transition = Column(VARCHAR(50), nullable=True)
    mag_var = Column(VARCHAR(10), nullable=True)
    # year = Column(VARCHAR(255),nullable=True)
    # supp_number = Column(VARCHAR(255),nullable=True)
    process_id = Column(VARCHAR(500), nullable=False) 


    procedure = relationship("Procedure")
    waypoint = relationship("Waypoint")

# TerminalHoldings Data Extarct from PDFs
class TerminalHolding(Base):
    __tablename__ = "terminal_holding"
    id = Column(Integer, primary_key=True)  # Add primary key
    waypoint_id = Column(ForeignKey("waypoint.id"))
    path_descriptor = Column(VARCHAR(10), nullable=True)
    fly_over = Column(Boolean, default=False)
    course_angle = Column(VARCHAR(50), nullable=True)
    turn_dir = Column(VARCHAR(10), nullable=True)
    altitude_ul = Column(VARCHAR(20), nullable=True)
    altitude_ll = Column(VARCHAR(20), nullable=True)
    speed_limit = Column(VARCHAR(30), nullable=True)
    dst_time = Column(VARCHAR(10), nullable=True)
    vpa_tch = Column(VARCHAR(10), nullable=True)
    nav_spec = Column(VARCHAR(50))
    # process_id = Column(VARCHAR(500), nullable=False) 

    waypoint = relationship("Waypoint")
    
    
# Aerodrome Obstacle Data Extarct from PDFs
class Aerodrome_Obstacle(Base):
    __tablename__ = "aerodrome_obstacle"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(5),nullable=False)
    area_affected = Column(String(1000), nullable=False)
    obstacle_type = Column(String(100), nullable=False)
    coordinates_dd = Column(VARCHAR(500), nullable=False)
    geom = Column(Geometry(geometry_type="POINT"), nullable=False)
    elevation = Column(String(100), nullable=False)
    marking_lgt = Column(String(100), nullable=False)  
    remarks = Column(String(1000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
    
# Navaids Data from AD 2.19
class Navaids(Base):
    __tablename__ = "navaids"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(5), nullable=False)
    navaid_information = Column(String(50),nullable=False)
    identification = Column(String(50),nullable=False)
    frequency_and_channel = Column(String(100),nullable=False)
    hours_of_operation = Column(String(100),nullable=False)
    coordinates_dd = Column(VARCHAR(500), nullable=False)
    geom = Column(Geometry(geometry_type="POINT"), nullable=False)
    elevation = Column(String(500),nullable=False)
    service_volume_radius = Column(String(500),nullable=False)
    remarks = Column(String(500),nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
      

#  Waypoint Data from ENR 4.4 geosjon
class SignificantPoints(Base):
    __tablename__ = 'significantpoints'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    waypoints = Column(String(255), nullable=False)
    coordinates_dd = Column(VARCHAR(500), nullable=False)
    geom = Column(Geometry(geometry_type="POINT"), nullable=False)
    name_of_routes = Column(String(255))
    type = Column(String(255))
    process_id = Column(VARCHAR(500), nullable=False) 

#  Route Data from ENR 3.1 & 3.2 geosjon
class Route(Base):
    __tablename__ = 'route'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airway_name=Column(String(45), nullable=False)
    route_desginator = Column(String(45), nullable=False)
    rnp_type = Column(String(255), nullable=False)
    start_point = Column(String(255), nullable=False)
    end_point = Column(String(255), nullable=False)
    remarks = Column(String(2000))
    type = Column(String(255))
    process_id = Column(VARCHAR(500), nullable=False) 

#  Line Segment Data from ENR 3.1 & 3.2 geosjon
class LineSegment(Base):
    __tablename__ = 'linesegment'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name_of_significant_point = Column(String(255))
    route_name = Column(String(255))
    geom = Column(Geometry("LINESTRING", srid=4326))
    track_magnetic = Column(String(255)) 
    reverse_magnetic = Column(String(255))
    radial_distance = Column(String(255))
    upper_limit = Column(String(255))
    lower_limit = Column(String(255))
    airspace = Column(String(255))
    mea = Column(String(50))
    lateral_limits = Column(String(255))
    direction_of_cruising_levels = Column(String(255))
    route_id =Column(ForeignKey("route.id"))
    process_id = Column(VARCHAR(500), nullable=False) 
    
    routes = relationship("Route")
   # J1ZasnIPyZ7CGAgUP88S
# Conventional AWYs Data from ENR 3.1 & 3.2 geosjon  
class ConvLineData(Base):
    __tablename__ = 'convlinedata'
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    airway_id = Column(String(255)) 
    start_point = Column(String(255))
    start_point_geom = Column(String(255))
    end_point = Column(String(255)) 
    end_point_geom = Column(String(255))
    track_magnetic = Column(String(255)) 
    reverse_magnetic = Column(String(255))
    radial_distance = Column(String(255))
    upper_limit = Column(String(255))
    lower_limit = Column(String(255))
    airspace = Column(String(255))
    mea = Column(String(50))
    lateral_limits = Column(String(255))
    direction_of_cruising_levels = Column(String(255))
    geomcolumn = Column(Geometry)
    type = Column(VARCHAR(4), nullable=False)
    remarks = Column(String(2000))
    process_id = Column(VARCHAR(500), nullable=False) 
    
#  Non Conventional AWYs  Data from ENR 3.1 & 3.2 geosjon
class nonConvLineData(Base):
    __tablename__ = 'nonconvlinedata'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airway_id = Column(String(255)) 
    rnp_type = Column(String(500))
    start_point = Column(String(255))
    start_point_geom = Column(String(255))  # Change Geometry type to POINT
    end_point = Column(String(255)) 
    end_point_geom = Column(String(255))  # Change Geometry type to POINT
    track_magnetic = Column(String(255)) 
    reverse_magnetic = Column(String(255))
    radial_distance = Column(String(255))
    upper_limit = Column(String(255))
    lower_limit = Column(String(255))
    airspace = Column(String(255))
    mea = Column(String(50))
    lateral_limits = Column(String(255))
    direction_of_cruising_levels = Column(String(255))
    geomcolumn = Column(Geometry)
    type = Column(VARCHAR(10), nullable=False)
    remarks = Column(String(2000))
    process_id = Column(VARCHAR(500), nullable=False) 
    
# Airport Data Extract from Aerodromes AD
class AirportData(Base):
    __tablename__ = 'airportdata'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ICAOCode = Column(String(10), nullable=False)
    airport_name = Column(String(500), nullable=False)
    coordinate = Column(String(500), nullable=False)
    coordinates_dd = Column(VARCHAR(500), nullable=False)
    geom = Column(Geometry(geometry_type="POINT"), nullable=False)
    distance = Column(String(500), nullable=False)
    aerodrome_elevation =Column(String(500), nullable=False)
    magnetic_variation = Column(String(500), nullable=False)
    city = Column(String(500), nullable=False)
    IATACode = Column(String(500), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
    
# Runway data from AD
class RunwayCharacterstics(Base):
    __tablename__ = 'runway_characterstics'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(5), nullable=False)
    designation = Column(String(255), nullable=False)
    true_bearing = Column(String(255), nullable=False)
    dimensions_of_rwy = Column(String(255), nullable=False)
    strength_pavement = Column(String(255), nullable=False)
    associated_data = Column(String(255), nullable=False)
    surface_of_runway = Column(String(255), nullable=False)
    associated_stopways = Column(String(255), nullable=False)
    coordinates_geom_threshold_dd = Column(VARCHAR(500), nullable=False)
    geom_threshold = Column(Geometry(geometry_type="POINT")) 
    coordinates_geom_runway_end_dd = Column(String(500), nullable=False)
    geom_runway_end = Column(Geometry(geometry_type="POINT"))
    thr_elevation = Column(String(255), nullable=False)
    tdz_of_precision = Column(String(255), nullable=False)
    slope_of_runway = Column(String(255), nullable=False)
    dimension_of_stopway = Column(String(255), nullable=False)
    dimension_of_clearway = Column(String(255), nullable=False)
    dimension_of_strips = Column(String(255), nullable=False)
    dimension_of_runway = Column(String(255), nullable=False)
    location = Column(String(255), nullable=False)
    description_of_arresting_system = Column(String(255), nullable=False)
    existence_of_obstacle = Column(String(255), nullable=False)
    remarks = Column(String(1000), nullable=False)  # Increased length
    process_id = Column(VARCHAR(500), nullable=False) 

# Declared Distance data from AD
class DeclaredDistances(Base):
    __tablename__ = 'declared_distances'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(5), nullable=False)
    rwy_designator = Column(String(255), nullable=False)
    tora = Column(String(255), nullable=False)
    toda = Column(String(255), nullable=False)
    asda = Column(String(255), nullable=False)
    lda = Column(String(255), nullable=False)
    remarks = Column(String(1000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
    
# Approach & Runway Lighting data from AD
class ApproachAndRunwayLighting(Base):
    __tablename__ = 'approach_and_runway_lighting'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(255))
    runway_desginator = Column(String(255))
    type_of_approach_lighting_system = Column(String(255))
    length_of_approach_lighting_system =Column(String(255))
    intensity_of_approach_lighting_system =Column(String(255))
    runway_threshold_lights = Column(String(255))
    runway_threshold__colour =Column(String(255))
    runway_threshold_wing_bars = Column(String(255))
    type_of_visual_slope_indicator = Column(String(255))
    length_of_runway_touchdown_zone_lights = Column(String(255))
    length_of_runway_centeral_line_lights = Column(String(255))
    spacing_of_runway_centeral_line_lights = Column(String(255))
    colour_of_runway_centeral_line_lights  =Column(String(255))
    intensity_of_runway_centeral_line_lights = Column(String(255))
    length_of_runway_edge_lights = Column(String(255))
    spacing_of_runway_edge_lights = Column(String(255))
    colour_of_runway_edge_lights  =Column(String(255))
    intensity_of_runway_edge_lights = Column(String(255))
    colour_of_runway_end_lights = Column(String(255))
    wing_bar = Column(String(255))
    length_of_stopway_lights = Column(String(255))
    colour_of_stopway_lights = Column(String(255))
    remarks = Column(String(2000))
    process_id = Column(VARCHAR(500), nullable=False) 
    
# AirTrafficServiceAirspace data from AD
class AirTrafficServiceAirspace(Base):
    __tablename__ = 'air_traffic_service_airspace'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(5), nullable=False)
    airspace_designation = Column(String(255), nullable=False)
    geographical_coordinates = Column(String(1500), nullable=False)
    vertical_limits = Column(String(255), nullable=False)
    airspace_classification = Column(String(255), nullable=False)
    call_sign = Column(String(255), nullable=False)
    language_of_air_traffic_service = Column(String(255), nullable=False)
    transition_altitude = Column(String(255), nullable=False)
    hours_of_applicability = Column(String(255), nullable=False)
    remarks = Column(String(2000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
    
# AirTrafficServicesCommunicationFacilities data from AD
class AirTrafficServicesCommunicationFacilities(Base):
    __tablename__ = 'air_traffic_service_communication_facilities'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    airport_icao = Column(String(5), nullable=False)
    service_designation = Column(String(255), nullable=False)
    call_sign = Column(String(255), nullable=False)
    channel = Column(String(255), nullable=False)
    satvoice_number = Column(String(255), nullable=False)
    logon_address = Column(String(255), nullable=False)
    hours_of_operation = Column(String(255), nullable=False)
    remarks = Column(String(2000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
    
# FlightInformationRegion data from ENR 2.1
class FlightInformationRegion(Base):
    __tablename__ = 'flight_information_region'
    id = Column(Integer, primary_key=True, autoincrement=True)
    fir_name = Column(String(255), nullable=False)
    lateral_limits = Column(String(2000), nullable=False)
    vertical_limits = Column(String(2000), nullable=True)  # Allow NULL values
    unit_process_service = Column(String(255), nullable=False)
    call_sign = Column(String(255), nullable=False)
    language = Column(String(255), nullable=False)
    area = Column(String(255), nullable=False)
    frequency = Column(String(255), nullable=False)
    remarks = Column(String(2000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 
    
# ControlledAirspace data from ENR 2.1 (TMA,MTR,CTR)
class ControlAirspace(Base):
    __tablename__ = 'controlairspace'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    icao_code = Column(String(255), nullable=False)
    type = Column(String(255), nullable=False)
    AirspaceClassification = Column(String(255), nullable=False)
    lateral_limits = Column(String(2000), nullable=False)
    vertical_limits = Column(String(2000), nullable=True)  # Allow NULL values
    unit_process_service = Column(String(255), nullable=False)
    call_sign = Column(String(255), nullable=False)
    language = Column(String(255), nullable=False)
    area = Column(String(255), nullable=False)
    frequency = Column(String(255), nullable=False)
    geom = Column(String(50000), nullable=False)
    geometry = Column(Geometry)
    remarks = Column(String(2000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 

# Controlled Data geosjon
class Controlled(Base):
    __tablename__ = 'controlled'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    type= Column(String)
    geometry = Column(String, nullable=False) 
    geom = Column(Geometry("MultiPolygon"), nullable=False) 
     
# RestrictedAirspace data from ENR 5.1 & 5.2
class RestrictedAreas(Base):
    __tablename__ = 'restricted_areas'
    id = Column(Integer, primary_key=True, autoincrement=True)
    identification = Column(String(500), nullable=False)
    code = Column(String(255), nullable=False)
    restrictive_type = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    icao_code = Column(String(255), nullable=False)
    region = Column(String(255), nullable=False)
    lateral_limits = Column(String(5000), nullable=False)
    upper_limits = Column(String(500), nullable=False)
    lower_limits = Column(String(500), nullable=False)
    geom = Column(String(50000), nullable=False)
    geometry = Column(Geometry)
    remarks = Column(String(2000), nullable=False)
    process_id = Column(VARCHAR(500), nullable=False) 

# Controlled Data from geosjon
class Restricted(Base):
    __tablename__ = 'restricted'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    Identi_No = Column(String) 
    name = Column(String)
    type= Column(String)
    geometry = Column(String, nullable=False) 
    geom = Column(Geometry("MultiPolygon"), nullable=False) 
     

    
# Create the tables in the database
Base.metadata.create_all(engine)
