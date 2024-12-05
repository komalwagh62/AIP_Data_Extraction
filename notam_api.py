import requests

# Define the API endpoint and API key
api_endpoint = "https://applications.icao.int/dataservices/api/notams-realtime-list"
api_key = "de628610-0fee-46c3-b4f3-825fdf7b7feb"

# List of ICAO codes
icao_codes = [
    "VAAH", "VAAU", "VABB", "VABO", "VABP", "VABV", "VADN", "VAGD", "VAHS",
    "VAID", "VAJB", "VAJJ", "VAJL", "VAKE", "VAKP", "VAKS", "VAMA", "VANP",
    "VAOZ", "VAPO", "VAPR", "VASD", "VASU", "VAUD", "VEAH", "VEAT", "VEAY",
    "VEBI", "VEBN", "VEBD", "VEBS", "VEBU", "VECC", "VECO", "VEDG", "VEGK",
    "VEDO", "VEGT", "VEGY", "VEHO", "VEIM", "VEJR", "VEJS", "VEJH", "VEJT",
    "VEKI", "VEKO", "VELP", "VELR", "VEMN", "VEMR", "VEPT", "VEPY", "VERC",
    "VERK", "VERP", "VERU", "VETJ", "VIAG", "VIAR", "VIBR", "VIBY", "VICG",
    "VIDD", "VIDN", "VIDP", "VIDX", "VIGG", "VIGR", "VIHR", "VIJO", "VIJP",
    "VIJU", "VIKG", "VILD", "VILH", "VILK", "VIPG", "VIPT", "VISM", "VIUT",
    "VOAT", "VOBG", "VOBL", "VOBM", "VOBR", "VOBZ", "VOCB", "VOCI", "VOCL",
    "VOCP", "VOGA", "VOGB", "VOGO", "VOHB", "VOHS", "VOHY", "VOJV", "VOKN",
    "VOKU", "VOMD", "VOML", "VOMM", "VOMY", "VOPB", "VOPC", "VOPN", "VORY",
    "VOSM", "VOSH", "VOSR", "VOTK", "VOTP", "VOTR", "VOTV", "VOVZ"
]

# Split the ICAO codes into chunks of 50 (adjust if needed)
chunk_size = 25
icao_chunks = [icao_codes[i:i + chunk_size] for i in range(0, len(icao_codes), chunk_size)]

# Function to call the API
def fetch_notam_data(icao_chunk):
    locations_param = ",".join(icao_chunk)
    params = {
        "api_key": api_key,
        "format": "jsoncsv",
        "criticality": "",
        "locations": locations_param
    }
    try:
        response = requests.get(api_endpoint, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred for chunk {icao_chunk}: {e}")
        return None

# Fetch and process data for each chunk
all_data = []
for chunk in icao_chunks:
    print(f"Fetching data for ICAO chunk: {chunk}")
    data = fetch_notam_data(chunk)
    if data:
        all_data.extend(data)
        print(all_data)
# Process or save the combined data
print("Total NOTAMs fetched:", len(all_data))
