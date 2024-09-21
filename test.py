import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# API endpoint and parameters
url = 'https://api.bouncie.dev/v1/trips'
headers = {
    'Accept': 'application/json',
    'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiI1ZTVmZjgxMzUxMDU2ZDFmMDBiNGYxODQiLCJjbGllbnRJZCI6InB5dGhvbi10ZXN0IiwiYXBwbGljYXRpb25JZCI6IjY2YWQzOGU5MTgwMTQ3NGVlNGJjMWY5ZiIsInNjb3BlcyI6WyJ0cmlwRGF0YSIsInZlaGljbGUiLCJsb2NhdGlvbiJdLCJpYXQiOjE3MjY5NDA3NDMsImV4cCI6MTcyNjk0NDM0M30.qzzGlUGXyUwT3MaLM3DWXY025MMWKIFC3svHba2-mYw',
    'Content-Type': 'application/json'
}

# Start date and end date (replace with your desired dates)
end_date = datetime(2024, 9, 20, 23, 15, 22)
start_date = end_date - timedelta(days=365)  # Go back one year

# List of IMEI's
imeis = ['864486065781342', '352602113969379']

# Function to fetch data for a single interval and IMEI
def fetch_trips_for_interval_imei(interval_start, interval_end, imei):
    starts_after_str = interval_start.isoformat() + 'Z'
    ends_before_str = interval_end.isoformat() + 'Z'

    params = {
        'imei': imei,
        'gps-format': 'geojson',
        'starts-after': starts_after_str,
        'ends-before': ends_before_str
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"API request failed with status code: {response.status_code} for interval {starts_after_str} to {ends_before_str}, IMEI: {imei}")
        return None

# Create a list of intervals and IMEI combinations
tasks = []
current_date = end_date
while current_date > start_date:
    interval_start = current_date - timedelta(days=6)
    for imei in imeis:
        tasks.append((interval_start, current_date, imei))
    current_date = interval_start

# Use ThreadPoolExecutor to fetch data concurrently
with ThreadPoolExecutor() as executor:
    results = list(executor.map(lambda task: fetch_trips_for_interval_imei(*task), tasks))

# Create a GeoJSON FeatureCollection and combine results
geojson = {
    "type": "FeatureCollection",
    "features": []
}

for trips in results:
    if trips:
        for trip in trips:
            if 'gps' in trip and trip['gps']['type'] == 'LineString':
                feature = {
                    "type": "Feature",
                    "geometry": trip['gps'],
                    "properties": {
                        "transactionId": trip['transactionId'],
                        "startTime": trip['startTime'],
                        # Handle missing endTime
                        "endTime": trip.get('endTime', None),  # Use get() with a default value
                        "imei": trip['imei'], 
                        # Add other properties as needed
                    }
                }
                geojson["features"].append(feature)

# Write the GeoJSON to a file
with open('trips_all_imeis.geojson', 'w') as f:
    json.dump(geojson, f)

print("GeoJSON file created successfully: trips_all_imeis.geojson") 