# Independent script to simulate and inspect the structure of 'point'

# Assuming this is a mockup for how data is structured
mock_trips = [
    # Each trip contains multiple points
    {
        "trip_id": 1,
        "points": [
            (32.9697, -96.80322, 50, 200, "2024-09-10T13:19:03Z"),  # Example point with 5 values
            (33.01822, -96.80191, 55, 250, "2024-09-10T13:21:03Z"),  # Another point with 5 values
            (33.01958, -96.80067, 45, 180, "2024-09-10T13:25:03Z"),  # Example point with 5 values
        ]
    },
    {
        "trip_id": 2,
        "points": [
            (32.9697, -96.80322, 50, 200, "2024-09-10T14:19:03Z", "extra_value1"),  # Point with extra values
            (33.01822, -96.80191, 55, 250, "2024-09-10T14:21:03Z"),  # Point with 5 values
        ]
    }
]

# Mockup of the method from TripProcessor class
def create_geojson_features_from_trips(trips):
    for trip in trips:
        for point in trip["points"]:
            try:
                # This is where the error occurs if there are too many values
                lat, lon, _, _, timestamp = point
                print(f"Point unpacked successfully: lat={lat}, lon={lon}, timestamp={timestamp}")
            except ValueError as e:
                # Catching the error to inspect the structure of 'point'
                print(f"Error unpacking point: {point} -> {e}")

# Running the simulation to inspect the structure of each point
create_geojson_features_from_trips(mock_trips)