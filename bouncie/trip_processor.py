import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class TripProcessor:
    @staticmethod
    def calculate_metrics(live_trip_data):
        time_since_update = datetime.now(timezone.utc) - live_trip_data["last_updated"]
        if time_since_update.total_seconds() > 45:
            live_trip_data["data"] = []

        total_distance = 0
        total_time = 0
        max_speed = 0
        start_time = None
        end_time = None

        for i in range(1, len(live_trip_data["data"])):
            prev_point = live_trip_data["data"][i - 1]
            curr_point = live_trip_data["data"][i]

            total_distance += curr_point["trip_data"]["distance"]
            time_diff = curr_point["timestamp"] - prev_point["timestamp"]
            total_time += time_diff
            max_speed = max(max_speed, curr_point["trip_data"]["maxSpeed"])

            if start_time is None:
                start_time = prev_point["timestamp"]
            end_time = curr_point["timestamp"]

        formatted_metrics = {
            "total_distance": round(total_distance, 2),
            "total_time": TripProcessor._format_time(total_time),
            "max_speed": max_speed,
            "start_time": datetime.fromtimestamp(start_time, timezone.utc).isoformat() if start_time else "N/A",
            "end_time": datetime.fromtimestamp(end_time, timezone.utc).isoformat() if end_time else "N/A",
        }

        logger.info(f"Returning trip metrics: {formatted_metrics}")
        return formatted_metrics

    @staticmethod
    def _format_time(seconds):
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    @staticmethod
    def create_geojson_features_from_trips(data):
        features = []
        logger.info(f"Processing {len(data)} trips")

        for trip in data:
            if not isinstance(trip, dict):
                logger.warning(f"Skipping non-dict trip data: {trip}")
                continue

            coordinates = trip['gps']['coordinates']
            
            # Remove duplicate coordinates while maintaining order
            unique_coordinates = []
            seen = set()
            for coord in coordinates:
                coord_tuple = tuple(coord)
                if coord_tuple not in seen:
                    seen.add(coord_tuple)
                    unique_coordinates.append(coord)

            if len(unique_coordinates) > 1:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": unique_coordinates
                    },
                    "properties": {
                        "transactionId": trip.get('transactionId'),
                        "startTime": trip.get('startTime'),
                        "endTime": trip.get('endTime'),
                        "distance": trip.get('distance'),
                        "averageSpeed": trip.get('averageSpeed'),
                        "maxSpeed": trip.get('maxSpeed'),
                        "hardBrakingCount": trip.get('hardBrakingCount'),
                        "hardAccelerationCount": trip.get('hardAccelerationCount'),
                        "fuelConsumed": trip.get('fuelConsumed'),
                        "totalIdleDuration": trip.get('totalIdleDuration'),
                    },
                }
                features.append(feature)
            else:
                logger.warning(f"Skipping trip with insufficient unique coordinates: {len(unique_coordinates)}")

        logger.info(f"Created {len(features)} GeoJSON features from trip data")
        return features