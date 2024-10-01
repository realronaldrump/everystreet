import logging
from datetime import datetime, timezone
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

class TripProcessor:
    @staticmethod
    def calculate_metrics(live_trip_data):
        time_since_update = datetime.now(timezone.utc) - live_trip_data.get(
            "last_updated", datetime.now(timezone.utc)
        )
        if time_since_update.total_seconds() > 45:
            live_trip_data["data"] = []

        total_distance, total_time, max_speed = 0.0, 0, 0
        start_time, end_time = None, None

        for i in range(1, len(live_trip_data["data"])):
            prev_point = live_trip_data["data"][i - 1]
            curr_point = live_trip_data["data"][i]

            total_distance += TripProcessor._calculate_distance(prev_point, curr_point)
            time_diff = curr_point["timestamp"] - prev_point["timestamp"]
            total_time += time_diff
            max_speed = max(max_speed, curr_point["speed"])

            start_time = start_time or prev_point["timestamp"]
            end_time = curr_point["timestamp"]

        return {
            "total_distance": round(total_distance, 2),
            "total_time": TripProcessor._format_time(total_time),
            "max_speed": max_speed,
            "start_time": TripProcessor._format_timestamp(start_time),
            "end_time": TripProcessor._format_timestamp(end_time),
        }

    @staticmethod
    def _calculate_distance(prev_point, curr_point):
        return geodesic(
            (prev_point["latitude"], prev_point["longitude"]),
            (curr_point["latitude"], curr_point["longitude"])
        ).miles

    @staticmethod
    def _format_time(seconds):
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    @staticmethod
    def _format_timestamp(timestamp):
        if timestamp:
            return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
        return "N/A"

    @staticmethod
    def create_geojson_features_from_trips(trips):
        features = []
        for trip in trips:
            if 'gps' in trip and trip['gps'].get('type') == 'LineString':
                coordinates = trip['gps'].get('coordinates', [])
                if len(coordinates) >= 2:
                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coordinates
                        },
                        "properties": {
                            "timestamp": trip.get('startTime'),
                            "end_timestamp": trip.get('endTime'),
                            "distance": trip.get('distance'),
                            "transactionId": trip.get('transactionId')
                        }
                    })
        return features