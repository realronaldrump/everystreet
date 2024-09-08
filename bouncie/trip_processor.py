import logging
import logging
from datetime import datetime, timezone
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

class TripProcessor:
    @staticmethod
    def calculate_metrics(live_trip_data):
        current_time = datetime.now(timezone.utc)
        time_since_update = current_time - live_trip_data["last_updated"]
        
        if time_since_update.total_seconds() > 45:
            live_trip_data["data"] = []
        
        total_distance = 0
        total_time = 0
        max_speed = 0
        start_time = None
        end_time = None
        
        data = live_trip_data["data"]
        
        for i in range(1, len(data)):
            prev_point = data[i - 1]
            curr_point = data[i]
            
            distance = geodesic(
                (prev_point["latitude"], prev_point["longitude"]),
                (curr_point["latitude"], curr_point["longitude"])
            ).miles
            
            total_distance += distance
            time_diff = curr_point["timestamp"] - prev_point["timestamp"]
            total_time += time_diff
            max_speed = max(max_speed, curr_point["speed"])
            
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
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    @staticmethod
    def create_geojson_features_from_trips(data):
        features = []
        logger.info(f"Processing {len(data)} trips")
        
        for trip in data:
            if not isinstance(trip, dict):
                logger.warning(f"Skipping non-dict trip data: {trip}")
                continue
            
            coordinates = []
            timestamps = []
            
            for band in trip.get("bands", []):
                for path in band.get("paths", []):
                    if len(path[0]) >= 5:  # Check for lat, lon, timestamp at least
                        for point in path:
                            lat, lon, _, _, timestamp = point
                            try:
                                iso_timestamp = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                                coordinates.append([lon, lat])
                                timestamps.append(iso_timestamp)
                            except (TypeError, ValueError) as e:
                                logger.error(f"Invalid timestamp {timestamp}: {str(e)}. Skipping point.")
                                continue
                    else:
                        logger.warning(f"Skipping invalid path: {path}")
            
            if len(coordinates) > 1 and timestamps:
                feature = {
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coordinates},
                    "properties": {
                        "timestamp": timestamps[0],
                        "end_timestamp": timestamps[-1],
                        "timestamps": timestamps
                    },
                }
                features.append(feature)
            else:
                logger.warning(f"Skipping trip with insufficient data: coordinates={len(coordinates)}, timestamps={len(timestamps)}")
        
        logger.info(f"Created {len(features)} GeoJSON features from trip data")
        return features
from datetime import datetime, timezone
from geopy.distance import geodesic

logger = logging.getLogger(__name__)