import logging
from datetime import datetime, timezone
from geopy.distance import geodesic
import numpy as np

logger = logging.getLogger(__name__)


class TripProcessor:
    """
    Processes trip data to calculate metrics and create GeoJSON features.
    """

    @staticmethod
    def calculate_metrics(live_trip_data):
        """
        Calculate various trip metrics like total distance, total time,
        max speed, and trip start/end times.

        Args:
            live_trip_data (dict): Contains trip data, including GPS coordinates, timestamps, and speed.

        Returns:
            dict: A dictionary containing total distance, total time, max speed, start time, and end time.
        """
        # Clear outdated trip data if the last update is too old
        time_since_update = datetime.now(timezone.utc) - live_trip_data.get(
            "last_updated", datetime.now(timezone.utc)
        )
        if time_since_update.total_seconds() > 45:
            live_trip_data["data"] = []

        total_distance, total_time, max_speed = 0.0, 0, 0
        start_time, end_time = None, None

        # Iterate through trip points and compute metrics
        for i in range(1, len(live_trip_data["data"])):
            prev_point = live_trip_data["data"][i - 1]
            curr_point = live_trip_data["data"][i]

            total_distance += TripProcessor._calculate_distance(
                prev_point, curr_point
            )
            time_diff = curr_point["timestamp"] - prev_point["timestamp"]
            total_time += time_diff
            max_speed = max(max_speed, curr_point["speed"])

            # Set start and end times
            start_time = start_time or prev_point["timestamp"]
            end_time = curr_point["timestamp"]

        metrics = {
            "total_distance": round(total_distance, 2),
            "total_time": TripProcessor._format_time(total_time),
            "max_speed": max_speed,
            "start_time": TripProcessor._format_timestamp(start_time),
            "end_time": TripProcessor._format_timestamp(end_time),
        }

        logger.info("Returning trip metrics: %s", metrics)
        return metrics

    @staticmethod
    def _calculate_distance(prev_point, curr_point):
        """
        Calculate the geodesic distance between two points.

        Args:
            prev_point (dict): The previous point with latitude and longitude.
            curr_point (dict): The current point with latitude and longitude.

        Returns:
            float: The distance in miles.
        """
        return geodesic(
            (prev_point["latitude"], prev_point["longitude"]),
            (curr_point["latitude"], curr_point["longitude"])
        ).miles

    @staticmethod
    def _format_time(seconds):
        """
        Format time in seconds into an 'HH:MM:SS' string.

        Args:
            seconds (int): Total time in seconds.

        Returns:
            str: Time formatted as 'HH:MM:SS'.
        """
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    @staticmethod
    def _format_timestamp(timestamp):
        """
        Format a timestamp into ISO format.

        Args:
            timestamp (int or None): Unix timestamp.

        Returns:
            str: Timestamp formatted in ISO 8601 format, or 'N/A' if None.
        """
        if timestamp:
            return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
        return "N/A"

    @staticmethod
    def create_geojson_features_from_trips(data):
        """
        Convert trip data into GeoJSON features.

        Args:
            data (list): A list of trips, where each trip contains GPS coordinates and timestamps.

        Returns:
            list: A list of GeoJSON features.
        """
        features = []
        logger.info("Processing %d trips", len(data))

        for trip in data:
            if not isinstance(trip, dict):
                logger.warning("Skipping invalid trip data: %s", trip)
                continue

            feature = TripProcessor._process_trip_to_geojson(trip)
            if feature:
                features.append(feature)

        logger.info("Created %d GeoJSON features from trip data", len(features))
        return features

    @staticmethod
    def _process_trip_to_geojson(trip):
        """
        Convert a single trip into a GeoJSON feature.

        Args:
            trip (dict): A dictionary containing trip data.

        Returns:
            dict or None: A GeoJSON feature or None if the trip data is invalid.
        """
        coordinates, timestamps = [], []

        for band in trip.get("bands", []):
            for path in band.get("paths", []):
                TripProcessor._process_path(path, coordinates, timestamps)

        if len(coordinates) > 1 and timestamps:
            return {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "timestamp": timestamps[0],
                    "end_timestamp": timestamps[-1],
                    "timestamps": timestamps,
                },
            }

        logger.warning(
            "Skipping trip with insufficient data: coordinates=%d, timestamps=%d",
            len(coordinates),
            len(timestamps),
        )
        return None

    @staticmethod
    def _process_path(path, coordinates, timestamps):
        """
        Process a path and extract coordinates and timestamps.

        Args:
            path (list): List of points (latitude, longitude, etc.).
            coordinates (list): List to store the coordinates.
            timestamps (list): List to store the timestamps.
        """
        path_array = np.array(path)

        # Validate if the path has at least latitude, longitude, and timestamp
        if path_array.shape[1] >= 5:
            for lat, lon, _, _, timestamp in path_array[:, [0, 1, 2, 3, 4]]:
                if timestamp is None:
                    logger.warning(
                        "Skipping point with None timestamp: %s", path)
                    continue
                try:
                    iso_timestamp = datetime.fromtimestamp(
                        timestamp, timezone.utc).isoformat()
                    coordinates.append([lon, lat])
                    timestamps.append(iso_timestamp)
                except (TypeError, ValueError) as e:
                    logger.error(
                        "Invalid timestamp %s: %s. Skipping point.", timestamp, str(e)
                    )
        else:
            logger.warning("Skipping invalid path: %s", path)
