import os
import json
import logging
import asyncio
from datetime import datetime, timezone
import aiofiles
from dateutil import parser
import numpy as np

# Set up logger
logger = logging.getLogger(__name__)

# Constants
FEATURE_COLLECTION_TYPE = "FeatureCollection"
EPSG_4326 = "EPSG:4326"


class FileHandler:
    """
    A handler class for managing monthly geojson feature files.
    """

    @staticmethod
    async def update_monthly_files(handler, new_features):
        """
        Updates the monthly data with new features, handling duplicates based on
        timestamps.

        Args:
            handler: The GeoJSONHandler instance containing monthly data.
            new_features (list): List of new features to be added.
        """
        logger.info("Starting update with %d new features", len(new_features))
        months_to_update = set()

        for feature in new_features:
            # Validate GeoJSON feature structure
            if (
                not isinstance(feature, dict)
                or "geometry" not in feature
                or "type" not in feature["geometry"]
                or "coordinates" not in feature["geometry"]
                or "properties" not in feature
                or "timestamp" not in feature["properties"]
            ):
                logger.warning("Invalid GeoJSON feature: %s", feature)
                continue
            if feature["geometry"]["type"] != "LineString":
                logger.warning(
                    "Unsupported geometry type: %s", feature["geometry"]["type"]
                )
                continue
            if not isinstance(feature["geometry"]["coordinates"], list):
                logger.warning("Invalid coordinates: %s",
                               feature["geometry"]["coordinates"])
                continue
            # Validate coordinates
            for coord in feature["geometry"]["coordinates"]:
                if (
                    not isinstance(coord, list)
                    or len(coord) != 2
                    or not all(isinstance(c, (int, float)) for c in coord)
                ):
                    logger.warning("Invalid coordinates in feature: %s", feature)
                    continue

            feature["geometry"]["coordinates"] = FileHandler._convert_ndarray_to_list(
                feature["geometry"]["coordinates"]
            )
            timestamp = FileHandler._parse_timestamp(
                feature["properties"].get("timestamp")
            )
            if timestamp is None:
                continue

            month_year = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
                "%Y-%m"
            )

            if month_year not in handler.monthly_data:
                handler.monthly_data[month_year] = []

            if not any(
                f["properties"]["timestamp"] == timestamp
                for f in handler.monthly_data[month_year]
            ):
                handler.monthly_data[month_year].append(feature)
                months_to_update.add(month_year)

        if months_to_update:
            await FileHandler._write_updated_monthly_files(
                handler.monthly_data, months_to_update
            )
            logger.info(
                "Updated monthly files for %d months", len(months_to_update)
            )

    @staticmethod
    async def _write_updated_monthly_files(monthly_data, months_to_update):
        """
        Writes the updated monthly files based on months that need updating.

        Args:
            monthly_data (dict): Updated monthly data.
            months_to_update (set): Set of month-year strings that need to be updated.
        """
        write_tasks = [
            FileHandler._update_single_file(
                f"static/historical_data_{month_year}.geojson", monthly_data[month_year]
            )
            for month_year in months_to_update
        ]
        await asyncio.gather(*write_tasks)

    @staticmethod
    async def _update_single_file(filename, features):
        """
        Updates a single geojson file by merging existing and new features.

        Args:
            filename (str): The path to the geojson file.
            features (list): List of features to write to the file.
        """
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            existing_features = await FileHandler._load_existing_features(filename)

            all_features = FileHandler._merge_features(existing_features, features)
            await FileHandler._write_geojson_file(filename, all_features)
            logger.info(
                "Successfully wrote %d features to %s", len(all_features), filename
            )
        except Exception as e:
            logger.error(
                "Error writing to file %s: %s", filename, str(e), exc_info=True
            )

    @staticmethod
    async def _load_existing_features(filename):
        """
        Loads existing features from a geojson file, handling potential errors.

        Args:
            filename (str): Path to the file.

        Returns:
            list: List of existing features, or an empty list if file not found or
            corrupted.
        """
        try:
            async with aiofiles.open(filename, "r") as f:
                existing_data = json.loads(await f.read())
                return existing_data.get("features", [])
        except FileNotFoundError:
            logger.info("File %s not found, creating a new one", filename)
            return []
        except json.JSONDecodeError:
            logger.warning(
                "File %s is corrupted, initializing with empty features",
                filename
            )
            return []

    @staticmethod
    async def _write_geojson_file(filename, features):
        """
        Writes the given features to a geojson file.

        Args:
            filename (str): The path to the file.
            features (list): List of features to write.
        """
        async with aiofiles.open(filename, "w") as f:
            geojson_data = {
                "type": FEATURE_COLLECTION_TYPE,
                "crs": {"type": "name", "properties": {"name": EPSG_4326}},
                "features": features,
            }
            await f.write(json.dumps(geojson_data, indent=4))

    @staticmethod
    def _convert_ndarray_to_list(obj):
        """
        Recursively converts numpy arrays to Python lists.

        Args:
            obj (object): The object that may contain numpy arrays.

        Returns:
            object: A Python list if input was a numpy array, otherwise the object
            itself.
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, list):
            return [FileHandler._convert_ndarray_to_list(item) for item in obj]
        return obj

    @staticmethod
    def _merge_features(existing_features, new_features):
        """
        Merges new features with existing features, avoiding duplicates by timestamp.

        Args:
            existing_features (list): List of existing features.
            new_features (list): List of new features.

        Returns:
            list: Merged list of features.
        """
        existing_timestamps = {f["properties"]["timestamp"] for f in existing_features}
        merged_features = existing_features.copy()

        for new_feature in new_features:
            if new_feature["properties"]["timestamp"] not in existing_timestamps:
                merged_features.append(new_feature)
                existing_timestamps.add(new_feature["properties"]["timestamp"])

        return merged_features

    @staticmethod
    def _parse_timestamp(timestamp):
        """
        Parses a timestamp from either string or numeric format.

        Args:
            timestamp (str or float): The timestamp to parse.

        Returns:
            float: Parsed timestamp as a UNIX epoch, or None if invalid.
        """
        if isinstance(timestamp, str):
            try:
                return parser.isoparse(timestamp).timestamp()
            except ValueError:
                logger.error("Invalid timestamp format: %s", timestamp)
                return None
        try:
            return float(timestamp)
        except (TypeError, ValueError):
            logger.error("Invalid timestamp type: %s", timestamp)
            return None
