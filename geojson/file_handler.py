import os
import json
import logging
import asyncio
from datetime import datetime, timezone
import aiofiles
from dateutil import parser
import aiohttp
import time

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FEATURE_COLLECTION_TYPE = "FeatureCollection"
EPSG_4326 = "EPSG:4326"
OSRM_API_URL = "http://router.project-osrm.org/match/v1/driving/"
MAX_COORDINATES_PER_REQUEST = 100  # OSRM typically limits to 100 coordinates per request
REQUEST_RATE_LIMIT = 1  # One request per second

class FileHandler:
    """
    A handler class for managing monthly geojson feature files.
    """

    @staticmethod
    async def update_monthly_files(handler, new_features):
        """
        Updates the monthly data with new features, handling duplicates based on
        timestamps and performing map matching.

        Args:
            handler: The GeoJSONHandler instance containing monthly data.
            new_features (list): List of new features to be added.
        """
        logger.info("Starting update with %d new features", len(new_features))
        months_to_update = set()

        for feature in new_features:
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
                f["properties"]["timestamp"] == feature["properties"]["timestamp"]
                for f in handler.monthly_data[month_year]
            ):
                matched_feature = await FileHandler._map_match_feature(feature)
                handler.monthly_data[month_year].append(matched_feature)
                months_to_update.add(month_year)

        if months_to_update:
            await FileHandler._write_updated_monthly_files(
                handler.monthly_data, months_to_update
            )
            logger.info(
                "Updated monthly files for %d months", len(months_to_update)
            )

@staticmethod
async def _map_match_feature(feature):
    """
    Performs map matching on a single feature using the OSRM API.

    Args:
        feature (dict): A GeoJSON feature to be map matched.

    Returns:
        dict: The map matched GeoJSON feature.
    """
    coordinates = feature["geometry"]["coordinates"]

    # Split coordinates into chunks of MAX_COORDINATES_PER_REQUEST
    coord_chunks = [coordinates[i:i + MAX_COORDINATES_PER_REQUEST]
                    for i in range(0, len(coordinates), MAX_COORDINATES_PER_REQUEST)]

    matched_coords = []

    async def fetch_matching(session, chunk):
        """
        Fetch the map-matching result for a chunk of coordinates.
        """
        coords_str = ";".join([f"{lon},{lat}" for lon, lat in chunk])
        url = f"{OSRM_API_URL}{coords_str}?overview=full&geometries=geojson&tidy=true"
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("matchings") and len(data["matchings"]) > 0:
                        matched_chunk = data["matchings"][0]["geometry"]["coordinates"]
                        
                        # Check for unreasonable jumps in the matched route
                        if FileHandler._check_for_jumps(chunk, matched_chunk):
                            logger.warning("Detected unreasonable jump in matched route. Using original coordinates.")
                            return chunk
                        
                        logger.info(f"Successfully map matched chunk of {len(chunk)} coordinates")
                        return matched_chunk
                    else:
                        logger.warning(f"No matching found for chunk of {len(chunk)} coordinates")
                        return chunk  # Return original if no match found
                else:
                    response_text = await response.text()
                    logger.error(f"Error in map matching API call: {response.status}, Response: {response_text}")
                    return chunk  # Return original on error
        except Exception as e:
            logger.error(f"Error during map matching: {str(e)}")
            return chunk  # Return original on exception

    # Use a client session to make parallel requests
    async with aiohttp.ClientSession() as session:
        tasks = []
        for chunk in coord_chunks:
            tasks.append(fetch_matching(session, chunk))
            await asyncio.sleep(REQUEST_RATE_LIMIT)  # Rate limiting to avoid hitting API limits
        results = await asyncio.gather(*tasks)

    # Combine the matched coordinates
    for result in results:
        matched_coords.extend(result)

    # Perform a final check on the entire matched route
    if FileHandler._check_for_jumps(coordinates, matched_coords):
        logger.warning("Detected unreasonable jump in final matched route. Using original coordinates.")
        matched_coords = coordinates

    feature["geometry"]["coordinates"] = matched_coords
    return feature

@staticmethod
def _check_for_jumps(original_coords, matched_coords):
    """
    Check for unreasonable jumps in the matched coordinates.
    
    Args:
        original_coords (list): Original coordinates.
        matched_coords (list): Matched coordinates.
    
    Returns:
        bool: True if an unreasonable jump is detected, False otherwise.
    """
    if len(original_coords) != len(matched_coords):
        return True  # Mismatch in number of coordinates

    max_distance = 0.01  # Maximum allowed distance in degrees (roughly 1km)
    for (lon1, lat1), (lon2, lat2) in zip(original_coords, matched_coords):
        if abs(lon1 - lon2) > max_distance or abs(lat1 - lat2) > max_distance:
            return True  # Detected a jump larger than the threshold
    
    return False  # No unreasonable jumps detected
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
