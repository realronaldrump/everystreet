import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from shapely.geometry import shape, Point


import aiofiles
from dateutil import parser

logger = logging.getLogger(__name__)


class FileHandler:
    async def update_monthly_files(self, handler, new_features):
        logger.info(
            f"Starting update_monthly_files with {len(new_features)} new features"
        )
        months_to_update = set()

        for feature in new_features:
            feature["geometry"]["coordinates"] = self._convert_ndarray_to_list(
                feature["geometry"]["coordinates"]
            )
            timestamp = self._parse_timestamp(feature["properties"]["timestamp"])
            if timestamp is None:
                continue

            date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            month_year = date.strftime("%Y-%m")
            if month_year not in handler.monthly_data:
                handler.monthly_data[month_year] = []

            if not any(
                existing_feature["properties"]["timestamp"] == timestamp
                for existing_feature in handler.monthly_data[month_year]
            ):
                handler.monthly_data[month_year].append(feature)
                months_to_update.add(month_year)

        await self._write_updated_monthly_files(handler, months_to_update)
        logger.info(f"Updated monthly files with {len(new_features)} new features")

    async def _write_updated_monthly_files(self, handler, months_to_update):
        write_tasks = [
            self._update_single_file(
                f"static/historical_data_{month_year}.geojson",
                handler.monthly_data[month_year],
            )
            for month_year in months_to_update
        ]
        await asyncio.gather(*write_tasks)

    async def _update_single_file(self, filename, features):
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            try:
                async with aiofiles.open(filename, "r") as f:
                    try:
                        existing_data = json.loads(await f.read())
                        existing_features = existing_data.get("features", [])
                    except json.JSONDecodeError:
                        logger.warning(
                            f"File {filename} is corrupted, initializing empty features"
                        )
                        existing_features = []
            except FileNotFoundError:
                logger.info(f"File {filename} not found, creating new file")
                existing_features = []

            all_features = self._merge_features(existing_features, features)
            async with aiofiles.open(filename, "w") as f:
                await f.write(
                    json.dumps(
                        {
                            "type": "FeatureCollection",
                            "crs": {
                                "type": "name",
                                "properties": {"name": "EPSG:4326"},
                            },
                            "features": all_features,
                        },
                        indent=4,
                    )
                )
            logger.info(
                f"Successfully wrote {len(all_features)} features to {filename}"
            )
        except Exception as e:
            logger.error(f"Error writing to file {filename}: {str(e)}", exc_info=True)
    @staticmethod
    def _convert_ndarray_to_list(obj):
        import numpy as np

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, list):
            return [FileHandler._convert_ndarray_to_list(item) for item in obj]
        else:
            return obj

    @staticmethod
    def _merge_features(existing_features, new_features):
        existing_timestamps = {
            feature["properties"]["timestamp"] for feature in existing_features
        }
        merged_features = existing_features.copy()
        for new_feature in new_features:
            if new_feature["properties"]["timestamp"] not in existing_timestamps:
                merged_features.append(new_feature)
                existing_timestamps.add(new_feature["properties"]["timestamp"])
        return merged_features

    @staticmethod
    def _parse_timestamp(timestamp):
        if isinstance(timestamp, str):
            try:
                return parser.isoparse(timestamp).timestamp()
            except ValueError:
                logger.error(f"Invalid timestamp format: {timestamp}")
                return None
        return float(timestamp)

    async def filter_historical_data(self, start_date, end_date, waco_boundary, filter_waco):
        filtered_features = []
        for month_year, features in self.monthly_data.items():
            for feature in features:
                timestamp = self._parse_timestamp(feature['properties']['timestamp'])
                if timestamp is None:
                    continue
                date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                if start_date <= date <= end_date:
                    if not filter_waco or self._is_within_waco(feature, waco_boundary):
                        filtered_features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": filtered_features
        }

    def _is_within_waco(self, feature, waco_boundary):
        # Get the Waco boundary polygon
        waco_polygon = self.waco_analyzer.get_waco_boundary(waco_boundary)
        
        # Extract coordinates from the feature
        coordinates = feature['geometry']['coordinates']
        
        # For LineString features
        if feature['geometry']['type'] == 'LineString':
            return any(waco_polygon.contains(Point(coord)) for coord in coordinates)
        
        # For Point features
        elif feature['geometry']['type'] == 'Point':
            return waco_polygon.contains(Point(coordinates))
        
        # For other geometry types, you may need to implement additional logic
        else:
            return False
