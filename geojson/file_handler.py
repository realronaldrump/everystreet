import os
import json
import logging
import asyncio
from datetime import datetime, timezone
import aiofiles
from dateutil import parser

logger = logging.getLogger(__name__)

FEATURE_COLLECTION_TYPE = "FeatureCollection"
EPSG_4326 = "EPSG:4326"

class FileHandler:
    @staticmethod
    async def update_monthly_files(handler, new_features):
        logger.info("Starting update with %d new features", len(new_features))
        months_to_update = set()

        for feature in new_features:
            timestamp = FileHandler._parse_timestamp(
                feature["properties"].get("startTime")
            )
            if timestamp is None:
                continue

            month_year = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
                "%Y-%m"
            )

            if month_year not in handler.monthly_data:
                handler.monthly_data[month_year] = []

            if not any(
                f["properties"]["startTime"] == feature["properties"]["startTime"]
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
        write_tasks = [
            FileHandler._update_single_file(
                f"static/historical_data_{month_year}.geojson", monthly_data[month_year]
            )
            for month_year in months_to_update
        ]
        await asyncio.gather(*write_tasks)

    @staticmethod
    async def _update_single_file(filename, features):
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
        async with aiofiles.open(filename, "w") as f:
            geojson_data = {
                "type": FEATURE_COLLECTION_TYPE,
                "crs": {"type": "name", "properties": {"name": EPSG_4326}},
                "features": features,
            }
            await f.write(json.dumps(geojson_data, indent=4))

    @staticmethod
    def _merge_features(existing_features, new_features):
        existing_timestamps = {f["properties"]["startTime"] for f in existing_features}
        merged_features = existing_features.copy()

        for new_feature in new_features:
            if new_feature["properties"]["startTime"] not in existing_timestamps:
                merged_features.append(new_feature)
                existing_timestamps.add(new_feature["properties"]["startTime"])

        return merged_features

    @staticmethod
    def _parse_timestamp(timestamp):
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