import logging
import json
import os
from datetime import datetime, timezone
import aiofiles
import asyncio

logger = logging.getLogger(__name__)

class FileHandler:
    async def update_monthly_files(self, handler, new_features):
        logger.info(f"Starting update_monthly_files with {len(new_features)} new features")
        
        # Track months that had new features added
        months_to_update = set()
        
        for feature in new_features:
            feature["geometry"]["coordinates"] = self._convert_ndarray_to_list(feature["geometry"]["coordinates"])
            timestamp = feature["properties"]["timestamp"]
            date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            month_year = date.strftime("%Y-%m")

            if month_year not in handler.monthly_data:
                handler.monthly_data[month_year] = []

            if not any(existing_feature["properties"]["timestamp"] == timestamp 
                       for existing_feature in handler.monthly_data[month_year]):
                handler.monthly_data[month_year].append(feature)
                months_to_update.add(month_year)  # Track that this month needs to be updated

        # Write updated files only for months that were modified
        await self._write_updated_monthly_files(handler, months_to_update)

        logger.info(f"Updated monthly files with {len(new_features)} new features")

    async def _write_updated_monthly_files(self, handler, months_to_update):
        write_tasks = []
        for month_year in months_to_update:
            filename = f"static/historical_data_{month_year}.geojson"
            logger.info(f"Updating file: {filename}")
            write_tasks.append(self._update_single_file(filename, handler.monthly_data[month_year]))

        await asyncio.gather(*write_tasks)

    async def _update_single_file(self, filename, features):
        try:
            if os.path.exists(filename):
                async with aiofiles.open(filename, "r") as f:
                    try:
                        existing_data = json.loads(await f.read())
                        existing_features = existing_data.get("features", [])
                    except json.JSONDecodeError:
                        logger.warning(f"File {filename} is corrupted, initializing empty features")
                        existing_features = []
            else:
                existing_features = []

            all_features = self._merge_features(existing_features, features)

            os.makedirs(os.path.dirname(filename), exist_ok=True)
            async with aiofiles.open(filename, "w") as f:
                await f.write(json.dumps({
                    "type": "FeatureCollection",
                    "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
                    "features": all_features
                }, indent=4))

            logger.info(f"Successfully wrote {len(all_features)} features to {filename}")
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
        existing_timestamps = set(feature["properties"]["timestamp"] for feature in existing_features)
        merged_features = existing_features.copy()

        for new_feature in new_features:
            if new_feature["properties"]["timestamp"] not in existing_timestamps:
                merged_features.append(new_feature)
                existing_timestamps.add(new_feature["properties"]["timestamp"])

        return merged_features