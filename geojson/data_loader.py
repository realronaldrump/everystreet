import json
import logging
import os
from typing import Dict, List, Set, Any

import aiofiles
from tqdm import tqdm

logger = logging.getLogger(__name__)


class DataLoader:
    async def load_data(self, handler: Any) -> Dict[str, Any]:
        total_features = 0
        async with handler.waco_analyzer.lock:
            if handler.historical_geojson_features:
                logger.info("Historical data already loaded.")
                return {
                    "historical_geojson_features": handler.historical_geojson_features,
                    "monthly_data": handler.monthly_data,
                    "total_features": len(handler.historical_geojson_features),
                }

            try:
                logger.info("Loading historical data from monthly files.")
                monthly_files = self._get_monthly_files()

                if monthly_files:
                    with tqdm(
                        total=len(monthly_files),
                        desc="Loading and processing historical data",
                        unit="file",
                    ) as pbar:
                        for file in monthly_files:
                            month_features, month_year = await self._process_file(
                                file, handler.fetched_trip_timestamps
                            )

                            handler.historical_geojson_features.extend(month_features)
                            handler.monthly_data[month_year] = month_features
                            total_features += len(month_features)

                            pbar.update(1)
                            pbar.set_postfix(
                                {
                                    "Total Features": total_features,
                                    "Current Month": month_year,
                                }
                            )

                    logger.info(
                        f"Loaded {total_features} features from {len(monthly_files)} monthly files"
                    )

                await handler.update_all_progress()

                return {
                    "historical_geojson_features": handler.historical_geojson_features,
                    "monthly_data": handler.monthly_data,
                    "total_features": total_features,
                }

            except Exception as e:
                logger.error(
                    f"Unexpected error loading historical data: {str(e)}", exc_info=True
                )
                raise

    @staticmethod
    def _get_monthly_files() -> List[str]:
        try:
            return [
                f
                for f in os.listdir("static")
                if f.startswith("historical_data_") and f.endswith(".geojson")
            ]
        except (FileNotFoundError, PermissionError) as e:
            logger.error("Error accessing 'static' directory: %s", str(e))
            return []

    @staticmethod
    async def _process_file(
        file: str, fetched_trip_timestamps: Set[str]
    ) -> tuple[List[Dict[str, Any]], str]:
        try:
            async with aiofiles.open(f"static/{file}", "r") as f:
                data = json.loads(await f.read())
                month_features = []

                for feature in data.get("features", []):
                    timestamp = feature["properties"].get("timestamp")
                    if timestamp and timestamp not in fetched_trip_timestamps:
                        month_features.append(feature)
                        fetched_trip_timestamps.add(timestamp)

                month_year = file.split("_")[2].split(".")[0]
                return month_features, month_year
        except Exception as e:
            logger.error("Error processing file %s: %s", file, str(e))
            return [], ""
