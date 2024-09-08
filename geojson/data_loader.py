import json
import logging
import os
from tqdm import tqdm
import aiofiles

logger = logging.getLogger(__name__)


class DataLoader:
    async def load_data(self, handler):
        async with handler.waco_analyzer.lock:
            if handler.historical_geojson_features:
                logger.info("Historical data already loaded.")
                return {
                    "historical_geojson_features": handler.historical_geojson_features,
                    "monthly_data": handler.monthly_data,
                    "total_features": total_features,
                }

            try:
                logger.info("Loading historical data from monthly files.")
                monthly_files = [
                    f
                    for f in os.listdir("static")
                    if f.startswith("historical_data_") and f.endswith(".geojson")
                ]

                total_features = 0
                if monthly_files:
                    with tqdm(
                        total=len(monthly_files),
                        desc="Loading and processing historical data",
                        unit="file",
                    ) as pbar:
                        for file in monthly_files:
                            async with aiofiles.open(f"static/{file}", "r") as f:
                                data = json.loads(await f.read())
                                month_features = data.get("features", [])

                                for feature in month_features:
                                    timestamp = feature["properties"]["timestamp"]
                                    if timestamp not in handler.fetched_trip_timestamps:
                                        handler.historical_geojson_features.append(
                                            feature
                                        )
                                        handler.fetched_trip_timestamps.add(timestamp)

                                month_year = file.split("_")[2].split(".")[0]
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

            except Exception as e:
                logger.error(
                    f"Unexpected error loading historical data: {str(e)}", exc_info=True
                )
                raise Exception(f"Error loading historical data: {str(e)}")
