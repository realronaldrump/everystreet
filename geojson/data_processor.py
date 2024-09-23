import logging
import asyncio
import json
from functools import wraps
from datetime import datetime, timedelta, timezone

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from date_utils import get_start_of_day, get_end_of_day, format_date, days_ago
from .file_handler import FileHandler

logger = logging.getLogger(__name__)


def log_method(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        logger.info("Starting %s", func.__name__)
        try:
            result = await func(*args, **kwargs)
            logger.info("Finished %s", func.__name__)
            return result
        except Exception as e:
            logger.error("Error in %s: %s", func.__name__, str(e), exc_info=True)
            raise
    return wrapper


class DataProcessor:
    def __init__(self, waco_analyzer, bouncie_api, concurrency=100,
                 start_date=datetime(2020, 8, 1, tzinfo=timezone.utc)):
        self.waco_analyzer = waco_analyzer
        self.bouncie_api = bouncie_api
        self.file_handler = FileHandler()
        self.concurrency = concurrency
        self.start_date = start_date
        self.semaphore = asyncio.Semaphore(self.concurrency)

    @log_method
    async def update_and_process_data(self, handler, fetch_all=False,
                                      start_date=None, end_date=None):
        await self.fetch_all_historical_data(handler, fetch_all, start_date,
                                             end_date)
        await self.process_routes_and_update_progress(handler)

    @log_method
    async def fetch_all_historical_data(self, handler, fetch_all=False,
                                        start_date=None, end_date=None):
        async with self.waco_analyzer.lock:
            start_date = self._get_start_date(handler, fetch_all, start_date)
            end_date = self._get_end_date(end_date)

            date_range = [start_date + timedelta(days=i)
                          for i in range((end_date - start_date).days + 1)]

            tasks = [self._fetch_data_for_date(date) for date in date_range]
            results = await asyncio.gather(*tasks)

            await self._process_fetched_results(handler, results)

    def _get_start_date(self, handler, fetch_all, start_date):
        if fetch_all:
            return self.start_date
        if start_date:
            return datetime.strptime(start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        if handler.historical_geojson_features:
            latest_timestamp = max(
                feature["properties"]["timestamp"]
                for feature in handler.historical_geojson_features
                if feature["properties"].get("timestamp") is not None
            )
            return datetime.fromtimestamp(
                latest_timestamp, tz=timezone.utc
            ) + timedelta(days=1)
        return self.bouncie_api.find_first_data_date()

    @staticmethod
    def _get_end_date(end_date):
        return (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if end_date
            else datetime.now(tz=timezone.utc)
        )

    async def _fetch_data_for_date(self, date):
        async with self.semaphore:
            try:
                logger.info("Fetching trips for %s",
                            date.strftime("%Y-%m-%d"))
                trips = await self.bouncie_api.fetch_trip_data(date, date)
                logger.info("Fetched %d trips for %s", len(trips),
                            date.strftime("%Y-%m-%d"))
                return date, trips
            except Exception as e:
                logger.error("Error fetching data for %s: %s", date, str(e))
                return date, None

    async def _process_fetched_results(self, handler, results):
        for date, trips in results:
            if not trips:
                logger.info("No trips found for %s",
                            date.strftime("%Y-%m-%d"))
                continue

            new_features = self.bouncie_api.create_geojson_features_from_trips(
                trips
            )
            logger.info("Created %d new features from trips on %s",
                        len(new_features), date)

            if not new_features:
                continue

            unique_new_features = [
                feature for feature in new_features
                if feature["properties"]["timestamp"]
                not in handler.fetched_trip_timestamps
            ]

            if not unique_new_features:
                logger.info("No new unique features to add")
                continue

            await self.file_handler.update_monthly_files(
                handler, unique_new_features
            )
            handler.historical_geojson_features.extend(unique_new_features)
            handler.fetched_trip_timestamps.update(
                feature["properties"]["timestamp"]
                for feature in unique_new_features
            )
            logger.info(
                "Added %d new unique features to historical_geojson_features",
                len(unique_new_features)
            )

    @log_method
    async def process_routes_and_update_progress(self, handler):
        batch_size = 1000
        for i in range(0, len(handler.historical_geojson_features),
                       batch_size):
            batch = handler.historical_geojson_features[i:i + batch_size]
            await self.waco_analyzer.update_progress(batch)

        progress = self.waco_analyzer.calculate_progress()
        logger.info("Updated progress: %s", progress)
        return progress

    @staticmethod
    async def filter_features(handler, start_date, end_date, filter_waco,
                              waco_limits, bounds=None):
        start_datetime = get_start_of_day(start_date)
        end_datetime = get_end_of_day(end_date)

        logger.info(
            "Filtering features from %s to %s, filter_waco=%s",
            start_datetime, end_datetime, filter_waco
        )

        if not handler.monthly_data:
            logger.warning(
                "No historical data loaded yet. Returning empty features."
            )
            return []

        filtered_features = []
        bounding_box = box(*bounds) if bounds else None

        for month_year, features in handler.monthly_data.items():
            month_start = datetime.strptime(month_year, "%Y-%m").replace(
                tzinfo=timezone.utc
            )
            month_end = (
                month_start.replace(day=28) + timedelta(days=4)
            ).replace(day=1, tzinfo=timezone.utc) - timedelta(seconds=1)

            if month_end < start_datetime or month_start > end_datetime:
                continue

            valid_features = []
            for feature in features:
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
                if feature["geometry"]["type"] not in [
                    "LineString", "MultiLineString"
                ]:
                    logger.warning(
                        "Unsupported geometry type: %s",
                        feature["geometry"]["type"]
                    )
                    continue
                if not isinstance(feature["geometry"]["coordinates"], list):
                    logger.warning(
                        "Invalid coordinates: %s",
                        feature["geometry"]["coordinates"]
                    )
                    continue
                # Validate coordinates
                if feature["geometry"]["type"] == "LineString":
                    if len(feature["geometry"]["coordinates"]) <= 1:
                        logger.warning(
                            "LineString with less than 2 coordinates: %s",
                            feature
                        )
                        continue
                    for coord in feature["geometry"]["coordinates"]:
                        if (
                            not isinstance(coord, list)
                            or len(coord) != 2
                            or not all(isinstance(c, (int, float))
                                       for c in coord)
                        ):
                            logger.warning(
                                "Invalid coordinates in LineString: %s",
                                feature
                            )
                            continue
                elif feature["geometry"]["type"] == "MultiLineString":
                    for linestring in feature["geometry"]["coordinates"]:
                        if len(linestring) <= 1:
                            logger.warning(
                                "LineString with less than 2 coordinates "
                                "in MultiLineString: %s",
                                feature
                            )
                            continue
                        for coord in linestring:
                            if (
                                not isinstance(coord, list)
                                or len(coord) != 2
                                or not all(isinstance(c, (int, float))
                                           for c in coord)
                            ):
                                logger.warning(
                                    "Invalid coordinates in "
                                    "MultiLineString: %s",
                                    feature
                                )
                                continue
                valid_features.append(feature)

            if not valid_features:
                logger.warning("No valid features found for %s", month_year)
                continue

            try:
                month_features = gpd.GeoDataFrame.from_features(
                    valid_features
                )
            except Exception as e:
                logger.error(
                    "Error creating GeoDataFrame for %s: %s",
                    month_year, str(e)
                )
                continue

            if "timestamp" in month_features.columns:
                month_features["timestamp"] = pd.to_datetime(
                    month_features["timestamp"], utc=True
                )
                mask = (
                    (month_features["timestamp"] > start_datetime)
                    & (month_features["timestamp"] <= end_datetime)
                )
            else:
                logger.warning(
                    "No 'timestamp' column found in data for %s. "
                    "Skipping filtering by date.",
                    month_year
                )
                mask = pd.Series(True, index=month_features.index)

            if bounding_box:
                mask &= month_features.intersects(bounding_box)

            filtered_mask = mask.copy()  # Create a copy of the mask

            if filter_waco and waco_limits:
                filtered_mask &= month_features.intersects(waco_limits)  # Use the copy
                clipped_features = month_features[filtered_mask].intersection(waco_limits)
            else:
                clipped_features = month_features[mask]

            # Iterate over the GeoSeries items
            for index, geometry in clipped_features.items():
                geometry_type = geometry.geom_type
                if geometry_type == "LineString":
                    coordinates = list(geometry.coords)
                elif geometry_type == "MultiLineString":
                    coordinates = [list(line.coords) for line in geometry.geoms]
                else:
                    logger.warning(
                        "Unsupported geometry type: %s", geometry_type
                    )
                    continue

                # Access other properties from the original GeoDataFrame using the index
                row = month_features.loc[index]

                filtered_features.append(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": geometry_type,
                            "coordinates": coordinates,
                        },
                        "properties": {
                            "timestamp": row.timestamp.isoformat()
                            if row.timestamp is not pd.NaT
                            else None,
                        },
                    }
                )

        logger.info("Filtered %d features", len(filtered_features))
        return filtered_features

    @log_method
    async def get_recent_data(self, handler):
        yesterday = days_ago(1)
        return await self.filter_features(
            handler,
            format_date(yesterday),
            format_date(datetime.now(timezone.utc)),
            filter_waco=False,
            waco_limits=None,
        )

    @log_method
    async def get_streets(self, handler, waco_boundary,
                          streets_filter="all"):
        street_network = await self.waco_analyzer.get_street_network(
            waco_boundary
        )
        if street_network is None:
            logger.error("Failed to get street network")
            return json.dumps({"error": "Failed to get street network"})

        logger.info("Total streets before filtering: %d",
                    len(street_network))

        if streets_filter == "traveled":
            street_network = street_network[street_network["traveled"]]
        elif streets_filter == "untraveled":
            street_network = street_network[~street_network["traveled"]]

        logger.info("Streets after filtering: %d", len(street_network))
        return street_network.to_json()