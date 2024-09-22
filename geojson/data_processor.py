import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from functools import wraps

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from date_utils import days_ago, format_date, get_end_of_day, get_start_of_day
from .file_handler import FileHandler

logger = logging.getLogger(__name__)


def log_method(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        logger.info(f"Starting {func.__name__}")
        try:
            result = await func(*args, **kwargs)
            logger.info(f"Finished {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            raise
    return wrapper

class DataProcessor:
    def __init__(self, waco_analyzer, bouncie_api, concurrency=100, start_date=datetime(2020, 8, 1, tzinfo=timezone.utc)):
        self.waco_analyzer = waco_analyzer
        self.bouncie_api = bouncie_api
        self.file_handler = FileHandler()
        self.concurrency = concurrency
        self.start_date = start_date
        self.semaphore = asyncio.Semaphore(self.concurrency)

    @log_method
    async def update_and_process_data(self, handler, fetch_all=False, start_date=None, end_date=None):
        await self.fetch_all_historical_data(handler, fetch_all, start_date, end_date)
        await self.process_routes_and_update_progress(handler)

    @log_method
    async def fetch_all_historical_data(self, handler, fetch_all=False, start_date=None, end_date=None):
        async with self.waco_analyzer.lock:
            start_date = self._get_start_date(handler, fetch_all, start_date)
            end_date = self._get_end_date(end_date)

            date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

            tasks = [self._fetch_data_for_date(date) for date in date_range]
            results = await asyncio.gather(*tasks)

            await self._process_fetched_results(handler, results)

    def _get_start_date(self, handler, fetch_all, start_date):
        if fetch_all:
            return self.start_date
        if start_date:
            return datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if handler.historical_geojson_features:
            latest_timestamp = max(
                feature["properties"]["timestamp"]
                for feature in handler.historical_geojson_features
                if feature["properties"].get("timestamp") is not None
            )
            return datetime.fromtimestamp(latest_timestamp, tz=timezone.utc) + timedelta(days=1)
        return self.bouncie_api.find_first_data_date()

    @staticmethod
    def _get_end_date(date_string, end_date):
        return datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date else datetime.now(tz=timezone.utc)

    async def _fetch_data_for_date(self, date):
        async with self.semaphore:
            try:
                logger.info(f"Fetching trips for {date.strftime('%Y-%m-%d')}")
                trips = await self.bouncie_api.fetch_trip_data(date, date)
                logger.info(f"Fetched {len(trips)} trips for {date.strftime('%Y-%m-%d')}")
                return date, trips
            except Exception as e:
                logger.error(f"Error fetching data for {date}: {str(e)}")
                return date, None

    async def _process_fetched_results(self, handler, results):
        for date, trips in results:
            if not trips:
                logger.info(f"No trips found for {date.strftime('%Y-%m-%d')}")
                continue

            new_features = self.bouncie_api.create_geojson_features_from_trips(trips)
            logger.info(f"Created {len(new_features)} new features from trips on {date}")

            if not new_features:
                continue

            unique_new_features = [
                feature for feature in new_features
                if feature["properties"]["timestamp"] not in handler.fetched_trip_timestamps
            ]

            if not unique_new_features:
                logger.info("No new unique features to add")
                continue

            await self.file_handler.update_monthly_files(handler, unique_new_features)
            handler.historical_geojson_features.extend(unique_new_features)
            handler.fetched_trip_timestamps.update(
                feature["properties"]["timestamp"] for feature in unique_new_features
            )
            logger.info(f"Added {len(unique_new_features)} new unique features to historical_geojson_features")

    @log_method
    async def process_routes_and_update_progress(self, handler):
        batch_size = 1000
        for i in range(0, len(handler.historical_geojson_features), batch_size):
            batch = handler.historical_geojson_features[i:i+batch_size]
            await self.waco_analyzer.update_progress(batch)

        progress = self.waco_analyzer.calculate_progress()
        logger.info(f"Updated progress: {progress}")
        return progress

    @staticmethod
    async def filter_features(handler, start_date, end_date, filter_waco, waco_limits, bounds=None):
        start_datetime = get_start_of_day(start_date)
        end_datetime = get_end_of_day(end_date)

        logger.info(f"Filtering features from {start_datetime} to {end_datetime}, filter_waco={filter_waco}")

        if not handler.monthly_data:
            logger.warning("No historical data loaded yet. Returning empty features.")
            return []

        filtered_features = []
        bounding_box = box(*bounds) if bounds else None

        for month_year, features in handler.monthly_data.items():
            month_start = datetime.strptime(month_year, "%Y-%m").replace(tzinfo=timezone.utc)
            month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1, tzinfo=timezone.utc) - timedelta(seconds=1)

            if month_end < start_datetime or month_start > end_datetime:
                continue

            valid_features = [f for f in features if f["geometry"]["type"] == "LineString" and len(f["geometry"]["coordinates"]) > 1]

            if not valid_features:
                logger.warning(f"No valid features found for {month_year}")
                continue

            try:
                month_features = gpd.GeoDataFrame.from_features(valid_features)
            except Exception as e:
                logger.error(f"Error creating GeoDataFrame for {month_year}: {str(e)}")
                continue

            if "timestamp" in month_features.columns:
                month_features["timestamp"] = pd.to_datetime(month_features["timestamp"], utc=True)
                mask = (month_features["timestamp"] > start_datetime) & (month_features["timestamp"] <= end_datetime)
            else:
                logger.warning(f"No 'timestamp' column found in data for {month_year}. Skipping filtering by date.")
                mask = pd.Series(True, index=month_features.index)

            if bounding_box:
                mask &= month_features.intersects(bounding_box)

            if filter_waco and waco_limits:
                mask &= month_features.intersects(waco_limits)
                clipped_features = month_features[mask].intersection(waco_limits)
            else:
                clipped_features = month_features[mask]

            filtered_features.extend(clipped_features.__geo_interface__["features"])

        logger.info(f"Filtered {len(filtered_features)} features")
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
    async def get_streets(self, handler, waco_boundary, streets_filter="all"):
        street_network = await self.waco_analyzer.get_street_network(waco_boundary)
        if street_network is None:
            logger.error("Failed to get street network")
            return json.dumps({"error": "Failed to get street network"})

        logger.info(f"Total streets before filtering: {len(street_network)}")

        if streets_filter == "traveled":
            street_network = street_network[street_network["traveled"]]
        elif streets_filter == "untraveled":
            street_network = street_network[~street_network["traveled"]]

        logger.info(f"Streets after filtering: {len(street_network)}")
        return street_network.to_json()
