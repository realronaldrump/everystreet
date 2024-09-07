import logging
import json
from datetime import datetime, timedelta, timezone
import asyncio
import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from .file_handler import FileHandler
from date_utils import get_start_of_day, get_end_of_day, days_ago, format_date  # Import from date_utils

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, waco_analyzer, bouncie_api):  # Accept bouncie_api as a parameter
        self.waco_analyzer = waco_analyzer
        self.bouncie_api = bouncie_api  # Store the passed bouncie_api instance
        self.file_handler = FileHandler()
        self.concurrency = 100  # Adjust this value based on your needs

    async def update_and_process_data(self, handler, fetch_all=False):
        try:
            logger.info("Starting update_and_process_data")
            
            # Step 1: Fetch all historical data
            await self.fetch_all_historical_data(handler, fetch_all)

            # Step 2: Process routes and update Waco streets progress
            await self.process_routes_and_update_progress(handler)

            logger.info("Finished update_and_process_data")
        except Exception as e:
            logger.error(f"An error occurred during data update and processing: {str(e)}", exc_info=True)
            raise

    async def fetch_all_historical_data(self, handler, fetch_all=False):
        async with handler.waco_analyzer.lock:
            try:
                logger.info("Starting fetch_all_historical_data")

                if fetch_all:
                    start_date = datetime(2020, 8, 1, tzinfo=timezone.utc)
                    logger.info(f"Fetching all data starting from {start_date}")
                elif handler.historical_geojson_features:
                    latest_timestamp = max(
                        feature["properties"]["timestamp"]
                        for feature in handler.historical_geojson_features
                        if feature["properties"].get("timestamp") is not None
                    )
                    start_date = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc) + timedelta(days=1)
                    logger.info(f"Fetching data starting from the latest timestamp: {start_date}")
                else:
                    start_date = await self.bouncie_api.find_first_data_date()
                    logger.info(f"No historical features loaded, fetching data starting from first data date: {start_date}")

                end_date = datetime.now(tz=timezone.utc)
                logger.info(f"Fetching data until {end_date}")

                semaphore = asyncio.Semaphore(self.concurrency)
                date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

                async def fetch_data_for_date(date):
                    async with semaphore:
                        try:
                            logger.info(f"Fetching trips for {date.strftime('%Y-%m-%d')}")
                            trips = await self.bouncie_api.fetch_trip_data(date, date)
                            logger.info(f"Fetched {len(trips)} trips for {date.strftime('%Y-%m-%d')}")
                            return date, trips
                        except Exception as e:
                            logger.error(f"Error fetching data for {date}: {str(e)}")
                            return date, None

                tasks = [fetch_data_for_date(date) for date in date_range]
                results = await asyncio.gather(*tasks)

                for date, trips in results:
                    if trips:
                        new_features = self.bouncie_api.create_geojson_features_from_trips(trips)
                        logger.info(f"Created {len(new_features)} new features from trips on {date}")

                        if new_features:
                            unique_new_features = [
                                feature for feature in new_features
                                if feature["properties"]["timestamp"] not in handler.fetched_trip_timestamps
                            ]
                            
                            if unique_new_features:
                                await self.file_handler.update_monthly_files(handler, unique_new_features)
                                handler.historical_geojson_features.extend(unique_new_features)
                                handler.fetched_trip_timestamps.update(
                                    feature["properties"]["timestamp"] for feature in unique_new_features
                                )
                                logger.info(f"Added {len(unique_new_features)} new unique features to historical_geojson_features")
                            else:
                                logger.info("No new unique features to add")
                    else:
                        logger.info(f"No trips found for {date.strftime('%Y-%m-%d')}")

                logger.info("Finished fetch_all_historical_data")
            except Exception as e:
                logger.error(f"An error occurred during historical data fetch: {str(e)}", exc_info=True)
                raise

    async def process_routes_and_update_progress(self, handler):
        try:
            logger.info("Starting process_routes_and_update_progress")

            # Process all routes
            for feature in handler.historical_geojson_features:
                await self.waco_analyzer.update_progress([feature])

            # Calculate final progress
            progress = self.waco_analyzer.calculate_progress()
            logger.info(f"Updated progress: {progress}")

            logger.info("Finished process_routes_and_update_progress")
            return progress
        except Exception as e:
            logger.error(f"Error in process_routes_and_update_progress: {str(e)}", exc_info=True)
            raise

    async def filter_features(self, handler, start_date, end_date, filter_waco, waco_limits, bounds=None):
        # Use get_start_of_day and get_end_of_day from date_utils
        start_datetime = get_start_of_day(start_date)
        end_datetime = get_end_of_day(end_date)

        logger.info(f"Filtering features from {start_datetime} to {end_datetime}, filter_waco={filter_waco}")

        filtered_features = []

        if bounds:
            bounding_box = box(*bounds)

        if not handler.monthly_data:
            logger.warning("No historical data loaded yet. Returning empty features.")
            return filtered_features

        for month_year, features in handler.monthly_data.items():
            month_start = datetime.strptime(month_year, "%Y-%m").replace(tzinfo=timezone.utc)
            month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1, tzinfo=timezone.utc) - timedelta(seconds=1)

            if month_start <= end_datetime and month_end >= start_datetime:
                valid_features = []
                for feature in features:
                    if feature['geometry']['type'] == 'LineString' and len(feature['geometry']['coordinates']) > 1:
                        valid_features.append(feature)
                    else:
                        logger.warning(f"Skipping invalid feature in {month_year}: {feature}")

                if not valid_features:
                    logger.warning(f"No valid features found for {month_year}")
                    continue

                try:
                    month_features = gpd.GeoDataFrame.from_features(valid_features)
                except Exception as e:
                    logger.error(f"Error creating GeoDataFrame for {month_year}: {str(e)}")
                    continue

                if 'timestamp' in month_features.columns:
                    month_features['timestamp'] = pd.to_datetime(month_features['timestamp'], utc=True)
                    mask = (month_features['timestamp'] >= start_datetime) & (month_features['timestamp'] <= end_datetime)
                else:
                    logger.warning(f"No 'timestamp' column found in data for {month_year}. Skipping filtering by date.")
                    mask = pd.Series(True, index=month_features.index)

                if bounds:
                    mask &= month_features.intersects(bounding_box)

                if filter_waco and waco_limits:
                    mask &= month_features.intersects(waco_limits)
                    clipped_features = month_features[mask].intersection(waco_limits)
                else:
                    clipped_features = month_features[mask]

                filtered_features.extend(clipped_features.__geo_interface__['features'])

        logger.info(f"Filtered {len(filtered_features)} features")
        return filtered_features

    async def get_recent_data(self, handler):
        try:
            # Use days_ago and format_date from date_utils
            yesterday = days_ago(1)
            filtered_features = await self.filter_features(
                handler,
                format_date(yesterday),
                format_date(datetime.now(timezone.utc)),
                filter_waco=False,
                waco_limits=None,
            )
            return filtered_features
        except Exception as e:
            logger.error(f"Error in get_recent_historical_data: {str(e)}", exc_info=True)
            return []

    async def get_streets(self, handler, waco_boundary, streets_filter='all'):
        try:
            logger.info(f"Getting Waco streets: boundary={waco_boundary}, filter={streets_filter}")
            street_network = await self.waco_analyzer.get_street_network(waco_boundary)
            if street_network is None:
                logger.error("Failed to get street network")
                return json.dumps({"error": "Failed to get street network"})

            logger.info(f"Total streets before filtering: {len(street_network)}")

            if streets_filter == 'traveled':
                street_network = street_network[street_network['traveled']]
            elif streets_filter == 'untraveled':
                street_network = street_network[~street_network['traveled']]

            logger.info(f"Streets after filtering: {len(street_network)}")
            return street_network.to_json()
        except Exception as e:
            logger.error(f"Error in get_waco_streets: {str(e)}", exc_info=True)
            return json.dumps({"error": str(e)})