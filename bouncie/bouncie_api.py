import logging
from datetime import datetime, timezone
from .client import BouncieClient
from .data_fetcher import DataFetcher
from .geocoder import Geocoder
from .trip_processor import TripProcessor

logger = logging.getLogger(__name__)

class BouncieAPI:
    def __init__(self):
        self.client = BouncieClient()
        self.data_fetcher = DataFetcher(self.client)
        self.geocoder = Geocoder()
        self.trip_processor = TripProcessor()
        self.live_trip_data = {"last_updated": datetime.now(timezone.utc), "data": []}

    async def get_latest_bouncie_data(self):
        try:
            await self.client.get_access_token()
            vehicle_data = await self.client.get_vehicle_by_imei()
            if not vehicle_data or "stats" not in vehicle_data:
                logger.error("No vehicle data or stats found in Bouncie response")
                return None

            new_data_point = await self.data_fetcher.process_vehicle_data(vehicle_data)
            if new_data_point:
                if self.live_trip_data["data"] and self.live_trip_data["data"][-1]["timestamp"] == new_data_point["timestamp"]:
                    logger.info("Duplicate timestamp found, not adding new data point.")
                    return None

                self.live_trip_data["data"].append(new_data_point)
                self.live_trip_data["last_updated"] = datetime.now(timezone.utc)
                return new_data_point

            return None

        except Exception as e:
            logger.error(f"An error occurred while fetching live data: {e}")
            return None

    async def get_trip_metrics(self):
        return self.trip_processor.calculate_metrics(self.live_trip_data)

    async def fetch_trip_data(self, start_date, end_date):
        return await self.data_fetcher.fetch_trip_data(start_date, end_date)

    @staticmethod
    def create_geojson_features_from_trips(data):
        return TripProcessor.create_geojson_features_from_trips(data)

    async def find_first_data_date(self):
        # Implement this method to find the first date with data
        # For now, we'll return a default date
        return datetime(2020, 8, 1, tzinfo=timezone.utc)