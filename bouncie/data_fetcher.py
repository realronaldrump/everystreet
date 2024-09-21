import asyncio
import logging
from datetime import datetime, timedelta

from .geocoder import Geocoder

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, client):
        self.client = client
        self.geocoder = Geocoder()

    async def fetch_trip_data(self, start_date, end_date):
        if not isinstance(start_date, datetime):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if not isinstance(end_date, datetime):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))

        date_range = [
            (start_date + timedelta(days=i))
            for i in range((end_date - start_date).days + 1)
        ]

        all_trips = []
        for date in date_range:
            trips_data = await self.client.get_trips(
                starts_after=date.isoformat(),
                ends_before=(date + timedelta(days=1)).isoformat()
            )
            if trips_data:
                all_trips.extend(trips_data)

        return all_trips

    async def process_vehicle_data(self, vehicle_data):
        if not vehicle_data:
            logger.error("No vehicle data provided")
            return None

        try:
            last_trip = vehicle_data[-1]  # Assume the last trip is the most recent
            last_coordinate = last_trip['gps']['coordinates'][-1]  # Last coordinate of the last trip

            location_address = await self.geocoder.reverse_geocode(
                last_coordinate[1], last_coordinate[0]
            )

            return {
                "latitude": last_coordinate[1],
                "longitude": last_coordinate[0],
                "timestamp": int(datetime.fromisoformat(last_trip['endTime'].replace('Z', '+00:00')).timestamp()),
                "speed": last_trip['averageSpeed'],
                "device_id": self.client.device_imei,
                "address": location_address,
                "trip_data": last_trip  # Include the full trip data
            }
        except Exception as e:
            logger.error(f"Error processing vehicle data: {e}")
            return None