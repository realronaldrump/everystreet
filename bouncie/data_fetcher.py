import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
from .geocoder import Geocoder

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, client):
        self.client = client
        self.geocoder = Geocoder()

    async def fetch_summary_data(self, session, date):
        start_time = date.strftime("%Y-%m-%dT00:00:00-05:00")
        end_time = date.strftime("%Y-%m-%dT23:59:59-05:00")
        summary_url = (
            f"https://www.bouncie.app/api/vehicles/{self.client.vehicle_id}/triplegs/details/summary"
            f"?bands=true&defaultColor=%2355AEE9&overspeedColor=%23CC0000"
            f"&startDate={start_time}&endDate={end_time}"
        )
        headers = {
            "Accept": "application/json",
            "Authorization": self.client.client.access_token,
            "Content-Type": "application/json",
        }
        try:
            async with session.get(summary_url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error for {date}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {date}: {e}")
        return None

    async def fetch_trip_data(self, start_date, end_date):
        if not await self.client.get_access_token():
            return None

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3600)
        ) as session:
            tasks = (
                self.fetch_summary_data(session, start_date + timedelta(days=i))
                for i in range((end_date - start_date).days + 1)
            )
            all_trips_data = await asyncio.gather(*tasks, return_exceptions=True)

        all_trips = []
        for trips_data in all_trips_data:
            if isinstance(trips_data, list):
                all_trips.extend(trips_data)
            elif isinstance(trips_data, Exception):
                logger.error(f"Error fetching trip data: {trips_data}")

        return all_trips

    async def process_vehicle_data(self, vehicle_data):
        stats = vehicle_data.get("stats", {})
        location = stats.get("location", {})
        if not location:
            logger.error("No location data found in Bouncie stats")
            return None

        try:
            location_address = await self.geocoder.reverse_geocode(
                location.get("lat"), location.get("lon")
            )
            bouncie_status = stats.get("battery", {}).get("status", "unknown")
            battery_state = (
                "full" if bouncie_status == "normal"
                else "unplugged" if bouncie_status == "low"
                else "unknown"
            )
            last_updated = stats.get("lastUpdated")
            timestamp = self._parse_timestamp(last_updated)
            if timestamp is None:
                return None

            return {
                "latitude": location.get("lat"),
                "longitude": location.get("lon"),
                "timestamp": timestamp,
                "battery_state": battery_state,
                "speed": stats.get("speed", 0),
                "device_id": self.client.device_imei,
                "address": location_address,
            }
        except Exception as e:
            logger.error(f"Error processing vehicle data: {e}")
            return None

    def _parse_timestamp(self, last_updated):
        try:
            if isinstance(last_updated, str):
                return int(datetime.fromisoformat(last_updated.replace("Z", "+00:00")).timestamp())
            elif isinstance(last_updated, (int, float)):
                return int(last_updated)
            else:
                logger.error(f"Unexpected lastUpdated format: {last_updated}")
        except Exception as e:
            logger.error(f"Error parsing timestamp: {e}")
        return None