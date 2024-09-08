import logging
import asyncio
import aiohttp
from datetime import timedelta, datetime
from .geocoder import Geocoder

logger = logging.getLogger(__name__)


class DataFetcher:
    def __init__(self, client):
        self.client = client
        self.geocoder = Geocoder()

    async def fetch_summary_data(self, session, date):
        start_time = f"{date}T00:00:00-05:00"
        end_time = f"{date}T23:59:59-05:00"
        summary_url = f"https://www.bouncie.app/api/vehicles/{self.client.vehicle_id}/triplegs/details/summary?bands=true&defaultColor=%2355AEE9&overspeedColor=%23CC0000&startDate={start_time}&endDate={end_time}"

        headers = {
            "Accept": "application/json",
            "Authorization": self.client.client.access_token,
            "Content-Type": "application/json",
        }

        async with session.get(summary_url, headers=headers) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(
                    f"Error: Failed to fetch data for {date}. HTTP Status code: {response.status}"
                )
                return None

    async def fetch_trip_data(self, start_date, end_date):
        if not await self.client.get_access_token():
            return None

        date_range = [
            (start_date + timedelta(days=i))
            for i in range((end_date - start_date).days + 1)
        ]

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3600)
        ) as session:
            tasks = [
                self.fetch_summary_data(session, date.strftime("%Y-%m-%d"))
                for date in date_range
            ]
            all_trips_data = await asyncio.gather(*tasks)

        all_trips = []
        for trips_data in all_trips_data:
            if trips_data:
                all_trips.extend(trips_data)

        return all_trips

    async def process_vehicle_data(self, vehicle_data):
        stats = vehicle_data["stats"]
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
                "full"
                if bouncie_status == "normal"
                else "unplugged" if bouncie_status == "low" else "unknown"
            )

            last_updated = stats.get("lastUpdated")
            if isinstance(last_updated, str):
                timestamp = int(
                    datetime.fromisoformat(
                        last_updated.replace("Z", "+00:00")
                    ).timestamp()
                )
            elif isinstance(last_updated, (int, float)):
                timestamp = int(last_updated)
            else:
                logger.error(f"Unexpected lastUpdated format: {last_updated}")
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
