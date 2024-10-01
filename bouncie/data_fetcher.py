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
        start_time = f"{date}T00:00:00-05:00"
        end_time = f"{date}T23:59:59-05:00"
        summary_url = (
            "https://www.bouncie.app/api/vehicles/"
            f"{self.client.vehicle_id}/triplegs/details/summary?"
            "bands=true&defaultColor=%2355AEE9&overspeedColor=%23CC0000&"
            f"startDate={start_time}&endDate={end_time}"
        )

        headers = {
            "Accept": "application/json",
            "Authorization": self.client.client.access_token,
            "Content-Type": "application/json",
        }

        async with session.get(summary_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                # Validate the structure of the response data
                if not isinstance(data, list):
                    logger.error(
                        "Invalid data format received from Bouncie API")
                    return None
                return data
            logger.error(
                "Error: Failed to fetch data for %s. HTTP Status code: %s",
                date,
                response.status,
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
                logger.error("Unexpected lastUpdated format: %s", last_updated)
                return None

            # Validate latitude and longitude
            latitude = location.get("lat")
            longitude = location.get("lon")
            if not isinstance(latitude, (int, float)
                              ) or not (-90 <= latitude <= 90):
                raise ValueError("Invalid latitude value")
            if not isinstance(longitude, (int, float)) or not (
                -180 <= longitude <= 180
            ):
                raise ValueError("Invalid longitude value")

            return {
                "latitude": latitude,
                "longitude": longitude,
                "timestamp": timestamp,
                "battery_state": battery_state,
                "speed": stats.get("speed", 0),
                "device_id": self.client.device_imei,
                "address": location_address,
            }
        except Exception as e:
            logger.error("Error processing vehicle data: %s", e)
            return None
