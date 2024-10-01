import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp

from .geocoder import Geocoder

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, client):
        self.client = client
        self.geocoder = Geocoder()

    async def fetch_trips(self, access_token, imei, start_date, end_date):
        url = "https://api.bouncie.dev/v1/trips"
        headers = {"Authorization": access_token, "Content-Type": "application/json"}
        params = {
            "imei": imei,
            "gps-format": "geojson",
            "starts-after": start_date.isoformat(),
            "ends-before": end_date.isoformat()
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Failed to fetch trips. Status: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Error fetching trips: {e}")
            return []

    async def process_vehicle_data(self, data):
        if data['eventType'] != 'tripData':
            return None

        try:
            location = data['data'][-1]['gps']
            location_address = await self.geocoder.reverse_geocode(
                location['lat'], location['lon']
            )

            last_updated = data['data'][-1]['timestamp']
            timestamp = int(datetime.fromisoformat(last_updated.replace("Z", "+00:00")).timestamp())

            return {
                "latitude": location['lat'],
                "longitude": location['lon'],
                "timestamp": timestamp,
                "speed": data['data'][-1]['speed'],
                "device_id": data['imei'],
                "address": location_address,
            }
        except Exception as e:
            logger.error(f"Error processing vehicle data: {e}")
            return None

    async def fetch_summary_data(self, session, date):
        # This method is no longer needed with the new API structure
        # Keeping it here as a placeholder in case we need to implement something similar in the future
        pass