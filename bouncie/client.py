import logging
import aiohttp

logger = logging.getLogger(__name__)

class BouncieClient:
    def __init__(self, authorization, device_imei):
        self.authorization = authorization
        self.device_imei = device_imei
        self.base_url = "https://api.bouncie.dev/v1"

    async def get_trips(self, starts_after, ends_before):
        url = f"{self.base_url}/trips"
        params = {
            "imei": self.device_imei,
            "gps-format": "geojson",
            "starts-after": starts_after,
            "ends-before": ends_before
        }
        headers = {
            "Accept": "application/json",
            "Authorization": self.authorization,
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Error fetching trips: {response.status}")
                    return None