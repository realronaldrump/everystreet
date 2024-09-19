import asyncio
import logging

from geopy.geocoders import Nominatim

logger = logging.getLogger(__name__)


class Geocoder:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="bouncie_viewer", timeout=10)

    async def reverse_geocode(self, lat, lon, retries=3):
        for attempt in range(retries):
            try:
                location = await asyncio.to_thread(
                    self.geolocator.reverse, (lat, lon), addressdetails=True
                )
                if location:
                    address = location.raw["address"]
                    formatted_address = f"{address.get('place', '')}<br>"
                    formatted_address += f"{address.get('building', '')}<br>"
                    formatted_address += f"{address.get('house_number', '')} {address.get('road', '')}<br>"
                    formatted_address += f"{address.get('city', '')}, {address.get('state', '')} {address.get('postcode', '')}"

                    logger.debug("Geocoding successful: %s", formatted_address)
                    return formatted_address.strip("<br>")
                return "N/A"  # Only return "N/A" if location is not found
            except Exception as e:
                logger.error(
                    "Reverse geocoding attempt %d failed with error: %s", attempt + 1, e
                )
                if attempt < retries - 1:
                    await asyncio.sleep(1)
        return "N/A"
