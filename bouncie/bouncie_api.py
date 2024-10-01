import asyncio
import logging
from datetime import datetime, timezone, timedelta
import aiohttp
import json
from quart import Quart, request, jsonify

from .client import BouncieClient
from .data_fetcher import DataFetcher
from .geocoder import Geocoder
from .trip_processor import TripProcessor

logger = logging.getLogger(__name__)

class BouncieAPI:
    def __init__(self, config):
        self.client = BouncieClient(
            client_id=config["CLIENT_ID"],
            client_secret=config["CLIENT_SECRET"],
            redirect_uri=config["REDIRECT_URI"],
            auth_code=config["AUTH_CODE"],
            device_imei=config["DEVICE_IMEI"],
            vehicle_id=config["VEHICLE_ID"],
        )
        self.data_fetcher = DataFetcher(self.client)
        self.geocoder = Geocoder()
        self.trip_processor = TripProcessor()
        self.live_trip_data = {
            "last_updated": datetime.now(timezone.utc),
            "data": []
        }
        self.session = None
        self.ws = None
        self.webhook_key = "672963516656223170063865111105419"
        self.webhook_url = "/webhooks/bouncie"

    async def create_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    # Comment out or remove these methods
    async def connect_websocket(self):
        pass

    async def listen_for_live_data(self):
        pass

    async def reconnect_websocket(self):
        pass

    async def poll_for_data(self):
        while True:
            try:
                data = await self.get_latest_bouncie_data()
                if data:
                    await self.process_live_data(data)
            except Exception as e:
                logger.error(f"Error polling for data: {e}")
            await asyncio.sleep(1)  # Poll every second

    async def process_live_data(self, data):
        if 'eventType' in data and data['eventType'] == 'tripData':
            new_data_point = await self.data_fetcher.process_vehicle_data(data)
            if new_data_point:
                self.live_trip_data["data"].append(new_data_point)
                self.live_trip_data["last_updated"] = datetime.now(timezone.utc)
        elif 'latitude' in data and 'longitude' in data:
            # Process data from polling
            new_data_point = {
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "timestamp": data.get("timestamp"),
                "imei": data.get("imei")
            }
            self.live_trip_data["data"].append(new_data_point)
            self.live_trip_data["last_updated"] = datetime.now(timezone.utc)
        else:
            logger.error("Data format not recognized in process_live_data")

    async def get_latest_bouncie_data(self):
        try:
            vehicle_data = await self.client.get_vehicle_by_imei()
            if not vehicle_data or "stats" not in vehicle_data:
                logger.error("No vehicle data or stats found in Bouncie response")
                return None

            # Extract latitude and longitude from vehicle_data
            location = vehicle_data["stats"].get("location", {})
            latitude = location.get("lat")
            longitude = location.get("lon")

            if latitude is None or longitude is None:
                logger.error("No latitude or longitude found in vehicle data")
                return None

            return {
                "latitude": latitude,
                "longitude": longitude,
                "timestamp": vehicle_data["stats"].get("lastUpdated"),
                "imei": self.client.device_imei
            }
        except Exception as e:
            logger.error(f"An error occurred while fetching live data: {e}")
            return None

    async def fetch_trip_data(self, start_date, end_date):
        access_token = await self.client.get_access_token()
        all_trips = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=7), end_date)
            trips = await self.data_fetcher.fetch_trips(
                access_token, self.client.device_imei, current_start, current_end
            )
            all_trips.extend(trips)
            current_start = current_end + timedelta(seconds=1)

        return all_trips

    @staticmethod
    def create_geojson_features_from_trips(trips):
        features = []
        for trip in trips:
            if 'gps' in trip and trip['gps'].get('type') == 'LineString':
                coordinates = trip['gps'].get('coordinates', [])
                if len(coordinates) >= 2:
                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": coordinates
                        },
                        "properties": {
                            "startTime": trip.get('startTime'),
                            "endTime": trip.get('endTime'),
                            "distance": trip.get('distance'),
                            "transactionId": trip.get('transactionId'),
                            # Add any other relevant properties from the trip object
                        }
                    })
        return features

    @staticmethod
    async def find_first_data_date():
        # Implement this method to find the first date with data
        # For now, we'll return a default date
        return datetime(2020, 8, 1, tzinfo=timezone.utc)

    def setup_webhook_route(self, app: Quart):
        @app.route(self.webhook_url, methods=['POST'])
        async def bouncie_webhook():
            if request.headers.get('Authorization') != self.webhook_key:
                return jsonify({"error": "Unauthorized"}), 401

            data = await request.get_json()
            await self.process_live_data(data)
            return jsonify({"status": "success"}), 200

    def start(self, app: Quart):
        self.setup_webhook_route(app)
        # Remove these lines
        # asyncio.create_task(self.connect_websocket())
        # asyncio.create_task(self.listen_for_live_data())