import asyncio
import logging
from datetime import datetime, timezone
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

    async def connect_websocket(self):
        await self.create_session()
        if self.ws is None or self.ws.closed:
            access_token = await self.client.get_access_token()
            try:
                self.ws = await self.session.ws_connect(
                    f"wss://api.bouncie.dev/v1/stream?access_token={access_token}",
                    ssl=False  # Try this if SSL verification is causing issues
                )
            except Exception as e:
                logger.error(f"Failed to connect to WebSocket: {e}")
                self.ws = None

    async def listen_for_live_data(self):
        while True:
            try:
                if self.ws:
                    async for msg in self.ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            await self.process_live_data(data)
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
                else:
                    logger.warning("WebSocket not connected. Falling back to polling.")
                    await self.poll_for_data()
            except Exception as e:
                logger.error(f"Error in WebSocket connection: {e}")
            finally:
                await asyncio.sleep(5)  # Wait before attempting to reconnect
                await self.connect_websocket()

    async def reconnect_websocket(self):
        await asyncio.sleep(5)  # Wait before attempting to reconnect
        await self.connect_websocket()
        if self.ws:
            asyncio.create_task(self.listen_for_live_data())

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
        if data['eventType'] == 'tripData':
            new_data_point = await self.data_fetcher.process_vehicle_data(data)
            if new_data_point:
                self.live_trip_data["data"].append(new_data_point)
                self.live_trip_data["last_updated"] = datetime.now(timezone.utc)

    async def get_latest_bouncie_data(self):
        try:
            vehicle_data = await self.client.get_vehicle_by_imei()
            if not vehicle_data or "stats" not in vehicle_data:
                logger.error("No vehicle data or stats found in Bouncie response")
                return None

            # Construct a data structure that matches what process_vehicle_data expects
            return {
                'eventType': 'tripData',
                'data': [{
                    'timestamp': vehicle_data['stats'].get('lastUpdated'),
                    'gps': vehicle_data['stats'].get('location', {}),
                    'speed': vehicle_data['stats'].get('speed', 0)
                }],
                'imei': self.client.device_imei
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
            trips = await self.data_fetcher.fetch_trips(access_token, self.client.device_imei, current_start, current_end)
            all_trips.extend(trips)
            current_start = current_end
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
                        "geometry": {"type": "LineString", "coordinates": coordinates},
                        "properties": {
                            "startTime": trip.get('startTime'),
                            "endTime": trip.get('endTime'),
                            "distance": trip.get('distance'),
                            "transactionId": trip.get('transactionId')
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
        asyncio.create_task(self.connect_websocket())
        asyncio.create_task(self.listen_for_live_data())