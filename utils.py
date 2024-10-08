import asyncio
import json
import logging
from functools import wraps
from logging.handlers import RotatingFileHandler

from geopy.geocoders import Nominatim
from quart import redirect, session, url_for

# Live Route Data File
LIVE_ROUTE_DATA_FILE = "live_route_data.geojson"

# Initialize logger
logger = logging.getLogger(__name__)


def load_live_route_data():
    try:
        with open(LIVE_ROUTE_DATA_FILE, "r") as f:
            data = json.load(f)

            # Ensure 'crs' is in the loaded data
            if "crs" not in data:
                data["crs"] = {
                    "type": "name", "properties": {
                        "name": "EPSG:4326"}}

            return data
    except FileNotFoundError:
        logger.warning(
            f"File not found: {LIVE_ROUTE_DATA_FILE}. Creating an empty GeoJSON.")
        # Default GeoJSON structure with CRS
        empty_geojson = {
            "type": "FeatureCollection",
            "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
            "features": [],
        }
        save_live_route_data(empty_geojson)
        return empty_geojson
    except json.JSONDecodeError:
        logger.error(
            f"Error decoding JSON from {LIVE_ROUTE_DATA_FILE}. File may be corrupted.")
        return {
            "type": "FeatureCollection",
            "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
            "features": [],
        }


def save_live_route_data(data):
    # Ensure 'crs' is present in the data before saving
    if "crs" not in data:
        data["crs"] = {"type": "name", "properties": {"name": "EPSG:4326"}}

    with open(LIVE_ROUTE_DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)


def login_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        return await func(*args, **kwargs)

    return wrapper


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5),
            logging.StreamHandler(),
        ],
    )


geolocator = Nominatim(user_agent="bouncie_viewer", timeout=10)


class TaskManager:
    def __init__(self):
        self.tasks = set()

    def add_task(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def cancel_all(self):
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
