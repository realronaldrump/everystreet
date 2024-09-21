import asyncio
import json
import logging
from collections import defaultdict
import shapely.geometry

from .data_loader import DataLoader
from .data_processor import DataProcessor
from .progress_updater import ProgressUpdater

logger = logging.getLogger(__name__)

class GeoJSONHandler:
    def __init__(self, waco_analyzer, bouncie_api):
        self.waco_analyzer = waco_analyzer
        self.data_loader = DataLoader()
        self.data_processor = DataProcessor(waco_analyzer, bouncie_api)
        self.progress_updater = ProgressUpdater(waco_analyzer)
        self.historical_geojson_features = []
        self.fetched_trip_timestamps = set()
        self.monthly_data = defaultdict(list)

    async def load_historical_data(self):
        if not self.historical_geojson_features:
            await self.data_loader.load_data(self)
            await self.update_all_progress()

    async def update_historical_data(self, fetch_all=False, start_date=None, end_date=None):
        new_data = await self.data_processor.update_and_process_data(self, fetch_all, start_date, end_date)
        self.historical_geojson_features.extend(new_data)
        await self.update_all_progress()

    async def filter_geojson_features(
        self, start_date, end_date, filter_waco, waco_limits, bounds=None
    ):
        return await self.data_processor.filter_features(
            self, start_date, end_date, filter_waco, waco_limits, bounds
        )

    async def update_all_progress(self):
        return await self.progress_updater.update_progress(self)

    def get_progress(self):
        return self.waco_analyzer.calculate_progress()

    async def get_progress_geojson(self, waco_boundary):
        return await self.waco_analyzer.get_progress_geojson(waco_boundary)

    async def get_recent_historical_data(self):
        return await self.data_processor.get_recent_data(self)

    async def get_waco_streets(self, waco_boundary, streets_filter="all"):
        return await self.data_processor.get_streets(
            self, waco_boundary, streets_filter
        )

    async def get_untraveled_streets(self, waco_boundary):
        untraveled_streets = await self.waco_analyzer.get_untraveled_streets(
            waco_boundary
        )
        return untraveled_streets.to_json()

    async def update_waco_streets_progress(self):
        return await self.progress_updater.update_streets_progress()

    def get_all_routes(self):
        logger.info(
            f"Retrieving all routes. Total features: {len(self.historical_geojson_features)}"
        )
        return self.historical_geojson_features

    async def load_waco_boundary(self, boundary_type):
        try:
            if boundary_type == "city_limits":
                file_path = "static/boundaries/city_limits.geojson"
            elif boundary_type == "less_goofy":
                file_path = "static/boundaries/less_goofy.geojson"
            elif boundary_type == "goofy":
                file_path = "static/boundaries/goofy.geojson"
            else:
                logger.warning(f"Unknown Waco boundary type: {boundary_type}")
                return None

            geojson_data = await asyncio.to_thread(self._read_json_file, file_path)
            return shapely.geometry.shape(geojson_data['features'][0]['geometry'])
        except Exception as e:
            logger.error(f"Error loading Waco boundary: {e}")
            return None

    @staticmethod
    def _read_json_file(file_path):
        with open(file_path, "r") as f:
            return json.load(f)