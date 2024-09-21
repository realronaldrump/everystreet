import os
import sys
import asyncio

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quart import Quart
from quart_cors import cors

from bouncie import BouncieAPI
from config import Config
from geojson import GeoJSONHandler
from utils import TaskManager, load_live_route_data, logger
from waco_streets_analyzer import WacoStreetsAnalyzer
from routes import register_routes

async def create_app():
    app = cors(Quart(__name__))
    config = Config()
    app.config.from_mapping(config.dict())
    app.secret_key = config.SECRET_KEY
    app.config["SESSION_TYPE"] = "filesystem"

    os.makedirs("static", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    app.historical_data_loaded = False
    app.historical_data_loading = False
    app.is_processing = False
    app.task_manager = TaskManager()
    app.live_route_data = load_live_route_data()
    app.clear_live_route = False

    app.historical_data_lock = asyncio.Lock()
    app.processing_lock = asyncio.Lock()
    app.live_route_lock = asyncio.Lock()
    app.progress_lock = asyncio.Lock()

    app.bouncie_api = BouncieAPI(config)
    logger.info("BouncieAPI initialized successfully")

    app.waco_streets_analyzer = WacoStreetsAnalyzer("static/Waco-Streets.geojson")
    await app.waco_streets_analyzer.initialize()
    logger.info("WacoStreetsAnalyzer initialized successfully")

    app.geojson_handler = GeoJSONHandler(app.waco_streets_analyzer, app.bouncie_api)
    await app.geojson_handler.load_historical_data()
    logger.info("GeoJSONHandler initialized successfully")

    logger.info("Registering routes...")
    register_routes(app)
    logger.info("Routes registered successfully")

    @app.before_serving
    async def startup():
        pass

    @app.after_request
    async def add_header(response):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    return app