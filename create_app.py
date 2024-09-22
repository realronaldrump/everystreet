"""
This module creates and configures the Quart application for the EveryStreet project.
It initializes various components and sets up the necessary configurations.
"""
import os
import sys
import asyncio
from quart import Quart
from quart_cors import cors
from bouncie import BouncieAPI
from config import Config
from geojson import GeoJSONHandler
from utils import TaskManager, load_live_route_data, logger
from waco_streets_analyzer import WacoStreetsAnalyzer
from routes import register_routes

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def create_app():
    """
    Asynchronously creates and configures the Quart application.

    Returns:
        Quart: The configured Quart application instance.
    """
    app = cors(Quart(__name__))
    config = Config()

    app.config.from_mapping({k: v for k, v in config.dict().items() if k != "Config"})
    app.secret_key = config.SECRET_KEY
    app.config["SESSION_TYPE"] = "filesystem"

    # Create necessary directories
    os.makedirs("static", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # Initialize app attributes
    app.historical_data_loaded = False
    app.historical_data_loading = False
    app.is_processing = False
    app.task_manager = TaskManager()
    app.live_route_data = load_live_route_data()
    app.clear_live_route = False

    # Asynchronous Locks
    app.historical_data_lock = asyncio.Lock()
    app.processing_lock = asyncio.Lock()
    app.live_route_lock = asyncio.Lock()
    app.progress_lock = asyncio.Lock()

    # Initialize BouncieAPI (Single Instance)
    app.bouncie_api = BouncieAPI(app.config)
    logger.info("BouncieAPI initialized successfully")

    # Initialize WacoStreetsAnalyzer
    app.waco_streets_analyzer = WacoStreetsAnalyzer("static/Waco-Streets.geojson")
    await app.waco_streets_analyzer.initialize()
    logger.info("WacoStreetsAnalyzer initialized successfully")

    # Initialize GeoJSONHandler (Pass the single BouncieAPI instance)
    app.geojson_handler = GeoJSONHandler(app.waco_streets_analyzer, app.bouncie_api)
    await app.geojson_handler.load_historical_data()
    logger.info("GeoJSONHandler initialized successfully")

    logger.info("Registering routes...")
    register_routes(app)
    logger.info("Routes registered successfully")

    @app.before_serving
    async def startup():
        """
        Executes before the application starts serving requests.
        Any additional startup tasks can be added here.
        """
        pass

    @app.after_request
    async def add_header(response):
        response.headers['Cache-Control'] = (
            'no-store, no-cache, must-revalidate, max-age=0'
        )
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    return app
