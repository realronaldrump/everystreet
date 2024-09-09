"""
This module creates and configures the Quart application for the EveryStreet project.
It initializes various components and sets up the necessary configurations.
"""

import os
import sys
import asyncio

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quart import Quart
from quart_cors import cors

from bouncie import BouncieAPI
from config import Config
from geojson import GeoJSONHandler
from utils import TaskManager, load_live_route_data, logger
from waco_streets_analyzer import WacoStreetsAnalyzer
from routes import register_routes
from celery_config import init_celery

async def create_app():
    """
    Asynchronously creates and configures the Quart application.

    This function initializes the Quart app, sets up CORS, configures the app
    with necessary settings, and initializes various components such as
    BouncieAPI, WacoStreetsAnalyzer, and GeoJSONHandler.

    Returns:
        Quart: The configured Quart application instance.
    """
    app = cors(Quart(__name__))
    config = Config()
    app.config.from_mapping(
        {k: v for k, v in config.dict().items() if k not in ["Config"]}
    )
    app.secret_key = config.SECRET_KEY
    app.config["SESSION_TYPE"] = "filesystem"

    # Initialize Celery
    celery = init_celery(app)
    app.celery = celery

    # Initialize app attributes
    app.historical_data_loaded = False
    app.historical_data_loading = False
    app.is_processing = False
    app.task_manager = TaskManager()
    app.live_route_data = load_live_route_data()

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

    return app