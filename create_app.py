import asyncio
from quart import Quart
from quart_cors import cors
from config import Config
from bouncie import BouncieAPI
from geojson import GeoJSONHandler
from waco_streets_analyzer import WacoStreetsAnalyzer
from utils import load_live_route_data, TaskManager, logger

async def create_app():
    app = cors(Quart(__name__))
    config = Config()
    app.config.from_mapping({k: v for k, v in config.dict().items() if k not in ['Config']})
    app.secret_key = config.SECRET_KEY 
    app.config['SESSION_TYPE'] = 'filesystem'

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
    app.bouncie_api = BouncieAPI()
    logger.info("BouncieAPI initialized successfully")

    # Initialize WacoStreetsAnalyzer
    app.waco_streets_analyzer = WacoStreetsAnalyzer('static/Waco-Streets.geojson')
    await app.waco_streets_analyzer.initialize()
    logger.info("WacoStreetsAnalyzer initialized successfully")
    
    # Initialize GeoJSONHandler (Pass the single BouncieAPI instance)
    app.geojson_handler = GeoJSONHandler(app.waco_streets_analyzer, app.bouncie_api)
    await app.geojson_handler.load_historical_data()
    logger.info("GeoJSONHandler initialized successfully")

    @app.before_serving
    async def startup():
        # Any additional startup tasks can go here
        pass

    return app