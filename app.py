import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import logging
from geojson import GeoJSONHandler
from waco_streets_analyzer import WacoStreetsAnalyzer
from logging.handlers import RotatingFileHandler
from hypercorn.asyncio import serve
from hypercorn.config import Config as HyperConfig
from bouncie import BouncieAPI
from create_app import create_app
from utils import setup_logging

# Set up logging
LOG_DIRECTORY = "logs"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
log_file = os.path.join(LOG_DIRECTORY, "app.log")

setup_logging(log_file)
logger = logging.getLogger(__name__)

app = create_app()

# Import routes after app creation to avoid circular import
from routes import register_routes
register_routes(app)

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    multiprocessing.set_start_method('spawn')

    async def run_app():
        config_local = HyperConfig()
        config_local.bind = ["0.0.0.0:8080"]
        config_local.workers = 1
        config_local.startup_timeout = 36000
        logger.info("Starting Hypercorn server...")
        try:
            await serve(app, config_local)
        except Exception as e:
            logger.error(f"Error starting Hypercorn server: {str(e)}", exc_info=True)
            raise
        finally:
            await app.shutdown()

    logger.info("Starting application...")
    asyncio.run(run_app())
    logger.info("Application has shut down.")