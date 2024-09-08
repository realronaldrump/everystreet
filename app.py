import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import logging
from hypercorn.asyncio import serve
from hypercorn.config import Config as HyperConfig
from create_app import create_app
from utils import setup_logging

# Set up logging
LOG_DIRECTORY = "logs"
os.makedirs(LOG_DIRECTORY, exist_ok=True)
log_file = os.path.join(LOG_DIRECTORY, "app.log")

setup_logging(log_file)
logger = logging.getLogger(__name__)


async def run_app():
    logger.info("Creating app...")
    app = await create_app()
    logger.info("App created successfully")

    logger.info("Registering routes...")
    from routes import register_routes

    register_routes(app)
    logger.info("Routes registered successfully")

    config_local = HyperConfig()
    config_local.bind = ["0.0.0.0:8080"]
    config_local.workers = 1
    config_local.startup_timeout = 3600
    logger.info("Starting Hypercorn server...")
    try:
        await serve(app, config_local)
    except Exception as e:
        logger.error(f"Error starting Hypercorn server: {str(e)}", exc_info=True)
        raise
    finally:
        await app.shutdown()


if __name__ == "__main__":
    logger.info("Starting application...")
    asyncio.run(run_app())
    logger.info("Application has shut down.")
