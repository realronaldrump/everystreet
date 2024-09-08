"""
This module is the entry point for the EveryStreet application.
It sets up logging and runs the Quart application using Hypercorn.
"""

import os
import sys
import asyncio
import logging

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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
    """
    Creates and runs the Quart application using Hypercorn.
    """
    logger.info("Creating app...")
    app = await create_app()
    logger.info("App created successfully")

    config_local = HyperConfig()
    config_local.bind = ["0.0.0.0:8080"]
    config_local.workers = 1
    config_local.startup_timeout = 3600
    logger.info("Starting Hypercorn server...")
    try:
        await serve(app, config_local)
    except Exception as e:
        logger.error(f"Error starting Hypercorn server: {e}", exc_info=True)
        raise
    finally:
        await app.shutdown()


if __name__ == "__main__":
    logger.info("Starting application...")
    asyncio.run(run_app())
    logger.info("Application has shut down.")