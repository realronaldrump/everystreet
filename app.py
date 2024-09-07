import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import logging
from logging.handlers import RotatingFileHandler
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

# Create the app synchronously
app = asyncio.run(create_app())

if __name__ == "__main__":
    logger.info("Starting application...")
    config_local = HyperConfig()

    if os.environ.get('PORT'):  # Check if running on Railway (PORT is set)
        config_local.bind = ["0.0.0.0:$PORT"]
    else:  
        config_local.bind = ["0.0.0.0:8080"]  # Use 8080 for local development
    
    config_local.workers = 1
    config_local.startup_timeout = 3600
    logger.info("Starting Hypercorn server...")
    try:
        asyncio.run(serve(app, config_local))
    except Exception as e:
        logger.error(f"Error starting Hypercorn server: {str(e)}", exc_info=True)
        raise
    finally:
        asyncio.run(app.shutdown())
    logger.info("Application has shut down.")