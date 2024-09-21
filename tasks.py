import asyncio
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

async def load_historical_data_background(app, geojson_handler):
    async with app.historical_data_lock:
        if app.historical_data_loading or app.historical_data_loaded:
            return

        app.historical_data_loading = True

    try:
        await geojson_handler.load_historical_data()
        async with app.historical_data_lock:
            app.historical_data_loaded = True
    except Exception as e:
        logger.error(f"Error loading historical data: {e}", exc_info=True)
    finally:
        async with app.historical_data_lock:
            app.historical_data_loading = False

async def poll_bouncie_api(app, bouncie_api):
    while True:
        try:
            latest_data = await bouncie_api.get_latest_bouncie_data()
            if latest_data:
                async with app.live_route_lock:
                    app.latest_bouncie_data = latest_data
                    if app.clear_live_route:
                        app.live_route_data = {"features": []}
                        app.clear_live_route = False
                    app.live_route_data["features"].append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [latest_data["longitude"], latest_data["latitude"]]
                        },
                        "properties": {
                            "timestamp": latest_data["timestamp"],
                            "speed": latest_data["speed"],
                            "address": latest_data["address"]
                        }
                    })
        except Exception as e:
            logger.error(f"Error polling Bouncie API: {e}", exc_info=True)
        await asyncio.sleep(5)  # Poll every 5 seconds