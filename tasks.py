import asyncio
import logging
from datetime import datetime, timezone

from utils import save_live_route_data

logger = logging.getLogger(__name__)


async def poll_bouncie_api(app, bouncie_api):
    last_active_time = datetime.now(timezone.utc)  # Initialize last active time

    while True:
        try:
            bouncie_data = await bouncie_api.get_latest_bouncie_data()
            if bouncie_data:
                last_active_time = datetime.now(timezone.utc)  # Update last active time

                async with app.live_route_lock:
                    if "features" not in app.live_route_data:
                        app.live_route_data["features"] = []

                    if not app.live_route_data["features"]:
                        app.live_route_data["features"].append(
                            {
                                "type": "Feature",
                                "geometry": {"type": "LineString", "coordinates": []},
                                "properties": {},
                            }
                        )

                    live_route_feature = app.live_route_data["features"][0]

                    new_coord = [bouncie_data["longitude"], bouncie_data["latitude"]]

                    if (
                        not live_route_feature["geometry"]["coordinates"]
                        or new_coord
                        != live_route_feature["geometry"]["coordinates"][-1]
                    ):
                        live_route_feature["geometry"]["coordinates"].append(new_coord)
                        save_live_route_data(app.live_route_data)
                        app.latest_bouncie_data = bouncie_data
                    else:
                        logger.debug(
                            "Duplicate point detected, not adding to live route"
                        )
            else:  # No bouncie_data received
                time_since_last_update = datetime.now(timezone.utc) - last_active_time
                if time_since_last_update.total_seconds() > 600:  # 10 minutes of inactivity
                    async with app.live_route_lock:
                        app.live_route_data["features"] = []  # Clear route data
                        logger.info("Live route data cleared due to inactivity.")

            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error fetching live data: {e}", exc_info=True)
            await asyncio.sleep(5)


async def load_historical_data_background(app, geojson_handler):
    async with app.historical_data_lock:
        app.historical_data_loading = True
    try:
        logger.info("Starting historical data load")
        await geojson_handler.load_historical_data()
        async with app.historical_data_lock:
            app.historical_data_loaded = True
        logger.info("Historical data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading historical data: {str(e)}", exc_info=True)
    finally:
        async with app.historical_data_lock:
            app.historical_data_loading = False
