# This Python module, `routes.py`, is designed for a Quart web application. It defines various asynchronous routes and WebSocket endpoints for managing and interacting with geographical and historical data, specifically focusing on the Waco area. Key functionalities include fetching and filtering historical data, managing live route data, searching locations, and handling user authentication. The module also manages application startup and shutdown processes, ensuring proper task management and API client session handling. Caching is used to optimize data retrieval, and error handling is implemented throughout to ensure robust operation.
import os
import asyncio
import json
import logging
from datetime import date, datetime, timezone, time

from cachetools import TTLCache
from quart import (jsonify, redirect, render_template, request,
                   session, url_for, websocket, make_response)

from config import Config
from date_utils import format_date, timedelta
from models import DateRange, HistoricalDataParams
from tasks import load_historical_data_background, poll_bouncie_api
from utils import geolocator, login_required
from functools import wraps

logger = logging.getLogger(__name__)

config = Config(
    PIN=os.environ.get('PIN', ''),
    CLIENT_ID=os.environ.get('CLIENT_ID', ''),
    CLIENT_SECRET=os.environ.get('CLIENT_SECRET', ''),
    REDIRECT_URI=os.environ.get('REDIRECT_URI', ''),
    AUTH_CODE=os.environ.get('AUTH_CODE', ''),
    VEHICLE_ID=os.environ.get('VEHICLE_ID', ''),
    DEVICE_IMEI=os.environ.get('DEVICE_IMEI', ''),
    GOOGLE_MAPS_API=os.environ.get('GOOGLE_MAPS_API', ''),
    USERNAME=os.environ.get('USERNAME', ''),
    PASSWORD=os.environ.get('PASSWORD', ''),
    SECRET_KEY=os.environ.get('SECRET_KEY', '')
)

cache: TTLCache = TTLCache(maxsize=100, ttl=3600)


def no_cache(view_function):
    @wraps(view_function)
    async def no_cache_impl(*args, **kwargs):
        response = await make_response(await view_function(*args, **kwargs))
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response
    return no_cache_impl


def register_routes(app):
    waco_analyzer = app.waco_streets_analyzer
    geojson_handler = app.geojson_handler
    bouncie_api = app.bouncie_api

    @app.route("/progress")
    async def get_progress():
        async with app.progress_lock:
            try:
                coverage_analysis = await geojson_handler.update_waco_streets_progress()
                if coverage_analysis is None:
                    raise ValueError("Failed to update Waco streets progress")
                logging.info(f"Progress update: {coverage_analysis}")
                return jsonify(
                    {
                        "total_streets": int(coverage_analysis["total_streets"]),
                        "traveled_streets": int(coverage_analysis["traveled_streets"]),
                        "coverage_percentage": float(
                            coverage_analysis["coverage_percentage"]
                        ),
                    }
                )
            except Exception as e:
                logging.error(f"Error in get_progress: {str(e)}", exc_info=True)
                return jsonify({"error": str(e)}), 500

    @app.route("/filtered_historical_data")
    async def get_filtered_historical_data():
        try:
            params = HistoricalDataParams(
                date_range=DateRange(
                    start_date=request.args.get("startDate") or "2020-01-01",
                    end_date=request.args.get("endDate")
                    or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                ),
                filter_waco=request.args.get("filterWaco", "false").lower() == "true",
                waco_boundary=request.args.get("wacoBoundary", "city_limits"),
                bounds=(
                    [float(x) for x in request.args.get("bounds", "").split(",")]
                    if request.args.get("bounds")
                    else None
                ),
            )
            logger.info(f"Received request for filtered historical data: {params}")
            waco_limits = None
            if params.filter_waco and params.waco_boundary != "none":
                waco_limits = await geojson_handler.load_waco_boundary(
                    params.waco_boundary
                )
            filtered_features = await geojson_handler.filter_geojson_features(
                params.date_range.start_date.isoformat(),
                params.date_range.end_date.isoformat(),
                params.filter_waco,
                waco_limits,
                bounds=params.bounds,
            )
            result = {
                "type": "FeatureCollection",
                "features": filtered_features,
                "total_features": len(filtered_features),
            }
            return jsonify(result)
        except ValueError as e:
            logger.error(f"Error parsing parameters: {str(e)}")
            return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
        except Exception as e:
            logger.error(f"Error filtering historical data: {str(e)}", exc_info=True)
            return jsonify({"error": f"Error filtering historical data: {str(e)}"}), 500

    @app.route("/waco_streets")
    async def get_waco_streets():
        try:
            waco_boundary = request.args.get("wacoBoundary", "city_limits")
            streets_filter = request.args.get("filter", "all")
            cache_key = f"waco_streets_{waco_boundary}_{streets_filter}"
            if cache_key in cache:
                return jsonify(cache[cache_key])
            logging.info(
                f"Fetching Waco streets: boundary={waco_boundary}, filter={streets_filter}"
            )
            streets_geojson = await geojson_handler.get_waco_streets(
                waco_boundary, streets_filter
            )
            streets_data = json.loads(streets_geojson)
            if "features" not in streets_data:
                raise ValueError("Invalid GeoJSON: 'features' key not found")
            cache[cache_key] = streets_data
            logging.info(f"Returning {len(streets_data['features'])} street features")
            return jsonify(streets_data)
        except Exception as e:
            logging.error(f"Error in get_waco_streets: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/update_progress", methods=["POST"])
    async def update_progress():
        async with app.progress_lock:
            try:
                coverage_analysis = await geojson_handler.update_all_progress()
                return (
                    jsonify(
                        {
                            "total_streets": int(coverage_analysis["total_streets"]),
                            "traveled_streets": int(
                                coverage_analysis["traveled_streets"]
                            ),
                            "coverage_percentage": float(
                                coverage_analysis["coverage_percentage"]
                            ),
                        }
                    ),
                    200,
                )
            except Exception as e:
                logger.error(f"Error updating progress: {str(e)}", exc_info=True)
                return jsonify({"error": f"Error updating progress: {str(e)}"}), 500

    @app.route("/untraveled_streets")
    async def get_untraveled_streets():
        waco_boundary = request.args.get("wacoBoundary", "city_limits")
        untraveled_streets = await geojson_handler.get_untraveled_streets(waco_boundary)
        return jsonify(json.loads(untraveled_streets))

    @app.route("/latest_bouncie_data")
    async def get_latest_bouncie_data():
        async with app.live_route_lock:
            return jsonify(getattr(app, "latest_bouncie_data", {}))

    @app.websocket("/ws/live_route")
    async def ws_live_route():
        try:
            last_sent_time = 0  # Initialize to track the last time data was sent

            while True:
                current_time = time()  # Get the current timestamp

                # Calculate the time difference since the last update
                time_diff = current_time - last_sent_time

                if time_diff >= 1:  # Check if at least 1 second has passed
                    async with app.live_route_lock:
                        data = app.live_route_data

                    # Send the live route data to the client over the WebSocket connection
                    await websocket.send(json.dumps(data))

                    # Update the last_sent_time to the current time
                    last_sent_time = current_time

                # Wait a small amount of time before the next check, e.g., 1 second
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Handle WebSocket disconnection
            pass

    @app.route("/historical_data_status")
    async def historical_data_status():
        async with app.historical_data_lock:
            return jsonify(
                {
                    "loaded": app.historical_data_loaded,
                    "loading": app.historical_data_loading,
                }
            )

    @app.websocket("/ws/trip_metrics")
    async def ws_trip_metrics():
        try: # Import time function
            last_sent_time = 0  # Initialize to track the last time data was sent

            while True:
                current_time = time()  # Get the current timestamp

                # Calculate the time difference since the last update
                time_diff = current_time - last_sent_time

                if time_diff >= 1:  # Check if at least 1 seconds have passed
                    async with app.live_route_lock:
                        formatted_metrics = await app.bouncie_api.get_trip_metrics()

                    # Send the trip metrics to the client over the WebSocket connection
                    await websocket.send(json.dumps(formatted_metrics))

                    # Update the last_sent_time to the current time
                    last_sent_time = current_time

                # Wait a small amount of time before the next check, e.g., 1 second
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Handle WebSocket disconnection
            pass

    @app.route("/search_location")
    async def search_location():
        query = request.args.get("query")
        if not query:
            return jsonify({"error": "No search query provided"}), 400
        try:
            location = await asyncio.to_thread(geolocator.geocode, query)
            if location:
                return jsonify(
                    {
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                        "address": location.address,
                    }
                )
            return jsonify({"error": "Location not found"}), 404
        except Exception as e:
            logger.error(f"Error during location search: {e}")
            return jsonify({"error": "An error occurred during the search"}), 500

    @app.route("/search_suggestions")
    async def search_suggestions():
        query = request.args.get("query")
        if not query:
            return jsonify({"error": "No search query provided"}), 400
        try:
            locations = await asyncio.to_thread(
                geolocator.geocode, query, exactly_one=False, limit=5
            )
            if locations:
                suggestions = [{"address": location.address} for location in locations]
                return jsonify(suggestions)
            return jsonify([])
        except Exception as e:
            logger.error(f"Error during location search: {e}")
            return jsonify({"error": "An error occurred during the search"}), 500

    @app.route("/update_historical_data", methods=["POST"])
    async def update_historical_data():
        async with app.processing_lock:
            if app.is_processing:
                return jsonify({"error": "Another process is already running"}), 429
            try:
                app.is_processing = True
                logger.info("Starting historical data update process")

                data = await request.get_json()
                start_date = data.get('startDate')
                end_date = data.get('endDate')
                
                await geojson_handler.update_historical_data(fetch_all=False, start_date=start_date, end_date=end_date)
                logger.info("Historical data update process completed")
                return jsonify({"message": "Historical data updated successfully!"}), 200
            except Exception as e:
                logger.error(f"An error occurred during the update process: {e}")
                return jsonify({"error": f"An error occurred: {str(e)}"}), 500
            finally:
                app.is_processing = False

    @app.route("/progress_geojson")
    async def get_progress_geojson():
        try:
            waco_boundary = request.args.get("wacoBoundary", "city_limits")
            progress_geojson = await geojson_handler.get_progress_geojson(waco_boundary)
            return jsonify(progress_geojson)
        except Exception as e:
            logger.error(f"Error getting progress GeoJSON: {str(e)}", exc_info=True)
            return jsonify({"error": f"Error getting progress GeoJSON: {str(e)}"}), 500

    @app.route("/processing_status")
    async def processing_status():
        async with app.processing_lock:
            return jsonify({"isProcessing": app.is_processing})

    @app.route("/reset_progress", methods=["POST"])
    @login_required
    async def reset_progress():
        async with app.processing_lock:
            if app.is_processing:
                return jsonify({"error": "Another process is already running"}), 429
            try:
                app.is_processing = True
                logger.info("Starting progress reset process")
                # Reset the progress in the WacoStreetsAnalyzer
                await waco_analyzer.reset_progress()
                # Recalculate the progress using all historical data
                await geojson_handler.update_all_progress()
                logger.info("Progress reset and recalculated successfully")
                return (
                    jsonify(
                        {
                            "message": "Progress has been reset and recalculated successfully!"
                        }
                    ),
                    200,
                )
            except Exception as e:
                logger.error(
                    f"An error occurred during the progress reset process: {e}"
                )
                return jsonify({"error": f"An error occurred: {str(e)}"}), 500
            finally:
                app.is_processing = False

    @app.route("/historical_data")
    async def get_historical_data():
        try:
            start_date = request.args.get("startDate")
            end_date = request.args.get("endDate")
            filter_waco = request.args.get("filterWaco", "false").lower() == "true"
            waco_boundary = request.args.get("wacoBoundary", "city_limits")
            logger.info(
                f"Fetching historical data for: {start_date} to {end_date}, filterWaco: {filter_waco}, wacoBoundary: {waco_boundary}"
            )
            waco_limits = None
            if filter_waco:
                waco_limits = await geojson_handler.load_waco_boundary(
                    waco_boundary
                )  # Await the coroutine
            filtered_features = (
                await geojson_handler.filter_geojson_features(  # Await the coroutine
                    start_date, end_date, filter_waco, waco_limits
                )
            )
            return jsonify({"type": "FeatureCollection", "features": filtered_features})
        except Exception as e:
            logger.error(f"Error fetching historical data: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/live_data")
    async def get_live_data():
        async with app.live_route_lock:
            latest_data = getattr(app, "latest_bouncie_data", {})
            return jsonify(latest_data)

    @app.route("/login", methods=["GET", "POST"])
    async def login():
        if request.method == "POST":
            form = await request.form
            pin = form.get("pin")
            if pin == app.config["PIN"]:
                session["authenticated"] = True
                return redirect(url_for("index"))
            return await render_template(
                "login.html", error="Invalid PIN. Please try again."
            )
        return await render_template("login.html")

    @app.route("/logout", methods=["GET", "POST"])
    async def logout():
        session.pop("authenticated", None)
        return redirect(url_for("login"))

    @app.route("/")
    @login_required
    @no_cache
    async def index():
        today = datetime.now().strftime("%Y-%m-%d")
        # Calculate the start date for the last month
        last_month_start = (date.today().replace(day=1) - timedelta(days=1)).replace(
            day=1
        )
        async with app.historical_data_lock:
            return await render_template(
                "index.html",
                today=today,
                historical_data_loaded=app.historical_data_loaded,
                last_month_start=last_month_start.strftime("%Y-%m-%d"),
                debug=config.DEBUG,
            )

    @app.before_serving
    async def startup():
        logger.info("Starting application initialization...")
        try:
            logger.info("Initializing historical data...")
            # Load historical data but do not update progress yet
            await load_historical_data_background(app, geojson_handler)
            logger.info("Historical data initialized without progress update.")
            if not hasattr(app, "background_tasks_started"):
                app.task_manager.add_task(poll_bouncie_api(app, bouncie_api))
                app.background_tasks_started = True
                logger.debug("Bouncie API polling task added")
            logger.debug(f"Available routes: {app.url_map}")
            logger.info("Application initialization complete")
        except Exception as e:
            logger.error(f"Error during startup: {str(e)}", exc_info=True)
            raise

    @app.after_serving
    async def shutdown():
        logger.info("Shutting down application...")
        try:
            await app.task_manager.cancel_all()
            logger.info("All tasks cancelled")
            if bouncie_api.client and bouncie_api.client.client_session:
                await bouncie_api.client.client_session.close()
                logger.info("Bouncie API client session closed")
            if (
                geojson_handler.bouncie_api.client
                and geojson_handler.bouncie_api.client.client_session
            ):
                await geojson_handler.bouncie_api.client.client_session.close()
                logger.info("GeoJSON handler Bouncie API client session closed")
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}", exc_info=True)
        finally:
            logger.info("Shutdown complete")

    @app.route("/api/load_historical_data", methods=["GET"])
    async def load_historical_data():
        try:
            data = await geojson_handler.data_loader.load_data(geojson_handler)
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/live_route_data", methods=["GET"])
    async def get_live_route_data():
        try:
            data = await geojson_handler.data_processor.get_live_route_data()
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/clear_live_route", methods=["POST"])
    async def clear_live_route():
        async with app.live_route_lock:
            app.live_route_data = {"features": []}
            app.clear_live_route = True
        return jsonify({"message": "Live route cleared successfully"})
