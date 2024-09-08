import asyncio
from datetime import datetime, date, timezone
import json
import logging

from quart import jsonify, redirect, render_template, request, session, url_for, Response, websocket
from quart_cors import cors
from cachetools import TTLCache

from config import Config
from date_utils import format_date, timedelta
from geojson.geojson_handler import GeoJSONHandler
from gpx_exporter import GPXExporter
from models import DateRange, HistoricalDataParams
from utils import login_required, geolocator, TaskManager
from waco_streets_analyzer import WacoStreetsAnalyzer
from tasks import load_historical_data_background, poll_bouncie_api

logger = logging.getLogger(__name__)
config = Config()

# Removed BouncieAPI instance creation here
gpx_exporter = GPXExporter(None)

# Initialize cache
cache = TTLCache(maxsize=100, ttl=3600)

def register_routes(app):
    waco_analyzer = app.waco_streets_analyzer
    geojson_handler = app.geojson_handler
    bouncie_api = app.bouncie_api
    gpx_exporter = GPXExporter(geojson_handler)

    @app.route('/progress')
    async def get_progress():
        async with app.progress_lock:
            try:
                coverage_analysis = await geojson_handler.update_waco_streets_progress()
                if coverage_analysis is None:
                    raise ValueError("Failed to update Waco streets progress")
                logging.info(f"Progress update: {coverage_analysis}")
                return jsonify({
                    "total_streets": int(coverage_analysis["total_streets"]),
                    "traveled_streets": int(coverage_analysis["traveled_streets"]),
                    "coverage_percentage": float(coverage_analysis["coverage_percentage"])
                })
            except Exception as e:
                logging.error(f"Error in get_progress: {str(e)}", exc_info=True)
                return jsonify({"error": str(e)}), 500

    @app.route("/filtered_historical_data")
    async def get_filtered_historical_data():
        try:
            params = HistoricalDataParams(
                date_range=DateRange(
                    start_date=request.args.get("startDate") or "2020-01-01",
                    end_date=request.args.get("endDate") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
                ),
                filter_waco=request.args.get("filterWaco", "false").lower() == "true",
                waco_boundary=request.args.get("wacoBoundary", "city_limits"),
                bounds=[float(x) for x in request.args.get("bounds", "").split(",")] if request.args.get("bounds") else None
            )

            logger.info(f"Received request for filtered historical data: {params}")

            waco_limits = None
            if params.filter_waco and params.waco_boundary != "none":
                waco_limits = await geojson_handler.load_waco_boundary(params.waco_boundary)

            filtered_features = await geojson_handler.filter_geojson_features(
                params.date_range.start_date.isoformat(),
                params.date_range.end_date.isoformat(),
                params.filter_waco,
                waco_limits,
                bounds=params.bounds
            )

            result = {
                "type": "FeatureCollection",
                "features": filtered_features,
                "total_features": len(filtered_features)
            }

            return jsonify(result)

        except ValueError as e:
            logger.error(f"Error parsing parameters: {str(e)}")
            return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
        except Exception as e:
            logger.error(f"Error filtering historical data: {str(e)}", exc_info=True)
            return jsonify({"error": f"Error filtering historical data: {str(e)}"}), 500

    @app.route('/waco_streets')
    async def get_waco_streets():
        try:
            waco_boundary = request.args.get("wacoBoundary", "city_limits")
            streets_filter = request.args.get("filter", "all")
            cache_key = f"waco_streets_{waco_boundary}_{streets_filter}"
            
            if cache_key in cache:
                return jsonify(cache[cache_key])
            
            logging.info(f"Fetching Waco streets: boundary={waco_boundary}, filter={streets_filter}")
            streets_geojson = await geojson_handler.get_waco_streets(waco_boundary, streets_filter)
            streets_data = json.loads(streets_geojson)
            
            if 'features' not in streets_data:
                raise ValueError("Invalid GeoJSON: 'features' key not found")
            
            cache[cache_key] = streets_data
            logging.info(f"Returning {len(streets_data['features'])} street features")
            return jsonify(streets_data)
        except Exception as e:
            logging.error(f"Error in get_waco_streets: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.websocket('/ws/live_route')
    async def ws_live_route():
        try:
            while True:
                async with app.live_route_lock:
                    data = app.live_route_data
                await websocket.send(json.dumps(data))
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Handle WebSocket disconnection
            pass

    @app.route("/update_progress", methods=["POST"])
    async def update_progress():
        async with app.progress_lock:
            try:
                coverage_analysis = await geojson_handler.update_all_progress()
                return jsonify({
                    "total_streets": int(coverage_analysis["total_streets"]),
                    "traveled_streets": int(coverage_analysis["traveled_streets"]),
                    "coverage_percentage": float(coverage_analysis["coverage_percentage"])
                }), 200
            except Exception as e:
                logger.error(f"Error updating progress: {str(e)}", exc_info=True)
                return jsonify({"error": f"Error updating progress: {str(e)}"}), 500

    @app.route('/untraveled_streets')
    async def get_untraveled_streets():
        waco_boundary = request.args.get("wacoBoundary", "city_limits")
        untraveled_streets = await geojson_handler.get_untraveled_streets(waco_boundary)
        return jsonify(json.loads(untraveled_streets))

    @app.route("/latest_bouncie_data")
    async def get_latest_bouncie_data():
        async with app.live_route_lock:
            return jsonify(getattr(app, 'latest_bouncie_data', {}))

    @app.route("/live_route", methods=["GET"])
    async def live_route():
        async with app.live_route_lock:
            return jsonify(app.live_route_data)

    @app.route("/historical_data_status")
    async def historical_data_status():
        async with app.historical_data_lock:
            return jsonify({
                "loaded": app.historical_data_loaded,
                "loading": app.historical_data_loading
            })

    @app.route("/trip_metrics")
    async def get_trip_metrics():
        formatted_metrics = await bouncie_api.get_trip_metrics()  # Use bouncie_api from app
        return jsonify(formatted_metrics)

    @app.route("/export_gpx")
    async def export_gpx():
        start_date = request.args.get("startDate") or "2020-01-01"
        end_date = request.args.get("endDate") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filter_waco = request.args.get("filterWaco", "false").lower() == "true"
        waco_boundary = request.args.get("wacoBoundary", "city_limits")

        try:
            gpx_data = await gpx_exporter.export_to_gpx(
                format_date(start_date), format_date(end_date), filter_waco, waco_boundary
            )

            if gpx_data is None:
                logger.warning("No data found for GPX export")
                return jsonify({"error": "No data found for the specified date range"}), 404

            return Response(
                gpx_data,
                mimetype="application/gpx+xml",
                headers={"Content-Disposition": "attachment;filename=export.gpx"},
            )
        except Exception as e:
            logger.error(f"Error in export_gpx: {str(e)}", exc_info=True)
            return jsonify({"error": f"An error occurred while exporting GPX: {str(e)}"}), 500

    @app.route("/search_location")
    async def search_location():
        query = request.args.get("query")
        if not query:
            return jsonify({"error": "No search query provided"}), 400

        try:
            location = await asyncio.to_thread(geolocator.geocode, query)
            if location:
                return jsonify({
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "address": location.address
                })
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
            locations = await asyncio.to_thread(geolocator.geocode, query, exactly_one=False, limit=5)
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
                await geojson_handler.update_historical_data(fetch_all=True)
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

    @app.route('/processing_status')
    async def processing_status():
        async with app.processing_lock:
            return jsonify({'isProcessing': app.is_processing})

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
                return jsonify({"message": "Progress has been reset and recalculated successfully!"}), 200
            except Exception as e:
                logger.error(f"An error occurred during the progress reset process: {e}")
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

            logger.info(f"Fetching historical data for: {start_date} to {end_date}, filterWaco: {filter_waco}, wacoBoundary: {waco_boundary}")

            waco_limits = None
            if filter_waco:
                waco_limits = await geojson_handler.load_waco_boundary(waco_boundary)  # Await the coroutine

            filtered_features = await geojson_handler.filter_geojson_features(  # Await the coroutine
                start_date, end_date, filter_waco, waco_limits
            )

            return jsonify({
                "type": "FeatureCollection",
                "features": filtered_features
            })
        except Exception as e:
            logger.error(f"Error fetching historical data: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/live_data")
    async def get_live_data():
        async with app.live_route_lock:
            latest_data = getattr(app, 'latest_bouncie_data', {})
            return jsonify(latest_data)

    @app.route("/login", methods=["GET", "POST"])
    async def login():
        if request.method == "POST":
            form = await request.form
            pin = form.get("pin")
            if pin == app.config["PIN"]:
                session["authenticated"] = True
                return redirect(url_for("index"))
            return await render_template("login.html", error="Invalid PIN. Please try again.")
        return await render_template("login.html")

    @app.route("/logout", methods=["GET", "POST"])
    async def logout():
        session.pop("authenticated", None)
        return redirect(url_for("login"))

    @app.route("/")
    @login_required
    async def index():
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Calculate the start date for the last month
        last_month_start = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)
        
        async with app.historical_data_lock:
            return await render_template(
                "index.html", 
                today=today, 
                historical_data_loaded=app.historical_data_loaded,
                last_month_start=last_month_start.strftime("%Y-%m-%d"),
                debug=config.DEBUG
            )

    @app.before_serving
    async def startup():
        logger.info("Starting application initialization...")
        try:
            logger.info("Initializing historical data...")

            # Load historical data but do not update progress yet
            await load_historical_data_background(app, geojson_handler)
            logger.info("Historical data initialized without progress update.")

            if not hasattr(app, 'background_tasks_started'):
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

            if geojson_handler.bouncie_api.client and geojson_handler.bouncie_api.client.client_session:
                await geojson_handler.bouncie_api.client.client_session.close()
                logger.info("GeoJSON handler Bouncie API client session closed")

        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}", exc_info=True)
        finally:
            logger.info("Shutdown complete")

    @app.route('/api/load_historical_data', methods=['GET'])
    async def load_historical_data():
        try:
            data = await geojson_handler.data_loader.load_data(geojson_handler)
            return jsonify(data)
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/live_route_data', methods=['GET'])
    async def get_live_route_data():
        try:
            data = await geojson_handler.data_processor.get_live_route_data()
            return jsonify(data)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
