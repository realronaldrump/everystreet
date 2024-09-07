import logging
import os
import pickle
import asyncio
import geopandas as gpd
from shapely.geometry import LineString
import aiofiles

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WacoStreetsAnalyzer:
    def __init__(self, streets_geojson_path):
        self.streets_geojson_path = streets_geojson_path
        self.cache_file = 'waco_streets_cache.pkl'
        self.streets_gdf = None
        self.traveled_streets = set()
        self.snap_distance = 0.00000001
        self.sindex = None
        self.lock = asyncio.Lock()

    async def initialize(self):
        logging.info("Initializing WacoStreetsAnalyzer...")
        try:
            await self.load_data()
            if self.streets_gdf is None or self.streets_gdf.empty:
                raise ValueError("streets_gdf is None or empty after load_data")
            self.sindex = self.streets_gdf.sindex
            logging.info(f"WacoStreetsAnalyzer initialized. Total streets: {len(self.streets_gdf)}")
        except Exception as e:
            logging.error(f"Error during WacoStreetsAnalyzer initialization: {str(e)}")
            raise

    async def load_data(self):
        try:
            if os.path.exists(self.cache_file):
                await self._load_from_cache()
            else:
                await self._process_and_cache_data()
        except Exception as e:
            logging.error(f"Error in load_data: {str(e)}")
            raise

    async def _load_from_cache(self):
        try:
            async with aiofiles.open(self.cache_file, 'rb') as f:
                cache_data = pickle.loads(await f.read())
            self.streets_gdf = cache_data['streets_gdf']
            self.traveled_streets = cache_data['traveled_streets']
            if self.streets_gdf is None or self.streets_gdf.empty:
                raise ValueError("Invalid data loaded from cache")
            logging.info(f"Loaded data from cache. Total streets: {len(self.streets_gdf)}")
        except Exception as e:
            logging.error(f"Error loading from cache: {str(e)}")
            await self._process_and_cache_data()

    async def _process_and_cache_data(self):
        try:
            self.streets_gdf = await asyncio.to_thread(gpd.read_file, self.streets_geojson_path)
            if self.streets_gdf is None or self.streets_gdf.empty:
                raise ValueError(f"Failed to load GeoJSON from {self.streets_geojson_path}")
            self.streets_gdf['street_id'] = self.streets_gdf.index
            self.streets_gdf = self.streets_gdf.to_crs(epsg=4326)
            self.streets_gdf = self.streets_gdf.set_index('street_id').sort_index()
            self.sindex = self.streets_gdf.sindex
            await self._save_to_cache()
            logging.info(f"Processed and cached street data. Total streets: {len(self.streets_gdf)}")
        except Exception as e:
            logging.error(f"Error processing street data: {str(e)}")
            raise

    async def _save_to_cache(self):
        try:
            cache_data = pickle.dumps({
                'streets_gdf': self.streets_gdf,
                'traveled_streets': self.traveled_streets
            })
            async with aiofiles.open(self.cache_file, 'wb') as f:
                await f.write(cache_data)
        except Exception as e:
            logging.error(f"Error saving to cache: {str(e)}")

    async def update_progress(self, routes):
        if self.streets_gdf is None:
            logging.error("streets_gdf is None. Unable to update progress.")
            return

        logging.info(f"Updating progress with {len(routes)} new routes...")
        if not routes:
            logging.warning("No routes provided for update_progress")
            return

        valid_features = [feature for feature in routes if feature['geometry']['type'] == 'LineString' and len(feature['geometry']['coordinates']) > 1]

        if not valid_features:
            logging.warning("No valid features to process")
            return

        try:
            gdf = await asyncio.to_thread(gpd.GeoDataFrame.from_features, valid_features)
            gdf.set_crs(epsg=4326, inplace=True)

            for _, route in gdf.iterrows():
                if isinstance(route.geometry, LineString):
                    line = route.geometry
                    logging.info(f"Processing route: {line.wkt[:100]}...")

                    if self.sindex is None:
                        logging.error("Spatial index is None. Rebuilding index.")
                        self.sindex = self.streets_gdf.sindex

                    possible_matches_index = list(self.sindex.intersection(line.bounds))
                    possible_matches = self.streets_gdf.iloc[possible_matches_index]

                    mask = possible_matches.intersects(line.buffer(self.snap_distance))
                    intersected_streets = possible_matches[mask]

                    self.traveled_streets.update(intersected_streets.index)

                    if len(intersected_streets) == 0:
                        logging.warning(f"Route did not intersect with any streets")
                    else:
                        logging.info(f"Route intersected with {len(intersected_streets)} streets")

                await asyncio.sleep(0)  # Yield control to the event loop

            await self._save_to_cache()
            logging.info(f"Total traveled streets: {len(self.traveled_streets)}")
            logging.info("Progress update completed.")
        except Exception as e:
            logging.error(f"Error processing routes: {str(e)}", exc_info=True)

    def calculate_progress(self):
        logger.info("Calculating progress...")
        if self.streets_gdf is None:
            logger.error("streets_gdf is None. Unable to calculate progress.")
            return {
                'coverage_percentage': 0,
                'total_streets': 0,
                'traveled_streets': 0
            }

        total_streets = len(self.streets_gdf)
        traveled_streets = len(self.traveled_streets)

        coverage_percentage = (traveled_streets / total_streets) * 100 if total_streets > 0 else 0

        return {
            'coverage_percentage': coverage_percentage,
            'total_streets': total_streets,
            'traveled_streets': traveled_streets
        }

    async def reset_progress(self):
        logger.info("Resetting progress...")
        self.traveled_streets.clear()
        await self._save_to_cache()

    async def get_progress_geojson(self, waco_boundary='city_limits'):
        logger.info("Generating progress GeoJSON...")
        if self.streets_gdf is None:
            logger.error("streets_gdf is None. Unable to generate progress GeoJSON.")
            return {"type": "FeatureCollection", "features": []}

        waco_limits = None
        if waco_boundary != "none":
            waco_limits = await asyncio.to_thread(gpd.read_file, f"static/boundaries/{waco_boundary}.geojson")
            waco_limits = waco_limits.geometry.unary_union

        self.streets_gdf['traveled'] = self.streets_gdf.index.isin(self.traveled_streets)

        if waco_limits is not None:
            filtered_streets = self.streets_gdf[self.streets_gdf.intersects(waco_limits)]
        else:
            filtered_streets = self.streets_gdf

        features = filtered_streets.apply(lambda row: {
            "type": "Feature",
            "geometry": row.geometry.__geo_interface__,
            "properties": {
                "street_id": row.name,
                "traveled": row.traveled,
                "color": "#00ff00" if row.traveled else "#ff0000"
            }
        }, axis=1).tolist()

        return {"type": "FeatureCollection", "features": features}

    async def get_untraveled_streets(self, waco_boundary='city_limits'):
        logger.info("Generating untraveled streets...")
        if self.streets_gdf is None:
            logger.error("streets_gdf is None. Unable to get untraveled streets.")
            return "{}"

        waco_limits = None
        if waco_boundary != "none":
            waco_limits = await asyncio.to_thread(gpd.read_file, f"static/boundaries/{waco_boundary}.geojson")
            waco_limits = waco_limits.geometry.unary_union

        untraveled_streets = self.streets_gdf[~self.streets_gdf.index.isin(self.traveled_streets)]
        if waco_limits is not None:
            untraveled_streets = untraveled_streets[untraveled_streets.intersects(waco_limits)]

        return untraveled_streets.to_json()

    async def get_street_network(self, waco_boundary='city_limits'):
        logger.info("Retrieving street network...")
        if self.streets_gdf is None:
            logger.error("streets_gdf is None. Unable to get street network.")
            return None

        waco_limits = None
        if waco_boundary != "none":
            waco_limits = await asyncio.to_thread(gpd.read_file, f"static/boundaries/{waco_boundary}.geojson")
            waco_limits = waco_limits.geometry.unary_union

        street_network = self.streets_gdf.copy()
        if waco_limits is not None:
            street_network = street_network[street_network.intersects(waco_limits)]

        street_network['traveled'] = street_network.index.isin(self.traveled_streets)

        return street_network
    
    def get_all_streets(self):
        if self.streets_gdf is None:
            logger.error("streets_gdf is None. Unable to get all streets.")
            return None
        logger.info(f"Retrieving all streets. Total streets: {len(self.streets_gdf)}")
        return self.streets_gdf