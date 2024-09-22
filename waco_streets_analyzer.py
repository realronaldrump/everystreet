import asyncio
import logging
import os
import aiofiles
import geopandas as gpd
from shapely.geometry import LineString
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class WacoStreetsAnalyzer:
    def __init__(self, streets_geojson_path):
        self.streets_geojson_path = streets_geojson_path
        self.cache_file = "waco_streets_cache.pkl"
        self.streets_gdf = None
        self.segments_gdf = None
        self.traveled_segments = set()
        self.snap_distance = 0.0000001
        self.sindex = None
        self.lock = asyncio.Lock()

    async def initialize(self):
        logger.info("Initializing WacoStreetsAnalyzer...")
        try:
            await self.load_data()
            if self.streets_gdf is None or self.streets_gdf.empty:
                raise ValueError("streets_gdf is None or empty after load_data")
            self.sindex = self.segments_gdf.sindex
            logger.info(
                "WacoStreetsAnalyzer initialized. Total streets: %s, "
                "Total segments: %s",
                len(self.streets_gdf), len(self.segments_gdf)
            )
        except Exception as e:
            logger.error("Error during WacoStreetsAnalyzer initialization: %s", str(e))
            raise

    async def load_data(self):
        try:
            if os.path.exists(self.cache_file):
                await self._load_from_cache()
            else:
                await self._process_and_cache_data()
        except Exception as e:
            logger.error("Error in load_data: %s", str(e))
            raise

    async def _load_from_cache(self):
        try:
            async with aiofiles.open(self.cache_file, "rb") as f:
                cache_data = await f.read()
            cache_dict = json.loads(cache_data.decode('utf-8'))
            self.streets_gdf = cache_dict["streets_gdf"]
            self.segments_gdf = cache_dict["segments_gdf"]
            self.traveled_segments = set(cache_dict["traveled_segments"])
            if (
                self.streets_gdf is None or self.streets_gdf.empty or
                self.segments_gdf is None or self.segments_gdf.empty
            ):
                raise ValueError("Invalid data loaded from cache")
            logger.info(
                "Loaded data from cache. Total streets: %s, Total segments: %s",
                len(self.streets_gdf), len(self.segments_gdf)
            )
        except Exception as e:
            logger.error("Error loading from cache: %s", str(e))
            await self._process_and_cache_data()

    async def _process_and_cache_data(self):
        try:
            self.streets_gdf = gpd.read_file(self.streets_geojson_path)
            if self.streets_gdf is None or self.streets_gdf.empty:
                raise ValueError(
                    f"Failed to load GeoJSON from {self.streets_geojson_path}"
                )
            if "street_id" not in self.streets_gdf.columns:
                self.streets_gdf["street_id"] = self.streets_gdf.index.astype(str)
            self.streets_gdf = self.streets_gdf.to_crs(epsg=4326)
            self.streets_gdf = self.streets_gdf.set_index(
                "street_id", drop=False
            ).sort_index()
            self.segments_gdf = self._create_segments()
            await self._save_to_cache()
            logger.info(
                "Processed and cached street data. Total streets: %d, "
                "Total segments: %d",
                len(self.streets_gdf), len(self.segments_gdf)
            )
        except Exception as e:
            logger.error("Error processing street data: %s", str(e))
            raise

    def _create_segments(self):
        segments = []
        for _, row in self.streets_gdf.iterrows():
            if isinstance(row.geometry, LineString):
                coords = list(row.geometry.coords)
                for i in range(len(coords) - 1):
                    segment = LineString([coords[i], coords[i + 1]])
                    segments.append(
                        {
                            "geometry": segment,
                            "street_id": row.street_id,
                            "segment_id": f"{row.street_id}_{i}",
                        }
                    )
        return gpd.GeoDataFrame(segments, crs=self.streets_gdf.crs)

    async def _save_to_cache(self):
        try:
            cache_data = json.dumps({
                "streets_gdf": self.streets_gdf.to_json(),
                "segments_gdf": self.segments_gdf.to_json(),
                "traveled_segments": list(self.traveled_segments),
            })
            async with aiofiles.open(self.cache_file, "wb") as f:
                await f.write(cache_data.encode('utf-8'))
        except Exception as e:
            logger.error("Error saving to cache: %s", str(e))

    async def update_progress(self, routes):
        if self.segments_gdf is None:
            logger.error("segments_gdf is None. Unable to update progress.")
            return
        logger.info("Updating progress with %s new routes...", len(routes))
        if not routes:
            logger.warning("No routes provided for update_progress")
            return
        valid_features = [
            feature for feature in routes
            if (
                feature["geometry"]["type"] == "LineString" and
                len(feature["geometry"]["coordinates"]) > 1
            )
        ]
        if not valid_features:
            logger.warning("No valid features to process")
            return
        try:
            batch_size = 10000
            for i in range(0, len(valid_features), batch_size):
                batch = valid_features[i:i+batch_size]
                gdf = gpd.GeoDataFrame.from_features(batch)
                gdf.set_crs(epsg=4326, inplace=True)

                joined = gpd.sjoin(
                    gdf,
                    self.segments_gdf,
                    how="inner",
                    predicate="intersects"
                )

                close_segments = joined[joined.apply(
                    lambda row: row.geometry.distance(
                        self.segments_gdf.loc[row.index_right].geometry
                    ) <= self.snap_distance,
                    axis=1
                )]

                self.traveled_segments.update(close_segments['segment_id'].tolist())

                logger.info(
                    "Batch processed. Total traveled segments: %s",
                    len(self.traveled_segments)
                )

            logger.info("Progress update completed.")
        except Exception as e:
            logger.error("Error processing routes: %s", str(e), exc_info=True)

    def calculate_progress(self):
        logger.info("Calculating progress...")
        if self.segments_gdf is None:
            logger.error("segments_gdf is None. Unable to calculate progress.")
            return {
                "coverage_percentage": 0,
                "total_streets": 0,
                "traveled_streets": 0,
                "total_segments": 0,
                "traveled_segments": 0,
            }
        total_segments = len(self.segments_gdf)
        traveled_segments = len(self.traveled_segments)
        total_streets = len(self.streets_gdf)
        traveled_streets = len(
            set(
                self.segments_gdf.loc[
                    self.segments_gdf["segment_id"].isin(self.traveled_segments),
                    "street_id",
                ]
            )
        )
        coverage_percentage = (
            (traveled_segments / total_segments) * 100 if total_segments > 0 else 0
        )
        return {
            "coverage_percentage": coverage_percentage,
            "total_streets": total_streets,
            "traveled_streets": traveled_streets,
            "total_segments": total_segments,
            "traveled_segments": traveled_segments,
        }

    async def reset_progress(self):
        logger.info("Resetting progress...")
        self.traveled_segments.clear()
        await self._save_to_cache()

    async def get_progress_geojson(self, waco_boundary="city_limits"):
        logger.info("Generating progress GeoJSON...")
        if self.segments_gdf is None:
            logger.error("segments_gdf is None. Unable to generate progress GeoJSON.")
            return {"type": "FeatureCollection", "features": []}
        waco_limits = None
        if waco_boundary != "none":
            waco_limits = gpd.read_file(f"static/boundaries/{waco_boundary}.geojson")
            waco_limits = waco_limits.geometry.unary_union
        self.segments_gdf["traveled"] = self.segments_gdf["segment_id"].isin(
            self.traveled_segments
        )
        if waco_limits is not None:
            filtered_segments = self.segments_gdf[
                self.segments_gdf.intersects(waco_limits)
            ]
        else:
            filtered_segments = self.segments_gdf
        features = filtered_segments.apply(
            lambda row: {
                "type": "Feature",
                "geometry": row.geometry.__geo_interface__,
                "properties": {
                    "segment_id": row.segment_id,
                    "street_id": row.street_id,
                    "traveled": row.traveled,
                    "color": "#00ff00" if row.traveled else "#ff0000",
                },
            },
            axis=1,
        ).tolist()
        return {"type": "FeatureCollection", "features": features}

    async def get_untraveled_streets(self, waco_boundary="city_limits"):
        logger.info("Generating untraveled streets...")
        if self.streets_gdf is None or self.segments_gdf is None:
            logger.error(
                "streets_gdf or segments_gdf is None. Unable to get untraveled streets."
            )
            return "{}"
        waco_limits = None
        if waco_boundary != "none":
            waco_limits = gpd.read_file(f"static/boundaries/{waco_boundary}.geojson")
            waco_limits = waco_limits.geometry.unary_union
        traveled_streets = set(
            self.segments_gdf[
                self.segments_gdf["segment_id"].isin(self.traveled_segments)
            ]["street_id"]
        )
        untraveled_streets = self.streets_gdf[
            ~self.streets_gdf["street_id"].isin(traveled_streets)
        ]
        if waco_limits is not None:
            untraveled_streets = untraveled_streets[
                untraveled_streets.intersects(waco_limits)
            ]
        return untraveled_streets.to_json()

    async def get_street_network(self, waco_boundary="city_limits"):
        logger.info("Retrieving street network...")
        if self.streets_gdf is None or self.segments_gdf is None:
            logger.error(
                "streets_gdf or segments_gdf is None. Unable to get street network."
            )
            return None
        waco_limits = None
        if waco_boundary != "none":
            waco_limits = gpd.read_file(f"static/boundaries/{waco_boundary}.geojson")
            waco_limits = waco_limits.geometry.unary_union
        street_network = self.streets_gdf.copy()
        if waco_limits is not None:
            street_network = street_network[street_network.intersects(waco_limits)]
        traveled_streets = set(
            self.segments_gdf[
                self.segments_gdf["segment_id"].isin(self.traveled_segments)
            ]["street_id"]
        )
        street_network["traveled"] = street_network["street_id"].isin(traveled_streets)
        return street_network

    def get_all_streets(self):
        if self.streets_gdf is None:
            logger.error("streets_gdf is None. Unable to get all streets.")
            return None
        logger.info("Retrieving all streets. Total streets: %d", len(self.streets_gdf))
        return self.streets_gdf
