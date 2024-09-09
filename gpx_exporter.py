import logging
from datetime import datetime, timezone
from defusedxml.lxml import _etree as etree
from date_utils import (date_range, format_date, get_end_of_day,
                        get_start_of_day, parse_date)
from typing import List, Dict, Any
import asyncio

logger = logging.getLogger(__name__)

class GPXExporter:
    def __init__(self, geojson_handler):
        self.geojson_handler = geojson_handler

    async def export_to_gpx(self, start_date: str, end_date: str, filter_waco: bool, waco_boundary: str) -> bytes:
        try:
            logger.info(f"Exporting GPX for date range: {start_date} to {end_date}")
            logger.info(f"Filter Waco: {filter_waco}, Waco Boundary: {waco_boundary}")

            start_date, end_date = map(parse_date, (start_date, end_date))
            waco_limits = self.geojson_handler.load_waco_boundary(waco_boundary) if filter_waco else None
            logger.info(f"Loaded Waco limits: {waco_limits is not None}")

            filtered_features = await self._gather_filtered_features(start_date, end_date, filter_waco, waco_limits)
            logger.info(f"Number of filtered features: {len(filtered_features)}")

            if not filtered_features:
                logger.warning("No features found after filtering")
                return None

            gpx = self._create_gpx_root(start_date, end_date)
            await self._add_tracks_to_gpx(gpx, filtered_features)

            gpx_data = etree.tostring(gpx, pretty_print=True, xml_declaration=True, encoding="UTF-8")
            logger.info(f"Successfully created GPX data of length: {len(gpx_data)}")
            return gpx_data

        except Exception as e:
            logger.error(f"Error in export_to_gpx: {str(e)}", exc_info=True)
            raise

    async def _gather_filtered_features(self, start_date: datetime, end_date: datetime, filter_waco: bool, waco_limits: Any) -> List[Dict[str, Any]]:
        filtered_features = []
        tasks = []

        for current_date in date_range(start_date, end_date):
            month_year = current_date.strftime("%Y-%m")
            if month_year in self.geojson_handler.monthly_data:
                task = self._filter_features_for_date(current_date, filter_waco, waco_limits, month_year)
                tasks.append(task)

        results = await asyncio.gather(*tasks)
        for result in results:
            filtered_features.extend(result)

        return filtered_features

    async def _filter_features_for_date(self, current_date: datetime, filter_waco: bool, waco_limits: Any, month_year: str) -> List[Dict[str, Any]]:
        return self.geojson_handler.filter_geojson_features(
            format_date(get_start_of_day(current_date)),
            format_date(get_end_of_day(current_date)),
            filter_waco,
            waco_limits,
            self.geojson_handler.monthly_data[month_year],
        )

    def _create_gpx_root(self, start_date: datetime, end_date: datetime) -> etree.Element:
        gpx = etree.Element("gpx", version="1.1", creator="EveryStreetApp")
        metadata = etree.SubElement(gpx, "metadata")
        name = etree.SubElement(metadata, "name")
        name.text = f"GPX Export {format_date(start_date)} to {format_date(end_date)}"
        time = etree.SubElement(metadata, "time")
        time.text = format_date(datetime.now(timezone.utc))
        return gpx

    async def _add_tracks_to_gpx(self, gpx: etree.Element, filtered_features: List[Dict[str, Any]]) -> None:
        for i, feature in enumerate(filtered_features, 1):
            logger.info(f"Processing feature {i}/{len(filtered_features)}")
            if "geometry" not in feature or "coordinates" not in feature["geometry"]:
                logger.warning(f"Invalid feature structure: {feature}")
                continue

            trk = etree.SubElement(gpx, "trk")
            name = etree.SubElement(trk, "name")
            name.text = f"Track {feature['properties'].get('id', f'Unknown_{i}')}"
            trkseg = etree.SubElement(trk, "trkseg")

            coordinates = feature["geometry"]["coordinates"]
            timestamps = self.geojson_handler.get_feature_timestamps(feature)

            logger.info(f"Number of coordinates in feature: {len(coordinates)}")
            await self._add_track_points(trkseg, coordinates, timestamps, i)

    async def _add_track_points(self, trkseg: etree.Element, coordinates: List[List[float]], timestamps: List[Any], feature_index: int) -> None:
        for j, coord in enumerate(coordinates):
            if not isinstance(coord, (list, tuple)) or len(coord) < 2:
                logger.warning(f"Invalid coordinate: {coord}")
                continue

            trkpt = etree.SubElement(trkseg, "trkpt", lat=str(coord[1]), lon=str(coord[0]))
            if j < len(timestamps):
                time = etree.SubElement(trkpt, "time")
                timestamp = timestamps[j]
                time.text = self._format_timestamp(timestamp, j, feature_index)
            else:
                logger.warning(f"No timestamp for coordinate {j} in feature {feature_index}")

    def _format_timestamp(self, timestamp: Any, coord_index: int, feature_index: int) -> str:
        if isinstance(timestamp, (int, float)):
            return format_date(datetime.fromtimestamp(timestamp, timezone.utc))
        elif isinstance(timestamp, tuple) and len(timestamp) >= 1:
            return format_date(datetime.fromtimestamp(timestamp[0], timezone.utc))
        else:
            logger.warning(f"Invalid timestamp format for coordinate {coord_index} in feature {feature_index}: {timestamp}")
            return ""