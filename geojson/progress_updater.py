import logging
logger = logging.getLogger(__name__)

class ProgressUpdater:
    def __init__(self, waco_analyzer):
        self.waco_analyzer = waco_analyzer

    async def update_progress(self, handler):
        try:
            logger.info("Updating progress for all historical data...")
            total_features = len(handler.historical_geojson_features)
            logger.info(f"Total features to process: {total_features}")

            # Await the progress update if it's an async operation
            await self.waco_analyzer.update_progress(handler.historical_geojson_features)

            # Calculate progress once and reuse the result
            final_coverage = self.waco_analyzer.calculate_progress()
            coverage_percentage = final_coverage["coverage_percentage"]
            traveled_streets = final_coverage["traveled_streets"]
            total_streets = final_coverage["total_streets"]
            traveled_segments = final_coverage["traveled_segments"]
            total_segments = final_coverage["total_segments"]

            logger.info(f"Progress updated successfully. Coverage: {coverage_percentage:.2f}%")
            logger.info(f"Streets: {traveled_streets} / {total_streets}")
            logger.info(f"Segments: {traveled_segments} / {total_segments}")

            return {
                "coverage_percentage": coverage_percentage,
                "total_streets": total_streets,
                "traveled_streets": traveled_streets,
                "total_segments": total_segments,
                "traveled_segments": traveled_segments,
            }
        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}", exc_info=True)
            raise

    async def update_streets_progress(self):
        try:
            coverage_analysis = self.waco_analyzer.calculate_progress()
            logger.info(f"Raw coverage analysis: {coverage_analysis}")
            return coverage_analysis
        except Exception as e:
            logger.error(f"Error updating Waco streets progress: {str(e)}", exc_info=True)
            return None