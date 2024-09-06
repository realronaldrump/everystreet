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

            await self.waco_analyzer.update_progress(handler.historical_geojson_features)

            final_coverage = self.waco_analyzer.calculate_progress()
            logger.info(f"Progress updated successfully. Coverage: {final_coverage['coverage_percentage']:.2f}%")
            return {
                'coverage_percentage': final_coverage['coverage_percentage'],
                'total_streets': final_coverage['total_streets'],
                'traveled_streets': final_coverage['traveled_streets']
            }
        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}", exc_info=True)
            raise

    async def update_streets_progress(self, handler):
        try:
            coverage_analysis = self.waco_analyzer.calculate_progress()
            logging.info(f"Raw coverage analysis: {coverage_analysis}")
            return coverage_analysis
        except Exception as e:
            logging.error(f"Error updating Waco streets progress: {str(e)}", exc_info=True)
            return None