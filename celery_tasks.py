from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

celery_app = Celery('everystreet', broker=os.getenv('CELERY_BROKER_URL'))
celery_app.conf.broker_connection_retry_on_startup = True

celery_app.conf.update(
    result_backend=os.getenv('CELERY_RESULT_BACKEND'),
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

@celery_app.task
def update_historical_data_task(fetch_all=False):
    from create_app import create_app
    app = create_app()
    with app.app_context():
        geojson_handler = app.geojson_handler
        geojson_handler.update_historical_data(fetch_all=fetch_all)

@celery_app.task
def export_gpx_task(start_date, end_date, filter_waco, waco_boundary):
    from create_app import create_app
    from gpx_exporter import GPXExporter
    app = create_app()
    with app.app_context():
        geojson_handler = app.geojson_handler
        gpx_exporter = GPXExporter(geojson_handler)
        return gpx_exporter.export_to_gpx(start_date, end_date, filter_waco, waco_boundary)