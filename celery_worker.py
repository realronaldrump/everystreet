import os
from dotenv import load_dotenv
from celery import Celery
from config import Config

load_dotenv()

config = Config()
celery = Celery(__name__)
celery.config_from_object(config)

celery.conf.update(
    broker_url=config.CELERY_BROKER_URL,
    result_backend=config.CELERY_RESULT_BACKEND,
    broker_connection_retry_on_startup=True,
    task_routes={
        'update_historical_data_task': {'queue': 'historical_data'},
        'export_gpx_task': {'queue': 'export'}
    },
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Import tasks here to avoid circular imports
from celery_tasks import update_historical_data_task, export_gpx_task

if __name__ == '__main__':
    celery.start()