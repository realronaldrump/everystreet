from celery import Celery
from config import Config

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

def init_celery(app):
    config = Config()
    app.config.update(
        CELERY_BROKER_URL=config.CELERY_BROKER_URL,
        CELERY_RESULT_BACKEND=config.CELERY_RESULT_BACKEND
    )
    celery = make_celery(app)
    celery.conf.update(broker_connection_retry_on_startup=True)
    return celery