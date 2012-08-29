from django.conf import settings

ZTASKD_URL = getattr(settings, 'ZTASKD_URL', 'tcp://127.0.0.1:5555')
ZTASK_WORKER_URL = getattr(settings, 'ZTASK_WORKER_URL', 'tcp://127.0.0.1:5556')
ZTASK_INTERNAL_QUEUE_URL = getattr(settings, 'ZTASK_INTERNAL_QUEUE_URL',
                                   'inproc://django_ztask_internal_queue')
ZTASKD_ON_LOAD = getattr(settings, 'ZTASKD_ON_LOAD', ())
ZTASKD_LOG_LEVEL = getattr(settings, 'ZTASKD_LOG_LEVEL', 'info')
ZTASKD_LOG_PATH = getattr(settings, 'ZTASKD_LOG_PATH', None)
