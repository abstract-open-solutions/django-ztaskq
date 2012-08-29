import atexit
from zmq import PULL
from django.core.management.base import BaseCommand
from ...models import Task
from ...conf import settings, logger
from ...context import shared_context as context


class Command(BaseCommand):

    args = ''
    help = 'Start a worker instance.'

    def handle(self, *args, **options): # pylint: disable=W0613
        logger.info(
            "Worker listening on %s." % (settings.ZTASK_WORKER_URL,))
        socket = context.socket(PULL)
        def _shutdown():
            logger.debug('Shutting down nicely')
            socket.close()
        atexit.register(_shutdown)
        socket.connect(settings.ZTASK_WORKER_URL)
        while True:
            task_id, = socket.recv_pyobj()
            logger.info('Worker received task (%s)' % (str(task_id),))
            task = Task.objects.get(pk=task_id)
            task.run()

