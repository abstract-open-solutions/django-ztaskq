import sys
import atexit
from zmq import PULL
from django.core.management.base import BaseCommand
from ...models import Task
from ...conf import settings, get_logger
from ...context import shared_context as context


class Command(BaseCommand):

    args = 'NAME'
    help = (
        'Start a worker instance.\n\n'
        'NAME is the name of the worker, which is necessary to differentiate \n'
        'correctly between them.\n\n'
        'Example: manage.py workerd worker1'
    )

    def handle(self, *args, **options): # pylint: disable=W0613
        if len(args) < 1:
            sys.stderr.write(
                "You must specify a name for this worker! See '%s -h'\n" % (
                    " ".join(sys.argv[:2]),
                )
            )
            sys.stderr.flush()
            sys.exit(-1)
        try:
            name = args[0]
            logger = get_logger(name)
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
                task.run(logger)
        except KeyboardInterrupt:
            raise sys.exit(0)
