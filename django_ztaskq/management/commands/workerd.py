from optparse import make_option
try:
    from zmq import PULL
except ImportError:
    from zmq import UPSTREAM as PULL
from django.core.management.base import BaseCommand
from django.utils import autoreload
from ...models import Task
from ...conf import settings, logger
from ...context import shared_context as context


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--noreload',
            action='store_false', dest='use_reloader', default=True,
            help='Tells Django to NOT use the auto-reloader.'),
    )
    args = ''
    help = 'Start a worker instance.'

    def handle(self, *args, **options): # pylint: disable=W0613
        use_reloader = options.get('use_reloader', True)

        if use_reloader:
            autoreload.main(self._handle)
        else:
            self._handle()

    def _handle(self):
        logger.info("Worker listening on %s." % (settings.ZTASK_WORKER_URL,))

        socket = context.socket(PULL)
        socket.connect(settings.ZTASK_WORKER_URL)

        while True:
            task_id, = socket.recv_pyobj()
            logger.info('Worker received task (%s)' % (str(task_id),))
            task = Task.objects.get(pk=task_id)
            task.run()
