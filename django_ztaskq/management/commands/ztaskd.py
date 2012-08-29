import sys
import traceback
from threading import Thread
from optparse import make_option
from zmq.core.device import device
try:
    from zmq import PUSH
except ImportError:
    from zmq import DOWNSTREAM as PUSH
try:
    from zmq import PULL
except ImportError:
    from zmq import UPSTREAM as PULL
from django.core.management.base import BaseCommand
from django.utils import autoreload
from ...models import Task
from ...conf import settings, logger
from ...context import shared_context as context


class DeviceType(object):
    QUEUE, FORWARDER, STREAMER = range(3)


class QueuingThread(Thread):
    """Handles passing the tasks to the worker process.
    """

    def __init__(self, group=None, name=None):
        super(QueuingThread, self).__init__(group=group, name=name)
        self.zmq_device = None
        self.queue_socket = context.socket(PULL)
        self.worker_socket = context.socket(PUSH)

    def bind_sockets(self):
        self.queue_socket.connect(settings.ZTASK_INTERNAL_QUEUE_URL)
        self.worker_socket.bind(settings.ZTASK_WORKER_URL)

    def run(self):
        logger.info('Queue thread is running.')
        self.bind_sockets()
        self.zmq_device = device(
            DeviceType.STREAMER,
            self.queue_socket,
            self.worker_socket
        )


class ServerThread(Thread):
    """Handles logging of tasks in the Django model.

    It also passes the task to the queueing component"""

    def __init__(self, group=None, name=None):
        super(ServerThread, self).__init__(group=group, name=name)
        self.server_socket = context.socket(PULL)
        self.queue_socket = context.socket(PUSH)

    def bind_sockets(self):
        self.server_socket.bind(settings.ZTASKD_URL)
        self.queue_socket.bind(settings.ZTASK_INTERNAL_QUEUE_URL)

    def recv_and_enqueue(self):
        try:
            id, function, args, kwargs = self.server_socket.recv_pyobj()
            task, was_created = Task.objects.get_or_create(
                taskid=id,
                function_name=function,
                args=args,
                kwargs=kwargs,
            )
            logger.info('Listed task in django database (%r)' % task.pk)
            if was_created:
                self.queue_socket.send_pyobj((task.pk,))
                logger.info('Passed task to worker queue (%r)' % task.pk)
            else:
                logger.info(
                    'Ignoring task (%r) because it is already computed.' % (
                        task.pk,
                    )
                )
        except Exception, e: # pylint: disable=W0703
            logger.error('Error setting up function. Details:\n%s' % e)
            traceback.print_exc(e)

    def run(self):
        logger.info('Server thread is running.')
        self.bind_sockets()
        while True:
            self.recv_and_enqueue()


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--noreload',
            action='store_false', dest='use_reloader', default=True,
            help='Tells Django to NOT use the auto-reloader.'),
        make_option('--replayfailed',
            action='store_true', dest='replay_failed', default=False,
            help='Replays all failed calls in the db'),
    )
    args = ''
    help = 'Start the ztaskd server'

    def handle(self, *args, **options): # pylint: disable=W0613
        use_reloader = options.get('use_reloader', True)

        if use_reloader:
            autoreload.main(lambda: self._handle(use_reloader))
        else:
            self._handle(use_reloader)

    def _on_load(self):
        """Execute any startup function callbacks associated with ztaskd.
        """

        for callable_name in settings.ZTASKD_ON_LOAD:
            logger.info("ON_LOAD calling %s" % callable_name)

            parts = callable_name.split('.')
            module_name = '.'.join(parts[:-1])
            member_name = parts[-1]

            if not module_name in sys.modules:
                __import__(module_name)

            callable_fn = getattr(sys.modules[module_name], member_name)
            callable_fn()

    def _handle(self, use_reloader):
        if use_reloader:
            logger.info(
                'Development server starting on %s.' % settings.ZTASKD_URL
            )
        else:
            logger.info(
                'Production server starting on %s.' % settings.ZTASKD_URL
            )
        self._on_load()
        # TODO: how should these threads be killed when reloaded
        queue_thread = QueuingThread()
        queue_thread.start()
        serve_thread = ServerThread()
        serve_thread.start()
