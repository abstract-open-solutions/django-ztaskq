import logging
from StringIO import StringIO
from uuid import uuid4
from multiprocessing import Process, Queue
from datetime import datetime
from pytz import utc
from zmq import PUSH, PULL, Context
from mock import patch, MagicMock
from django.test import TestCase
from .conf import settings
from .decorators import ztask
from .models import Task, Status


class SimpleReceiver(Process):

    def __init__(self, *args, **kwargs):
        attrs = {
            'queue': kwargs.pop('queue'),
            'socket_url': kwargs.pop('socket_url'),
            'socket_type': kwargs.pop('socket_type')
        }
        super(SimpleReceiver, self).__init__(*args, **kwargs)
        self.__dict__.update(attrs)

    def run(self):
        context = Context()
        socket = context.socket(self.socket_type)
        socket.bind(self.socket_url)
        data = socket.recv_pyobj()
        self.queue.put(data)
        context.destroy()
        self.queue.close()


@ztask()
def hello_world(name):
    return "Hello world, I am %s" % name


@ztask(True)
def hello_world_memoized(name):
    return "Hello world, I am %s" % name


@ztask()
def convert_int(value):
    return int(value)


class simple_receiver(object):

    def __init__(self, socket_url, socket_type):
        self.queue = Queue()
        self.receiver = SimpleReceiver(
            queue=self.queue,
            socket_url=socket_url,
            socket_type=socket_type
        )

    def __enter__(self):
        self.receiver.start()
        return self.queue

    def __exit__(self, exc_type, exc_value, traceback):
        self.queue.close()
        self.receiver.terminate()


class DecoratorTest(TestCase):
    """Tests decorating a function to make it a task.
    """

    def test_wraps(self):
        """Test that the function is properly wrapped
        """
        self.assertTrue(hasattr(hello_world, 'async'))
        self.assertEqual(hello_world("Joe"),
                          "Hello world, I am Joe")

    def test_send(self):
        """Tests that the dispatcher is contacted
        """
        with simple_receiver(settings.ZTASKD_URL, PULL) as queue:
            uuid = hello_world.async("Joe")
            self.assertEqual(
                queue.get(),
                (uuid, 'django_ztaskq.tests.hello_world', ('Joe',), {}, 0)
            )

    def test_delay(self):
        """Tests that the delay is propagated
        """
        with simple_receiver(settings.ZTASKD_URL, PULL) as queue:
            uuid = hello_world.async("Joe", ztaskq_delay=1)
            self.assertEqual(
                queue.get(),
                (uuid, 'django_ztaskq.tests.hello_world', ('Joe',), {}, 1)
            )

    def test_memoize(self):
        """Tests that the uuid is invariant
        """
        with simple_receiver(settings.ZTASKD_URL, PULL) as queue:
            uuid1 = hello_world_memoized.async("Joe")
            params1 = queue.get()
        with simple_receiver(settings.ZTASKD_URL, PULL) as queue:
            uuid2 = hello_world_memoized.async("Joe")
            params2 = queue.get()
        with simple_receiver(settings.ZTASKD_URL, PULL) as queue:
            uuid3 = hello_world_memoized.async("Joe", ztaskq_delay=1)
            params3 = queue.get()
        self.assertEqual(uuid1, uuid2)
        self.assertEqual(params1, params2)
        self.assertNotEqual(uuid1, uuid3)
        self.assertNotEqual(params1, params3)


class TaskModelTest(TestCase):
    """Tests creating, running, and failing tasks.
    """

    def get_logger(self):
        name = 'django_ztaskq.tests.%s' % uuid4()
        logger = logging.getLogger(name)
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        stream = StringIO()
        logger.addHandler(logging.StreamHandler(stream))
        return (name, logger, stream)

    def test_create(self):
        """Tests task creation
        """
        task = Task(taskid=uuid4(),
                    function_name='django_ztaskq.tests.hello_world',
                    args=("Joe",),
                    kwargs={})
        self.assertIsNone(task.return_value)
        self.assertIsNone(task.error)
        self.assertIsNone(task.queued)
        self.assertIsNone(task.started)
        self.assertIsNone(task.finished)
        self.assertEqual(task.status, Status.QUEUED)

    def test_run(self):
        """Tests running a task
        """
        now = utc.localize(datetime.utcnow())
        task = Task(taskid=uuid4(),
                    function_name='django_ztaskq.tests.hello_world',
                    args=("Joe",),
                    kwargs={},
                    queued=now)
        __, logger, logger_stream = self.get_logger()
        task.run(logger)
        self.assertEqual(task.return_value, "Hello world, I am Joe")
        self.assertIsNone(task.error)
        self.assertEqual(task.queued, now)
        self.assertGreaterEqual(task.started, now)
        self.assertGreaterEqual(task.finished, now)
        self.assertEqual(task.status, Status.COMPLETED)
        self.assertEqual(
            logger_stream.getvalue(),
            ("Executing task function (django_ztaskq.tests.hello_world)\n"
             "Task.run is calling %s(('Joe',), {})\n"
             "Successfully finished the function call.\n"
             "Called django_ztaskq.tests.hello_world successfully\n") % (
                repr(hello_world),
            )
        )

    def test_failure(self):
        """Tests a failing task
        """
        now = utc.localize(datetime.utcnow())
        task = Task(taskid=uuid4(),
                    function_name='django_ztaskq.tests.convert_int',
                    args=("This can't be an int",),
                    kwargs={},
                    queued=now)
        __, logger, logger_stream = self.get_logger()
        task.run(logger)
        self.assertIsNone(task.return_value)
        error_lines = task.error.splitlines()
        self.assertEqual(
            error_lines[0],
            'Traceback (most recent call last):'
        )
        self.assertEqual(
            error_lines[-1],
            ('ValueError: invalid literal for int() with base 10: '
             '"This can\'t be an int"')
        )
        self.assertEqual(task.queued, now)
        self.assertGreaterEqual(task.started, now)
        self.assertGreaterEqual(task.finished, now)
        self.assertEqual(task.status, Status.FAILED)
        self.assertEqual(
            logger_stream.getvalue(),
            ('Executing task function (django_ztaskq.tests.convert_int)\n'
             'Task.run is calling %s(("This can\'t be an int",), {})\n'
             'Error calling django_ztaskq.tests.convert_int. Details:\n'
             '%s\n') % (
                repr(convert_int), task.error
            )
        )


class WrappedCommand(Process):

    command_name = ''
    command_args = tuple()
    command_options = {}

    def __init__(self, *args, **kwargs):
        attrs = {
            'queue': kwargs.pop('queue'),
        }
        super(WrappedCommand, self).__init__(*args, **kwargs)
        self.__dict__.update(attrs)

    def on_load(self):
        pass

    def run(self):
        self.on_load()
        from django.core.management import call_command
        call_command(self.command_name, *self.command_args,
                     **self.command_options)


class WrappedWorker(WrappedCommand):

    command_name = 'workerd'
    command_args = ('test-worker-1',)

    def on_load(self):
        queue = self.queue
        patch(
            'django_ztaskq.management.commands.workerd.context',
            new=Context()
        ).start()
        patch(
            'django_ztaskq.management.commands.workerd.get_logger',
        ).start()
        # pylint: disable=W0613
        def notify_run(logger):
            queue.put(True)
            queue.close()
        def objects_get(*args, **kwargs):
            queue.put(kwargs['pk'])
            task = MagicMock()
            task.run = MagicMock(side_effect=notify_run)
            return task
        MockedTask = patch(
            'django_ztaskq.management.commands.workerd.Task').start()
        MockedTask.objects = MagicMock()
        MockedTask.objects.get = MagicMock(side_effect=objects_get)


class WorkerTest(TestCase):
    """Ensures the worker correctly handles messages
    """

    def setUp(self):
        self.queue = Queue()
        self.context = Context()
        self.socket = self.context.socket(PUSH)
        self.socket.bind(settings.ZTASK_WORKER_URL)
        self.worker = WrappedWorker(queue=self.queue)
        self.worker.start()

    def tearDown(self):
        self.worker.terminate()
        self.context.destroy()

    def test_exec(self):
        """Tests executing a task
        """
        uuid = str(uuid4())
        self.socket.send_pyobj((uuid,))
        self.assertEqual(
            self.queue.get(),
            uuid
        )
        self.assertTrue(self.queue.get())
        self.queue.close()


class DispatcherTest(TestCase):
    """Ensures the dispatcher "dispatches" correctly
    """

    def test_dispatch(self):
        """Tests dispatching a task
        """

    def test_delayed(self):
        """Tests dispatching a delayed task
        """

    def test_onload(self):
        """Tests onload calls
        """

    def test_leftover(self):
        """Tests enqueueing leftovers
        """
