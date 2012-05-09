import os

from django.http import HttpResponse
from django.shortcuts import render
from django.core.urlresolvers import reverse
from django_ztaskq.decorators import ztask

logs_dir = os.path.join(os.path.dirname(__file__), 'logs')


def home(request):
    return HttpResponse("""Click <a href="{0}">here</a> to go to a view that launches a ztask.""".format(reverse(launch_ztask)))


def launch_ztask(request):
    open(os.path.join(logs_dir, "webserver.log"), "w").write(
        "This is a message logged from the web server process: pid %d\n" % os.getpid())
    log_message.async()
    return HttpResponse("""This is another view.""")


@ztask()
def log_message():
    open(os.path.join(logs_dir, "workerd.log"), "w").write(
        "This is a message logged from a ztaskq workerd process: pid %d\n" % os.getpid())
