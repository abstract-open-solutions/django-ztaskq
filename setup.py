#!/usr/bin/env python
try:
    from setuptools import setup
except:
    from distutils.core import setup
import django_ztaskq as distmeta

setup(
    version=distmeta.__version__,
    description=distmeta.__doc__,
    author=distmeta.__author__,
    author_email=distmeta.__contact__,
    url=distmeta.__homepage__,
    #
    name='django-ztaskq',
    packages=['django_ztaskq'],
    install_requires=[
        'django-picklefield',
        'pyzmq',
    ]
)
