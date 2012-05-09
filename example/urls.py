from django.conf.urls.defaults import patterns, include, url


urlpatterns = patterns('',
    (r'^$', 'views.home'),
    (r'^launch_ztask/$', 'views.launch_ztask'),
)
