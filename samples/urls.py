from django.conf.urls.defaults import *
from samples.custom.server import CustomDavServer

urlpatterns = patterns('',
    # This will simply export the directory configured by DAV_ROOT in settings.py
    (r'^simple(?P<path>.*)$', 'django_webdav.views.export'),
    # This customized version will use a DavServer subclass.
    # This would be useful if authentication is being done via middlware.
    (r'^custom(?P<path>.*)$', 'django_webdav.views.export', { 'server_class': CustomDavServer }),
    # This more advanced version will use a customized view.
    # This would be useful if authentication is being done via decorators.
    (r'^advanced(?P<path>.*)$', 'samples.advanced.views.export'),
)
