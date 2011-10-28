from django_webdav.views import export as webdav_export
from django_basic.decorators import httpbasic
from samples.advanced.server import AdvancedDavServer

@httpbasic(realm='WebDAV')
def export(request, path):
    return webdav_export(request, path, server_class=AdvancedDavServer)
