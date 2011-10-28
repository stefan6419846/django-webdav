from django_webdav.views import export as webdav_export
from samples.advanced.server import AdvancedDavServer

def export(request, path):
    return webdav_export(request, path, server_class=AdvancedDavServer)