from django.http import HttpResponse
from django_webdav import DavServer

def export(request, path, server_class=DavServer):
    '''Default Django-WebDAV view.'''
    return server_class(request, path).get_response()

