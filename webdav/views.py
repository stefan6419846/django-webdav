from django.http import HttpResponse
from webdav import *

def export(request, path, fs_class=None, prop_class=None):
    '''Default Django-WebDAV view.'''
    return DavRequest(request, path, fs_class=fs_class, prop_class=prop_class).get_response()

