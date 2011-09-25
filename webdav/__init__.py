import os, datetime, mimetypes
from django.conf import settings
from django.http import HttpResponse, HttpResponseForbidden, Http404
from django.shortcuts import render_to_response

def safe_join(root, *paths):
    if not root.startswith('/'):
        root = '/' + root
    for path in paths:
        while root.endswith('/'):
            root = root[:-1]
        while path.startswith('/'):
            path = path[1:]
        root += '/' + path
    return root

class xml(object):
    def makeResponse():
        pass

class DavAcl(object):
    def __init__(self, read=True, write=True, delete=True, create=True, relocate=True, list=True, all=None):
        if not all is None:
            self.read = self.write = self.delete = \
            self.create = self.relocate = self.list = all
        self.read = read
        self.write = write
        self.delete = delete
        self.create = create
        self.relocate = relocate
        self.list = list

class DavStat(object):
    def __init__(self, request, path):
        root = request.fs.homedir(request.request.user)
        full_path = safe_join(root, path)
        self.path = path
        self.url = safe_join(request.base, path)
        self.url_up = safe_join(request.base, os.path.dirname(path))
        self.name = os.path.basename(full_path)
        self.dirname = os.path.dirname(full_path)
        self.isdir = os.path.isdir(full_path)
        self.isfile = os.path.isdir(full_path)
        self.size = os.path.getsize(full_path)
        self.mtime = datetime.datetime.fromtimestamp(os.stat(full_path).st_mtime)

class DavRequest(object):
    def __init__(self, request, path, fs_class=None, prop_class=None):
        self.request = request
        self.base = request.path[:-len(path)]
        if fs_class is None:
            fs_class = DavFileSystem
        self.fs = fs_class(self)
        if prop_class is None:
            prop_class = DavProperties
        self.prop = prop_class(self)
        self.path = path

    def doGET(self, head=False):
        root = self.fs.homedir(self.request.user)
        acl = self.fs.access(self.request.user, self.path)
        cwd = self.fs.stat(self, self.path)
        if cwd.isdir:
            if not acl.list:
                return HttpResponseForbidden()
            listing = self.fs.listdir(self.path)
            return render_to_response('webdav/index.html', { 'cwd': cwd, 'listing': listing })
        else:
            if not acl.read:
                return HttpResponseForbidden()
            if head:
                response =  HttpResponse(file(safe_join(root, self.path), 'r'))
                response['Content-Length'] = cwd.size
            else:
                response =  HttpResponse(file(safe_join(root, self.path), 'r'))
            response['Content-Type'] = mimetypes.guess_type(cwd.name)
            return response

    def doHEAD(self):
        return self.doGET(head=True)

    def doPOST(self):
        raise HttpResponse('Method not allowed: POST', status=405)

    def doPUT(self):
        if self.fs.exists(self.root):
            return HttpResponseForbidden()

    def doDELETE(self):
        if not self.fs.exists(self.root):
            raise Http404()
        if not self.fs.access(self.root).delete:
            return HttpResponseForbidden()
        self.fs.delete(self.root)
        return HttpResponse(xml.makeResponse(), status=200)

    def doMKCOL(self):
        pass

    def doCOPY(self):
        pass

    def doMOVE(self):
        pass

    def doLOCK(self):
        pass

    def doUNLOCK(self):
        pass

    def doOPTIONS(self):
        pass

    def doPROPFIND(self):
        pass

    def doPROPPATCH(self):
        pass

    def get_response(self):
        handler = getattr(self, 'do' + self.request.method, None)
        if not handler:
            raise Http404()
        return handler()


class DavProperties(object):
    def __init__(self, request):
        selfrequest = request


class DavFileSystem(object):
    def __init__(self, request):
        self.request = request

    def homedir(self, user):
        root = getattr(settings, 'DAV_ROOT', None)
        if not root:
            raise Http404()
        return root

    def access(self, user, path):
        '''Return permission as tuple (read, write, delete, create, relocate, list).'''
        return DavAcl(all=False)

    def listdir(self, path):
        root = self.homedir(self.request.request.user)
        for child in os.listdir(safe_join(root, path)):
            yield self.stat(root, safe_join(path, child))

    def stat(self, root, path):
        return DavStat(self.request, path)

    def open(self, path, mode):
        pass

    def exists(self, path):
        pass

    def remove(self, path):
        pass

    def mkdir(self, path):
        pass

    def touch(self, path):
        pass
