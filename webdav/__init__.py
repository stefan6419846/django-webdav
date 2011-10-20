import os, datetime, mimetypes
from xml.etree import ElementTree
from django.conf import settings
from django.http import HttpResponse, HttpResponseForbidden, Http404
from django.utils.http import http_date
from django.utils.encoding import smart_unicode
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

def url_join(base, *paths):
    paths = safe_join(*paths)
    while base.endswith('/'):
        base = base[:-1]
    return base + paths

def split_ns(tag):
    if tag.startswith("{") and "}" in tag:
        ns, name = tag.split("}", 1)
        return (ns[1:], name)
    return ("", tag)

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

class DavResource(object):
    def __init__(self, request, root, path):
        self.request = request
        self.root = root
        self.path = path

    def get_path(self):
        return self.path

    def get_abs_path(self):
        return safe_join(self.root, self.path)

    def isdir(self):
        return os.path.isdir(self.get_abs_path())

    def isfile(self):
        return os.path.isfile(self.get_abs_path())

    def exists(self):
        return os.path.exists(self.get_abs_path())

    def get_name(self):
        return os.path.basename(self.path)

    def get_dirname(self):
        return os.path.dirname(self.get_abs_path())

    def get_size(self):
        return os.path.getsize(self.get_abs_path())

    def get_mtime(self):
        return datetime.datetime.fromtimestamp(os.stat(self.get_abs_path()).st_mtime)

    def get_url(self):
        return url_join(self.request.get_base_url(), self.path)

    def get_parent(self):
        return self.__class__(self.request, self.root, os.path.dirname(self.path))

    def get_descendants(self, depth=1, include_self=True):
        if include_self:
            yield self
        if depth != 0:
            for child in self.listdir():
                for desc in child.get_descendants(depth=depth-1, include_self=True):
                    yield desc

    def listdir(self):
        for child in os.listdir(self.get_abs_path()):
            yield self.__class__(self.request, self.root, os.path.join(self.get_name(), child))

    def get_properties(self, *names, **kwargs):
        names_only = kwargs.get('names_only', False)
        found, missing = [], []
        for name in names:
            if names_only:
                found.append((name, None))
            else:
                ns, name = split_ns(name)
                if name == 'getlastmodified':
                    value = self.get_mtime()
                if name == 'resourcetype':
                    if self.isdir():
                        value = ElementTree.Element("{DAV:}collection")
                    else:
                        value = ''
                else:
                    value = 'foo'
                found.append((name, value))
        return found, missing

    def open(self, mode):
        return file(self.get_abs_path(), mode)

    def remove(self, path):
        os.remove(self.get_abs_path())

    def mkdir(self, path):
        os.mkdir(self.get_abs_path())

    def touch(self, path):
        os.close(os.open(self.get_abs_path()))


class DavRequest(object):
    '''Wraps a Django request object, and extends it with some WebDAV
    specific methods.'''
    def __init__(self, server, request, path):
        self.server = server
        self.request = request
        self.path = path

    def __getattr__(self, name):
        return getattr(self.request, name)

    def get_root(self):
        return self.server.fs.get_root()

    def get_base(self):
        return self.META['PATH_INFO'][:-len(self.path)]

    def get_base_url(self):
        return self.build_absolute_uri(self.get_base())


class DavFileSystem(object):
    stat_class = DavResource

    def __init__(self, request):
        self.request = request

    def get_root(self):
        return getattr(settings, 'DAV_ROOT', None)

    def access(self, path):
        '''Return permission as tuple (read, write, delete, create, relocate, list).'''
        return DavAcl(all=False)

    def stat(self, path):
        return self.stat_class(self.request, self.get_root(), path)


class DavProperties(object):
    def __init__(self, request):
        selfrequest = request


class DavServer(object):
    fs_class = DavFileSystem
    prop_class = DavProperties

    def __init__(self, request, path):
        self.request = DavRequest(self, request, path)
        self.fs = self.fs_class(self.request)
        self.prop = self.prop_class(self.request)

    def doGET(self, head=False):
        acl = self.fs.access(self.request.path)
        cwd = self.fs.stat(self.request.path)
        if cwd.isdir():
            if not acl.list:
                return HttpResponseForbidden()
            return render_to_response('webdav/index.html', { 'cwd': cwd, 'listing': cwd.listdir() })
        else:
            if not acl.read:
                return HttpResponseForbidden()
            if head:
                response =  HttpResponse()
                response['Content-Length'] = cwd.size
            else:
                response =  HttpResponse(cwd.open('r'))
            response['Content-Type'] = mimetypes.guess_type(cwd.get_name())
            return response

    def doHEAD(self):
        return self.doGET(head=True)

    def doPOST(self):
        raise HttpResponse('Method not allowed: POST', status=405)

    def doPUT(self):
        acl = self.fs.access(self.request.path)
        cwd = self.fs.stat(self.request.path)
        if cwd.exists() or not acl.upload:
            return HttpResponseForbidden()
        if not cwd.get_parent().exists():
            raise Http404()
        with cwd.open('w') as f:
            pass # TODO: write file contents

    def doDELETE(self):
        cwd = self.fs.stat(self.request.path)
        if not cwd.exists():
            raise Http404()
        acl = self.fs.access(self.request.path)
        if not acl.delete:
            return HttpResponseForbidden()
        cwd.delete(self.path)
        return HttpResponse(xml.makeResponse(), status=200)

    def doMKCOL(self):
        acl = self.fs.access(self.path)
        cwd = self.fs.stat(self, self.path)
        if not acl.create:
            return HttpResponseForbidden()
        cwd.mkdir()

    def doCOPY(self):
        pass

    def doMOVE(self):
        pass

    def doLOCK(self):
        pass

    def doUNLOCK(self):
        pass

    def doOPTIONS(self):
        response = HttpResponse()
        response['Content-Type'] =  'text/html'
        response['Content-Length'] = '0'
        response['DAV'] = '1,2'
        response['Date'] = http_date()
        if self.request.path in ('/', '*'):
            return response
        acl = self.fs.access(self.request.path)
        cwd = self.fs.stat(self.request.path)
        if not cwd.exists():
            cwd = cwd.get_parent()
            if not cwd.isdir():
                raise Http404()
            response['Allow'] = 'OPTIONS PUT MKCOL'
        elif cwd.isdir():
            response['Allow'] = 'OPTIONS HEAD GET DELETE PROPFIND PROPPATCH COPY MOVE LOCK UNLOCK'
        else:
            response['Allow'] = 'OPTIONS HEAD GET PUT DELETE PROPFIND PROPPATCH COPY MOVE LOCK UNLOCK'
            response['Allow-Ranges'] = 'bytes'
        return response

    def doPROPFIND(self):
        # <?xml version="1.0" encoding="utf-8"?>
        # <propfind xmlns="DAV:"><prop>
        # <getetag xmlns="DAV:"/>
        # <getcontentlength xmlns="DAV:"/>
        # <creationdate xmlns="DAV:"/>
        # <getlastmodified xmlns="DAV:"/>
        # <resourcetype xmlns="DAV:"/>
        # <executable xmlns="http://apache.org/dav/props/"/>
        # </prop></propfind>
        acl = self.fs.access(self.request.path)
        cwd = self.fs.stat(self.request.path)
        if not cwd.exists():
            raise Http404()
        depth = self.request.META.get('HTTP_DEPTH', 'infinity').lower()
        if not depth in ('0', '1', 'infinity'):
            return HttpResponse('Invalid depth header value %s' % depth, status=400)
        if depth == 'infinity':
            depth = -1
        else:
            depth = int(depth)
        names_only, props = False, []
        for ev, el in ElementTree.iterparse(self.request):
            if el.tag == '{DAV:}allprop':
                if props:
                    return HttpResponse(status=400)
            elif el.tag == '{DAV:}propname':
                names_only = True
            elif el.tag == '{DAV:}prop':
                if names_only:
                    return HttpResponse(status=400)
                for pr in el:
                    props.append(pr.tag)
        msr = ElementTree.Element('{DAV:}multistatus')
        for child in cwd.get_descendants(depth=depth, include_self=True):
            response = ElementTree.SubElement(msr, '{DAV:}response')
            ElementTree.SubElement(response, '{DAV:}href').text = child.get_url()
            found, missing = child.get_properties(*props, names_only=names_only)
            if found:
                propstat = ElementTree.SubElement(response, '{DAV:}propstat')
                ElementTree.SubElement(propstat, '{DAV:}status').text = 'HTTP/1.1 200 OK'
                for name, value in found:
                    prop = ElementTree.SubElement(propstat, '{DAV:}prop')
                    prop = ElementTree.SubElement(prop, name)
                    if ElementTree.iselement(value):
                        prop.append(value)
                    elif value:
                        prop.text = smart_unicode(value)
            if missing:
                propstat = ElementTree.SubElement(response, '{DAV:}propstat')
                ElementTree.SubElement(propstat, '{DAV:}status').text = 'HTTP/1.1 404 Not Found'
                for name in missing:
                    prop = ElementTree.SubElement(propstat, '{DAV:}prop')
                    prop = ElementTree.SubElement(prop, name)
        response = HttpResponse(ElementTree.tostring(msr, 'UTF-8'), status=207, mimetype='application/xml')
        response['Date'] = http_date()
        return response

    def doPROPPATCH(self):
        pass

    def get_response(self):
        handler = getattr(self, 'do' + self.request.method, None)
        if not handler:
            raise Http404()
        return handler()
