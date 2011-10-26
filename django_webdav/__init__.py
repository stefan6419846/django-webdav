import os, datetime, mimetypes, time, shutil, urllib, urlparse
from xml.etree import ElementTree
from django.conf import settings
from django.http import HttpResponse, HttpResponseForbidden, HttpResponseNotFound, \
HttpResponseNotAllowed, HttpResponseBadRequest, HttpResponseNotModified
from django.utils import hashcompat
from django.utils.http import http_date
from django.utils.encoding import smart_unicode
from django.shortcuts import render_to_response

DAV_LIVE_PROPERTIES = (
    '{DAV:}getetag', '{DAV:}getcontentlength', '{DAV:}creationdate',
    '{DAV:}getlastmodified', '{DAV:}resourcetype', '{DAV:}displayname'
)

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

def rfc3339_date(date):
  if not date:
      return ''
  if not isinstance(date, datetime.date):
      date = datetime.date.fromtimestamp(date)
  date = date + datetime.timedelta(seconds=-time.timezone)
  if time.daylight:
    date += datetime.timedelta(seconds=time.altzone)
  return date.strftime('%Y-%m-%dT%H:%M:%SZ')


class HttpResponseCreated(HttpResponse):
    status_code = 201


class HttpResponseNoContent(HttpResponse):
    status_code = 204


class HttpResponseMultiStatus(HttpResponse):
    status_code = 207


class HttpResponseNotAllowed(HttpResponse):
    status_code = 405


class HttpResponsePreconditionFailed(HttpResponse):
    status_code = 412


class HttpResponseNotImplemented(HttpResponse):
    status_code = 501


class HttpResponseBadGateway(HttpResponse):
    status_code = 502


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
        path = self.path
        while path.endswith('/'):
            path = path[:-1]
        return os.path.basename(path)

    def get_dirname(self):
        return os.path.dirname(self.get_abs_path())

    def get_size(self):
        return os.path.getsize(self.get_abs_path())

    def get_ctime_stamp(self):
        return os.stat(self.get_abs_path()).st_ctime

    def get_ctime(self):
        return datetime.datetime.fromtimestamp(self.get_ctime_stamp())

    def get_mtime_stamp(self):
        return os.stat(self.get_abs_path()).st_mtime

    def get_mtime(self):
        return datetime.datetime.fromtimestamp(self.get_mtime_stamp())

    def get_url(self):
        return url_join(self.request.get_base_url(), self.path)

    def get_parent(self):
        return self.__class__(self.request, self.root, os.path.dirname(self.path))

    def get_descendants(self, depth=1, include_self=True):
        if include_self:
            yield self
        if depth != 0:
            for child in self.get_children():
                for desc in child.get_descendants(depth=depth-1, include_self=True):
                    yield desc

    def get_children(self):
        for child in os.listdir(self.get_abs_path()):
            yield self.__class__(self.request, self.root, os.path.join(self.get_name(), child))

    def open(self, mode):
        return file(self.get_abs_path(), mode)

    def delete(self):
        if self.isdir():
            for child in self.get_children():
                child.delete()
            os.rmdir(self.get_abs_path())
        elif self.isfile():
            os.remove(self.get_abs_path())

    def mkdir(self):
        os.mkdir(self.get_abs_path())

    def touch(self):
        os.close(os.open(self.get_abs_path()))

    def copy(self, destination, depth=0):
        if self.isdir():
            destination.mkdir()
            if depth > 0:
                for child in self.get_children():
                    child.copy(safe_join(destination.get_abs_path(), child.get_name()), depth=depth-1)
        else:
            with destination.open('w') as dst:
                with self.open('r') as src:
                    shutil.copyfileobj(src, dst)

    def move(self, destination):
        if self.isdir():
            destination.mkdir()
            for child in self.get_children():
                child.move(safe_join(destination.get_abs_path(), child.get_name()))
            self.delete()
        else:
            os.rename(self.get_abs_path(), destination.get_abs_path())

    def get_etag(self):
        hash = hashcompat.md5_constructor()
        hash.update(self.get_abs_path())
        hash.update(str(self.get_mtime_stamp()))
        hash.update(str(self.get_size()))
        return hash.hexdigest()


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
    st_class = DavResource

    def __init__(self, request):
        self.request = request

    def get_root(self):
        return getattr(settings, 'DAV_ROOT', None)

    def access(self, path):
        '''Return permission as tuple (read, write, delete, create, relocate, list).'''
        return DavAcl(all=False)

    def stat(self, path):
        return self.st_class(self.request, self.get_root(), path)


class DavProperties(object):
    def __init__(self, request):
        self.request = request

    def get_properties(self, res, *names, **kwargs):
        names_only = kwargs.get('names_only', False)
        found, missing = [], []
        for name in names:
            if names_only:
                if name in DAV_LIVE_PROPERTIES:
                    found.append((name, None))
            else:
                value = None
                ns, bare_name = split_ns(name)
                if bare_name == 'getetag':
                    value = res.get_etag()
                elif bare_name == 'getcontentlength':
                    value = str(res.get_size())
                elif bare_name == 'creationdate':
                    # RFC3339:
                    value = rfc3339_date(res.get_ctime_stamp())
                elif bare_name == 'getlastmodified':
                    # RFC1123:
                    value = http_date(res.get_mtime_stamp())
                elif bare_name == 'resourcetype':
                    if res.isdir():
                        value = ElementTree.Element("{DAV:}collection")
                    else:
                        value = ''
                elif bare_name == 'displayname':
                    value = res.get_name()
                elif bare_name == 'href':
                    value = res.get_url()
                if value is None:
                    missing.append(name)
                else:
                    found.append((name, value))
        return found, missing


class DavLock(object):
    def __init__(self, request):
        self.request = request

    def acquire(self, url, type, scope, depth, owner, timeout):
        pass

    def release(self):
        pass


class DavServer(object):
    fs_class = DavFileSystem
    pm_class = DavProperties
    lk_class = DavLock

    def __init__(self, request, path):
        self.request = DavRequest(self, request, path)
        self.fs = self.fs_class(self.request)
        self.pm = self.pm_class(self.request)
        self.lk = self.lk_class(self.request)

    def doGET(self, head=False):
        res = self.fs.stat(self.request.path)
        acl = self.fs.access(res.get_abs_path())
        if res.isdir():
            if not acl.list:
                return HttpResponseForbidden()
            return render_to_response('webdav/index.html', { 'res': res })
        else:
            if not acl.read:
                return HttpResponseForbidden()
            if head and res.exists():
                response = HttpResponse()
            elif head:
                response = HttpResponseNotFound()
            else:
                response =  HttpResponse(res.open('r'))
            if res.exists():
                response['Content-Type'] = mimetypes.guess_type(res.get_name())
                response['Content-Length'] = res.get_size()
                response['Last-Modified'] = http_date(res.get_mtime_stamp())
                response['ETag'] = res.get_etag()
            response['Date'] = http_date()
        return response

    def doHEAD(self):
        return self.doGET(head=True)

    def doPOST(self):
        return HttpResponseNotAllowed('POST method not allowed')

    def doPUT(self):
        res = self.fs.stat(self.request.path)
        if res.isdir():
            return HttpResponseNotAllowed()
        if not res.get_parent().exists():
            return HttpResponseNotFound()
        acl = self.fs.access(res.get_abs_path())
        if not acl.write:
            return HttpResponseForbidden()
        created = not res.exists()
        with res.open('w') as f:
            shutil.copyfileobj(self.request, f)
        if created:
            return HttpResponseCreated()
        else:
            return HttpResponseNoContent()

    def doDELETE(self):
        res = self.fs.stat(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        acl = self.fs.access(res.get_abs_path())
        if not acl.delete:
            return HttpResponseForbidden()
        res.delete()
        response = HttpResponseNoContent()
        response['Date'] = http_date()
        return response

    def doMKCOL(self):
        res = self.fs.stat(self.request.path)
        if res.exists():
            return HttpResponseNotAllowed()
        acl = self.fs.access(res.get_abs_path())
        if not acl.create:
            return HttpResponseForbidden()
        res.mkdir()
        return HttpResponseCreated()

    def doCOPY(self, move=False):
        res = self.fs.stat(self.request.path)
        if not res.exists():
            return HtpResponseNotFound()
        acl = self.fs.access(res.get_abs_path())
        if not acl.relocate:
            return HttpResponseForbidden()
        dst = urllib.unquote(self.request.META.get('HTTP_DESTINATION', ''))
        if not dst:
            return HttpResponseBadRequest('Destination header missing.')
        dparts = urlparse.urlparse(dst)
        # TODO: ensure host and scheme portion matches ours...
        sparts = urlparse.urlparse(self.request.build_absolute_uri())
        if sparts.scheme != dparts.scheme or sparts.netloc != dparts.netloc:
            return HttpResponseBadGateway('Source and destination must have the same scheme and host.')
        # adjust path for our base url:
        dst = self.fs.stat(uparts.path[len(self.request.get_base()):])
        overwrite = self.request.META.get('HTTP_OVERWRITE', 'T')
        if overwrite not in ('T', 'F'):
            return HttpResponseBadRequest('Overwrite header must be T or F.')
        overwrite = (overwrite == 'T')
        if dst.isdir():
            dst = self.fs.stat(safe_join(dst.get_path(), res.get_name()))
        if not overwrite and dst.isfile():
            return HttpResponsePreconditionFailed('Destination exists and overwrite False.')
        depth = self.request.META.get('HTTP_DEPTH', 'infinity').lower()
        if not depth in ('0', '1', 'infinity'):
            return HttpResponseBadRequest('Invalid depth header value %s' % depth)
        if depth == 'infinity':
            depth = -1
        else:
            depth = int(depth)
        if move and depth != -1:
            return HttpResponseBadRequest()
        if depth not in (0, -1):
            return HttpResponseBadRequest()
        if dst.exists():
            response = HttpResponseNoContent()
        else:
            response = HttpResponseCreated()
        if move:
            dst.delete()
            errors = res.move(dst)
        else:
            errors = res.copy(dst, depth=depth)
        if errors:
            response = HttpResponseMultiStatus()
        return response

    def doMOVE(self):
        return self.doCOPY(move=True)

    def doLOCK(self):
        return HttpResponseNotImplemented()

    def doUNLOCK(self):
        return HttpResponseNotImplemented()

    def doOPTIONS(self):
        response = HttpResponse(mimetype='text/html')
        response['DAV'] = '1,2'
        response['Date'] = http_date()
        if self.request.path in ('/', '*'):
            return response
        res = self.fs.stat(self.request.path)
        acl = self.fs.access(res.get_abs_path())
        if not res.exists():
            res = res.get_parent()
            if not res.isdir():
                return HttpResponseNotFound()
            response['Allow'] = 'OPTIONS PUT MKCOL'
        elif res.isdir():
            response['Allow'] = 'OPTIONS HEAD GET DELETE PROPFIND PROPPATCH COPY MOVE LOCK UNLOCK'
        else:
            response['Allow'] = 'OPTIONS HEAD GET PUT DELETE PROPFIND PROPPATCH COPY MOVE LOCK UNLOCK'
            response['Allow-Ranges'] = 'bytes'
        return response

    def doPROPFIND(self):
        res = self.fs.stat(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        acl = self.fs.access(res.get_abs_path())
        if not acl.list:
            return HttpResponseForbidden()
        depth = self.request.META.get('HTTP_DEPTH', 'infinity').lower()
        if not depth in ('0', '1', 'infinity'):
            return HttpResponseBadRequest('Invalid depth header value %s' % depth)
        if depth == 'infinity':
            depth = -1
        else:
            depth = int(depth)
        names_only, props = False, []
        if int(self.request.META.get('CONTENT_LENGTH', 0)) == 0:
            # Allow empty request, must be treated as request for all properties.
            props = DAV_LIVE_PROPERTIES
        else:
            for ev, el in ElementTree.iterparse(self.request):
                if el.tag == '{DAV:}allprop':
                    if props:
                        return HttpResponseBadRequest()
                elif el.tag == '{DAV:}propname':
                    names_only = True
                elif el.tag == '{DAV:}prop':
                    if names_only:
                        return HttpResponseBadRequest()
                    for pr in el:
                        props.append(pr.tag)
        msr = ElementTree.Element('{DAV:}multistatus')
        for child in res.get_descendants(depth=depth, include_self=True):
            response = ElementTree.SubElement(msr, '{DAV:}response')
            ElementTree.SubElement(response, '{DAV:}href').text = child.get_url()
            found, missing = self.pm.get_properties(child, *props, names_only=names_only)
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
        response = HttpResponseMultiStatus(ElementTree.tostring(msr, 'UTF-8'), mimetype='application/xml')
        response['Date'] = http_date()
        return response

    def doPROPPATCH(self):
        pass

    def get_response(self):
        handler = getattr(self, 'do' + self.request.method, None)
        if not handler:
            return HttpResponseNotFound()
        return handler()
