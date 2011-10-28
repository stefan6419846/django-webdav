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
    '''The provided os.path.join() does not work as desired. Any path starting with /
    will simply be returned rather than actually being joined with the other elements.'''
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
    '''Assuming base is the scheme and host (and perhaps path) we will join the remaining
    path elements to it.'''
    paths = safe_join(*paths)
    while base.endswith('/'):
        base = base[:-1]
    return base + paths

def ns_split(tag):
    '''Splits the namespace and property name from a clark notation property name.'''
    if tag.startswith("{") and "}" in tag:
        ns, name = tag.split("}", 1)
        return (ns[1:-1], name)
    return ("", tag)

def ns_join(ns, name):
    '''Joins a namespace and property name into clark notation.'''
    return '{%s:}%s' % (ns, name)

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


class HttpResponseConflict(HttpResponse):
    status_code = 409


class HttpResponseMediatypeNotSupported(HttpResponse):
    status_code = 415


class HttpResponsePreconditionFailed(HttpResponse):
    status_code = 412


class HttpResponseNotImplemented(HttpResponse):
    status_code = 501


class HttpResponseBadGateway(HttpResponse):
    status_code = 502


class DavAcl(object):
    '''Represents all the permissions that a user might have on a resource. This
    makes it easy to implement virtual permissions.'''
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
    '''Implements an interface to the file system. This can be subclassed to provide
    a virtual file system (like say in MySQL). This default implementation simply uses
    python's os library to do most of the work.'''
    def __init__(self, server, path):
        self.server = server
        self.root = server.get_root()
        # Trailing / messes with dirname and basename.
        while path.endswith('/'):
            path = path[:-1]
        self.path = path

    def get_path(self):
        '''Return the path of the resource relative to the root.'''
        return self.path

    def get_abs_path(self):
        '''Return the absolute path of the resource. Used internally to interface with
        an actual file system. If you override all other methods, this one will not
        be used.'''
        return safe_join(self.root, self.path)

    def isdir(self):
        '''Return True if this resource is a directory (collection in WebDAV parlance).'''
        return os.path.isdir(self.get_abs_path())

    def isfile(self):
        '''Return True if this resource is a file (resource in WebDAV parlance).'''
        return os.path.isfile(self.get_abs_path())

    def exists(self):
        '''Return True if this resource exists.'''
        return os.path.exists(self.get_abs_path())

    def get_name(self):
        '''Return the name of the resource (without path information).'''
        # No need to use absolute path here
        return os.path.basename(self.path)

    def get_dirname(self):
        '''Return the resource's parent directory's absolute path.'''
        return os.path.dirname(self.get_abs_path())

    def get_size(self):
        '''Return the size of the resource in bytes.'''
        return os.path.getsize(self.get_abs_path())

    def get_ctime_stamp(self):
        '''Return the create time as UNIX timestamp.'''
        return os.stat(self.get_abs_path()).st_ctime

    def get_ctime(self):
        '''Return the create time as datetime object.'''
        return datetime.datetime.fromtimestamp(self.get_ctime_stamp())

    def get_mtime_stamp(self):
        '''Return the modified time as UNIX timestamp.'''
        return os.stat(self.get_abs_path()).st_mtime

    def get_mtime(self):
        '''Return the modified time as datetime object.'''
        return datetime.datetime.fromtimestamp(self.get_mtime_stamp())

    def get_url(self):
        '''Return the url of the resource. This uses the request base url, so it
        is likely to work even for an overridden DavResource class.'''
        return url_join(self.server.request.get_base_url(), self.path)

    def get_parent(self):
        '''Return a DavResource for this resource's parent.'''
        return self.__class__(self.server, os.path.dirname(self.path))

    # TODO: combine this and get_children()
    def get_descendants(self, depth=1, include_self=True):
        '''Return an iterator of all descendants of this resource.'''
        if include_self:
            yield self
        # If depth is less than 0, then it started out as -1.
        # We need to keep recursing until we hit 0, or forever
        # in case of infinity.
        if depth != 0:
            for child in self.get_children():
                for desc in child.get_descendants(depth=depth-1, include_self=True):
                    yield desc

    # TODO: combine this and get_descendants()
    def get_children(self):
        '''Return an iterator of all direct children of this resource.'''
        for child in os.listdir(self.get_abs_path()):
            yield self.__class__(self.server, os.path.join(self.get_path(), child))

    def open(self, mode):
        '''Open the resource, mode is the same as the Python file() object.'''
        return file(self.get_abs_path(), mode)

    def delete(self):
        '''Delete the resource, recursive is implied.'''
        if self.isdir():
            for child in self.get_children():
                child.delete()
            os.rmdir(self.get_abs_path())
        elif self.isfile():
            os.remove(self.get_abs_path())

    def mkdir(self):
        '''Create a directory in the location of this resource.'''
        os.mkdir(self.get_abs_path())

    def copy(self, destination, depth=0):
        '''Called to copy a resource to a new location. Overwrite is assumed, the DAV server
        will refuse to copy to an existing resource otherwise. This method needs to gracefully
        handle a pre-existing destination of any type. It also needs to respect the depth 
        parameter. depth == -1 is infinity.'''
        if self.isdir():
            if destination.isfile():
                destination.delete()
            if not destination.isdir():
                destination.mkdir()
            # If depth is less than 0, then it started out as -1.
            # We need to keep recursing until we hit 0, or forever
            # in case of infinity.
            if depth != 0:
                for child in self.get_children():
                    child.copy(self.__class__(self.server, safe_join(destination.get_path(), child.get_name())), depth=depth-1)
        else:
            if destination.isdir():
                destination.delete()
            with destination.open('w') as dst:
                with self.open('r') as src:
                    shutil.copyfileobj(src, dst)

    def move(self, destination):
        '''Called to move a resource to a new location. Overwrite is assumed, the DAV server
        will refuse to move to an existing resource otherwise. This method needs to gracefully
        handle a pre-existing destination of any type.'''
        if destination.exists():
            destination.delete()
        if self.isdir():
            destination.mkdir()
            for child in self.get_children():
                child.move(self.__class__(self.server, safe_join(destination.get_path(), child.get_name())))
            self.delete()
        else:
            os.rename(self.get_abs_path(), destination.get_abs_path())

    def get_etag(self):
        '''Calculate an etag for this resource. The default implementation uses an md5 sub of the
        absolute path modified time and size. Can be overridden if resources are not stored in a
        file system. The etag is used to detect changes to a resource between HTTP calls. So this
        needs to change if a resource is modified.'''
        hash = hashcompat.md5_constructor()
        hash.update(self.get_abs_path().encode('utf-8'))
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

    def get_base(self):
        '''Assuming the view is configured via urls.py to pass the path portion using
        a regular expression, we can subtract the provided path from the full request
        path to determine our base. This base is what we can make all absolute URLs
        from.'''
        return self.META['PATH_INFO'][:-len(self.path)]

    def get_base_url(self):
        '''Build a base URL for our request. Uses the base path provided by get_base()
        and the scheme/host etc. in the request to build a URL that can be used to
        build absolute URLs for WebDAV resources.'''
        return self.build_absolute_uri(self.get_base())


class DavProperty(object):
    def __init__(self, server):
        self.server = server

    def get_properties(self, res, *names, **kwargs):
        names_only = kwargs.get('names_only', False)
        found, missing = [], []
        for name in names:
            if names_only:
                if name in DAV_LIVE_PROPERTIES:
                    found.append((name, None))
            else:
                value = None
                ns, bare_name = ns_split(name)
                if ns != 'DAV':
                    pass # TODO: support "dead" properties.
                else:
                    if bare_name == 'getetag':
                        value = res.get_etag()
                    elif bare_name == 'getcontentlength':
                        value = str(res.get_size())
                    elif bare_name == 'creationdate':
                        value = rfc3339_date(res.get_ctime_stamp())     # RFC3339:
                    elif bare_name == 'getlastmodified':
                        value = http_date(res.get_mtime_stamp())        # RFC1123:
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
    def __init__(self, server):
        self.server = server

    def acquire(self, url, type, scope, depth, owner, timeout):
        pass

    def release(self):
        pass


class DavServer(object):
    def __init__(self, request, path, property_class=DavProperty, resource_class=DavResource, lock_class=DavLock, acl_class=DavAcl):
        self.request = DavRequest(self, request, path)
        self.resource_class = resource_class
        self.acl_class = acl_class
        self.properties = property_class(self)
        self.locks = lock_class(self)

    def get_root(self):
        '''Return the root of the file system we wish to export. By default the root
        is read from the DAV_ROOT setting in django's settings.py. You can override
        this method to export a different directory (maybe even different per user).'''
        return getattr(settings, 'DAV_ROOT', None)

    def get_access(self, path):
        '''Return permission as DavAcl object. A DavACL should have the following attributes:
        read, write, delete, create, relocate, list. By default we implement a read-only
        system.'''
        return self.acl_class(list=True, read=True, all=False)

    def get_resource(self, path):
        '''Return a DavResource object to represent the given path.'''
        return self.resource_class(self, path)

    def get_response(self):
        handler = getattr(self, 'do' + self.request.method, None)
        if not callable(handler):
            return HttpResponseNotFound()
        return handler()

    def doGET(self, head=False):
        res = self.get_resource(self.request.path)
        acl = self.get_access(res.get_abs_path())
        if not head and res.isdir():
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
        res = self.get_resource(self.request.path)
        if res.isdir():
            return HttpResponseNotAllowed()
        if not res.get_parent().exists():
            return HttpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
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
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
        if not acl.delete:
            return HttpResponseForbidden()
        res.delete()
        response = HttpResponseNoContent()
        response['Date'] = http_date()
        return response

    def doMKCOL(self):
        res = self.get_resource(self.request.path)
        if res.exists():
            return HttpResponseNotAllowed()
        if not res.get_parent().exists():
            return HttpResponseConflict()
        length = self.request.META.get('CONTENT_LENGTH', 0)
        if length and int(length) != 0:
            return HttpResponseMediatypeNotSupported()
        acl = self.get_access(res.get_abs_path())
        if not acl.create:
            return HttpResponseForbidden()
        res.mkdir()
        return HttpResponseCreated()

    def doCOPY(self, move=False):
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HtpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
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
        dst = self.get_resource(dparts.path[len(self.request.get_base()):])
        if not dst.get_parent().exists():
            return HttpResponseConflict()
        overwrite = self.request.META.get('HTTP_OVERWRITE', 'T')
        if overwrite not in ('T', 'F'):
            return HttpResponseBadRequest('Overwrite header must be T or F.')
        overwrite = (overwrite == 'T')
        if not overwrite and dst.exists():
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
        dst_exists = dst.exists()
        if move:
            dst.delete()
            errors = res.move(dst)
        else:
            errors = res.copy(dst, depth=depth)
        if errors:
            response = HttpResponseMultiStatus()
        elif dst_exists:
            response = HttpResponseNoContent()
        else:
            response = HttpResponseCreated()
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
        res = self.get_resource(self.request.path)
        acl = self.get_access(res.get_abs_path())
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
        res = self.get_resource(self.request.path)
        if not res.exists():
            return HttpResponseNotFound()
        acl = self.get_access(res.get_abs_path())
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
        length = self.request.META.get('CONTENT_LENGTH', 0)
        if not length or int(length) == 0:
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
            found, missing = self.properties.get_properties(child, *props, names_only=names_only)
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
        return HttpResponseNotImplemented()
