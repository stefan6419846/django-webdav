import base64
from django.http import HttpResponse
from django.contrib.auth import authenticate, login

def httpbasic(realm="WebDAV"):
    def decorator(view):
        def wrapper(request, *args, **kwargs):
            if request.user.is_authenticated():
                return view(request, *args, **kwargs)
            auth = request.META.get('HTTP_AUTHORIZATION')
            if auth:
                auth = auth.split()
                if len(auth) == 2 and auth[0].lower() == 'basic':
                    username, password = base64.b64decode(auth[1]).split(':')
                    user = authenticate(realm=None, username=username, password=password)
                    if user is not None and user.is_active:
                        login(request, user)
                        return view(request, *args, **kwargs)
            response = HttpResponse(status=401)
            response['WWW-Authenticate'] = 'Basic realm="%s"' % realm
            return response
        return wrapper
    return decorator
