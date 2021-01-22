import hashlib
import signal
import sys

def python2():
        return sys.version_info[0] <= 2

if python2():
        from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
        from SocketServer import ThreadingMixIn
        import urlparse
        import urllib
else:
        from http.server import BaseHTTPRequestHandler, HTTPServer
        from socketserver import ThreadingMixIn
        import urllib.parse


def str2buf(s):
    if python2():
        return s
    return bytes(s, 'ascii')


def buf2str(b):
    if python2():
        return b
    return b.decode('ascii')


def hash(s):
    return int(hashlib.md5(str2buf(s)).hexdigest(), 16)


def myurlparse(path):
        if python2():
                return urlparse.urlparse(path)
        return urllib.parse.urlparse(path)


class ThreadedServer(ThreadingMixIn, HTTPServer):
        pass


class RequestHandler(BaseHTTPRequestHandler, object):
        pass


def ServerFactory(http_request_handler, **kwargs):
        class ServerWithArgs(http_request_handler, object):
                def __init__(self, request, client_address, server):
                        super(ServerWithArgs, self).__init__(request, client_address, server,
                                        **kwargs)

        return ServerWithArgs


# Signal handlers for SIGINT (CTRL-C) and SIGTERM (e.g., sent by systemd).
# On a signal, a flag is set so that should_stop() returns true
class GracefulStopper:
        def __init__(self):
                self.stop = False
                signal.signal(signal.SIGTERM, self.handler)
                signal.signal(signal.SIGINT, self.handler)

        def handler(self, signum, frame):
                self.stop = True

        def force_stop(self):
                self.stop = True

        def should_stop(self):
                return self.stop


def html_unescape(s):
        if python2():
                import HTMLParser
                return HTMLParser.HTMLParser().unescape(s)
        else:
                import html
                return html.unescape(s)

def url_escape(s):
        if python2():
                return urllib.quote(s)
        else:
                return urllib.parse.quote(s)

def url_unescape(s):
        if python2():
                return urllib.unquote(s)
        else:
                return urllib.parse.unquote(s)
