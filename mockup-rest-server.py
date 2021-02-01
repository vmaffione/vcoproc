#!/usr/bin/python3
#
# Author: Vincenzo M (2020)
#

# Examples with CURL:
#     $ curl -X POST -H 'Content-Type: application/json' --data-binary '{}' localhost:5000/example
#     $ curl -X GET localhost:5000/example

import myhttp
import random

# Force python3, because python3 supports os.scandir() to read directory
# efficiently.
if myhttp.python2():
        print('python2 is not supported. Please run this program with python3.')
        quit(1)

from xml.dom import minidom
import threading
import logging
import argparse
import json
import uuid
import time
import sys
import os
import re

class TestRequestHandler(myhttp.RequestHandler):
        def __init__(self, request, client_address, server, args):
                self.args = args
                super(myhttp.RequestHandler, self).__init__(request, client_address, server)

        def set_headers(self, code, content_type = ''):
                self.send_response(code)
                if content_type:
                        self.send_header('Content-type', content_type)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

        def write_json(self, js):
                msg = json.dumps(js, ensure_ascii = False)
                self.wfile.write(bytes(msg, 'utf8'))

        def return_json(self, js):
                if 'reason' in js:
                    rcode = 400
                else:
                    rcode = 200
                self.set_headers(rcode, 'application/json')
                self.write_json(js)

        def return_error(self, code, reason = "Invalid request"):
                self.set_headers(code, 'application/json')
                self.write_json({ 'status': 'ERROR', 'reason': reason })

        def do_HEAD(self):
                self.return_error(404)

        def do_GET(self):
                # Extract path
                ppath = myhttp.myurlparse(self.path)
                p = myhttp.url_unescape(ppath.path)

                if p.startswith('/ping'):
                        self.do_GET_ping(p)
                else:
                        self.return_error(404)

        def do_GET_ping(self, path):
                jsresp = {'status': 'COMPLETE'}
                return jsresp

        def do_POST(self):
                ppath = myhttp.myurlparse(self.path)
                p = myhttp.url_unescape(ppath.path)

                # We need at least Content-Length to know the size of the JSON
                # request body.
                if 'Content-Length' not in self.headers:
                        return self.return_error(400, "Missing Content-Length HTTP header")
                try:
                        content_length = int(self.headers['Content-Length'])
                except:
                        return self.return_error(400, "Invalid Content-Length header value")

                # Read the request body and parse it as JSON.
                reqbody = self.rfile.read(content_length)
                reqbody = reqbody.decode('utf8')
                print(f"Received request {reqbody}")
                try:
                        jsreq = json.loads(reqbody)
                except Exception as e:
                        return self.return_error(400, "Request is not in JSON format")

                #if 'api_key' not in jsreq:
                #        return self.return_error(400, "Missing API key")
                #print("API key is {}".format(jsreq['api_key']), flush = True)

                # Demux to the proper POST handler.
                if p == '/process':
                        return self.do_POST_process(p, jsreq)
                else:
                        return self.return_error(404)

        def do_POST_process(self, path, jsreq):
                time.sleep(random.uniform(0.01, 1.1))
                if random.uniform(0.0, 1.0) < 0.2:
                    jsresp = {'status': 'ERROR', 'reason': 'You unlucky'}
                else:
                    if random.uniform(0.0, 1.0) < 0.8:
                        jsresp = {'status': 'COMPLETE'}
                    else:
                        jsresp = {'status': 'NOMETADATA'}
                return self.return_json(jsresp)


if __name__ == '__main__':
        description = "REST test server"
        epilog = "2020 Vincenzo M"

        # General options.
        argparser = argparse.ArgumentParser(description = description,
                                            epilog = epilog)
        argparser.add_argument('-v', '--verbose', help = "Be a little bit verbose",
                                action = 'store_true')
        argparser.add_argument('--pretty-json', help = "Pretty print JSON output",
                                action = 'store_true')
        argparser.add_argument('--port',
                               help = "TCP listening port for the REST API",
                               type = int, default = 5000)

        args = argparser.parse_args()

        # Setup logging
        logging.basicConfig(format = '')
        lg = logging.getLogger('ca')
        if args.verbose:
                lg.setLevel(logging.INFO)
        else:
                lg.setLevel(logging.WARNING)



        gs = myhttp.GracefulStopper()

        # Create a multi-thread server which serves HTTP requests as
        # specified by the TestRequestHandler class.
        rest_server_address = ('0.0.0.0', args.port)
        httpd = myhttp.ThreadedServer(rest_server_address,
                                       myhttp.ServerFactory(TestRequestHandler,
                                                args = args))
        # Create a single thread associated to the multi-threaded server.
        # The thread will accept() client requests and dispatch each
        # request to a dedicated thread.
        httpd_thread = threading.Thread(target = httpd.serve_forever)
        #httpd_thread.daemon = True

        # Start the thread.
        httpd_thread.start()

        while not gs.should_stop():
                time.sleep(3)

        # Stop the REST server and remove the temporary directory.
        httpd.shutdown()
        httpd.server_close()
        httpd_thread.join()
