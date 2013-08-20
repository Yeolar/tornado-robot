#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-12.  Yeolar <yeolar@gmail.com>
#

import logging

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from zmq.eventloop.ioloop import IOLoop


class Fetcher(object):

    def __init__(self, *args, **kwargs):
        self.identity = ''
        self.initialize(*args, **kwargs)

    def initialize(self, *args, **kwargs):
        pass

    def build_request(self, request):
        raise NotImplementedError()

    def prepare(self):
        pass

    def on_finish(self):
        pass

    def fetch(self, request, callback):
        raise NotImplementedError()

    def __call__(self, request, callback):
        self.prepare()
        self.fetch(request, callback)
        self._log(request)
        self.on_finish()

    def _log(self, request):
        logging.info('[%s] %s %s', self.identity, request.method, request.url)


class HTTPFetcher(Fetcher):

    def build_request(self, request):
        return HTTPRequest(
                url=request.url,
                method=request.method,
                body=request.body or None,
                connect_timeout=request.connect_timeout,
                request_timeout=request.request_timeout)

    def fetch(self, request, callback):
        client = AsyncHTTPClient(IOLoop.instance())
        client.fetch(self.build_request(request), callback)

