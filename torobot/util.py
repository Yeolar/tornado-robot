#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-27.  Yeolar <yeolar@gmail.com>
#

from tornado.escape import utf8
from zmq.utils import jsonapi


class Request(object):

    def __init__(self, url, method='GET', body=None,
            connect_timeout=20.0, request_timeout=20.0):
        self.url = url
        self.method = method
        self.body = utf8(body)
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

    def serialize(self):
        return jsonapi.dumps(self.__dict__)

    @staticmethod
    def deserialize(s):
        return Request(**jsonapi.loads(s))


class Response(object):

    def __init__(self, request, code, request_time=None, error=None):
        self.request = request
        self.code = code
        self.request_time = request_time
        self.error = error

    def serialize(self):
        d = self.__dict__
        d['request'] = d['request'].__dict__
        return jsonapi.dumps(d)

    @staticmethod
    def deserialize(s):
        d = jsonapi.loads(s)
        d['request'] = Request(**d['request'])
        return Response(**d)

