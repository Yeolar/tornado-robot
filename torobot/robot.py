#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-11.  Yeolar <yeolar@gmail.com>
#

import httplib
import logging
import re
import urlparse

from tornado.web import URLSpec
from tornado.escape import _unicode, url_unescape


class ResponseHandler(object):

    SUPPORTED_METHODS = ('GET', 'POST')

    def __init__(self, robot, response, **kwargs):
        self.robot = robot
        self.response = response
        self.request = response.request
        self._finished = False
        self._auto_finish = True
        self._status_code = response.code
        self.initialize(**kwargs)

    def initialize(self):
        pass

    def on_get(self, *args, **kwargs):
        raise OnResponseError(405)

    def on_post(self, *args, **kwargs):
        raise OnResponseError(405)

    def on_error(self):
        pass

    def prepare(self):
        pass

    def on_finish(self):
        pass

    def next(self):
        pass

    def decode_argument(self, value):
        return _unicode(value)

    def finish(self):
        if self._finished:
            raise RuntimeError('finish() called twice.  May be caused '
                               'by using async operations without the '
                               '@asynchronous decorator.')
        self._log()
        self._finished = True
        self.on_finish()

    def set_status(self, status_code):
        assert status_code in httplib.responses
        self._status_code = status_code

    def get_status(self):
        return self._status_code

    def _execute(self, *args, **kwargs):
        try:
            self.on_error()
            if self.request.method not in self.SUPPORTED_METHODS:
                raise OnResponseError(405)
            self.prepare()
            if not self._finished:
                args = [self.decode_argument(arg) for arg in args]
                kwargs = dict((k, self.decode_argument(k, name=k))
                        for (k, v) in kwargs.iteritems())
                method = getattr(self, 'on_' + self.request.method.lower())
                method(*args, **kwargs)
                if self._auto_finish and not self._finished:
                    self.finish()
        except Exception, e:
            self._handle_request_exception(e)

    def _log(self):
        self.robot.log_response(self)

    def _request_summary(self):
        return 'ON_' + self.request.method + ' ' + self.request.url

    def _handle_request_exception(self, e):
        if isinstance(e, OnResponseError):
            if e.log_message:
                format = '%d %s: ' + e.log_message
                args = [e.status_code, self._request_summary()] + list(e.args)
                logging.warning(format, *args)
        else:
            logging.error('Uncaught exception %s\n%r', self._request_summary(),
                    self.response, exc_info=True)
        if not self._finished:
            self.finish()


class ErrorHandler(ResponseHandler):

    def initialize(self, status_code):
        self.set_status(status_code)

    def prepare(self):
        raise OnResponseError(self._status_code)


class Robot(object):

    def __init__(self, fetcher, handlers, **kwargs):
        self.identity = ''
        self.fetcher = fetcher
        self.handlers = []
        if handlers:
            self.add_handlers('.*$', handlers)

    def fetch(self, request):
        self.fetcher(request, self)

    def add_handlers(self, host_pattern, host_handlers):
        if not host_pattern.endswith('$'):
            host_pattern += '$'
        handlers = []

        if self.handlers and self.handlers[-1][0].pattern == '.*$':
            self.handlers.insert(-1, (re.compile(host_pattern), handlers))
        else:
            self.handlers.append((re.compile(host_pattern), handlers))

        for spec in host_handlers:
            if type(spec) is type(()):
                assert len(spec) in (2, 3)
                pattern = spec[0]
                handler = spec[1]

                if len(spec) == 3:
                    kwargs = spec[2]
                else:
                    kwargs = {}
                spec = URLSpec(pattern, handler, kwargs)
            handlers.append(spec)

    def _get_host_handlers(self, request):
        host = urlparse.urlparse(request.url).netloc
        for pattern, handlers in self.handlers:
            if pattern.match(host):
                return handlers
        return None

    def __call__(self, response):
        handler = None
        args = []
        kwargs = {}
        handlers = self._get_host_handlers(response.request)
        if not handlers:
            handler = ErrorHandler(self, response, status_code=404)
        else:
            for spec in handlers:
                path = urlparse.urlparse(response.request.url).path
                match = spec.regex.match(path)
                if match:
                    handler = spec.handler_class(self, response, **spec.kwargs)
                    if spec.regex.groups:
                        def unquote(s):
                            if s is None:
                                return s
                            return escape.url_unescape(s, encoding=None)
                        if spec.regex.groupindex:
                            kwargs = dict((str(k), unquote(v))
                                    for (k, v) in match.groupdict().iteritems())
                        else:
                            args = [unquote(s) for s in match.groups()]
                    break
            if not handler:
                handler = ErrorHandler(self, response, status_code=404)
        handler._execute(*args, **kwargs)
        return handler

    def set_worker_identity(self, identity):
        self.identity = identity
        self.fetcher.identity = identity

    def log_response(self, handler):
        if handler.get_status() < 400:
            log_method = logging.info
        elif handler.get_status() < 500:
            log_method = logging.warning
        else:
            log_method = logging.error
        request_time = handler.response.request_time * 1000.0
        log_method('[%s] %d %s %.2fms', self.identity, handler.get_status(),
                handler._request_summary(), request_time)


class OnResponseError(Exception):

    def __init__(self, status_code, log_message=None, *args):
        self.status_code = status_code
        self.log_message = log_message
        self.args = args

    def __str__(self):
        message = 'OnResponse %d: %s' (
                self.status_code, httplib.response[self.status_code])
        if self.log_message:
            return message + ' (' + (self.log_message % self.args) + ')'
        else:
            return message
