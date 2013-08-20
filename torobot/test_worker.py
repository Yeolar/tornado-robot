#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-26.  Yeolar <yeolar@gmail.com>
#

from zmq.eventloop.ioloop import IOLoop

import fetcher
import robot
import worker
from options import parse_command_line


parse_command_line()


class AHandler(robot.ResponseHandler):

    def on_error(self):
        if self.response.error:
            raise robot.OnResponseError(self.response.code, self.response.error)

    def on_get(self):
        print self.response


def main():
    http_fetcher = fetcher.HTTPFetcher()
    rb = robot.Robot(http_fetcher, [
        ('/.*', AHandler),
    ])
    worker.Worker(rb).start()
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
