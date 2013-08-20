#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-27.  Copyright (C) Yeolar <yeolar@gmail.com>
#

from zmq.eventloop.ioloop import IOLoop
from tornado.options import define, parse_command_line

import fetcher
import frontier
import robot
import master
import worker
from process import fork_processes, master_id


parse_command_line()


class AHandler(robot.ResponseHandler):

    def on_error(self):
        if self.response.error:
            raise robot.OnResponseError(self.response.code, self.response.error)

    def on_get(self):
        print self.response


def main():
    ft = frontier.Frontier([
        ('http://m.sohu.com/', 1),
    ])
    http_fetcher = fetcher.HTTPFetcher()
    rb = robot.Robot(http_fetcher, [
        ('/.*', AHandler),
    ])
    id = fork_processes(0)
    if id == master_id():
        master.Master(ft).start()
    else:
        worker.Worker(rb).start()
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
