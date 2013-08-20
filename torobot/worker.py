#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-25.  Yeolar <yeolar@gmail.com>
#

import logging
import os
import socket

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from message import *


class Worker(object):

    def __init__(self, robot,
            data_in_sock='ipc:///tmp/robot-data-m2w.sock',
            data_out_sock='ipc:///tmp/robot-data-w2m.sock',
            msg_in_sock='ipc:///tmp/robot-msg-m2w.sock',
            msg_out_sock='ipc:///tmp/robot-msg-w2m.sock',
            io_loop=None):
        self.identity = 'worker:%s:%s' % (socket.gethostname(), os.getpid())

        context = zmq.Context()

        self._io_loop = io_loop or IOLoop.instance()

        self._in_socket = context.socket(zmq.PULL)
        self._in_socket.connect(data_in_sock)
        self._in_stream = ZMQStream(self._in_socket, io_loop)

        self._out_socket = context.socket(zmq.PUB)
        self._out_socket.connect(data_out_sock)
        self._out_stream = ZMQStream(self._out_socket, io_loop)

        self._running = False

        self.robot = robot
        self.robot.set_worker_identity(self.identity)
        self.messenger = ClientMessenger(msg_in_sock, msg_out_sock,
                context, io_loop)

    def start(self):
        logging.info('[%s] starting', self.identity)
        self.messenger.start()
        self.messenger.publish(CTRL_MSG_WORKER, self.identity,
                CTRL_MSG_WORKER_ONLINE)

        self._in_stream.on_recv(self._on_receive_request)
        self._running = True

    def stop(self):
        self._running = False
        self.messenger.stop()

    def close(self):
        self._in_stream.close()
        self._in_socket.close()
        self._out_stream.close()
        self._out_socket.close()
        self.messenger.close()

    def _on_receive_request(self, zmq_msg):
        msg = RequestMessage.deserialize(zmq_msg)
        request = msg.request
        logging.debug('[%s] receive request(%s)', self.identity, request.url)
        self.robot.fetch(request)


def main():
    from fetcher import HTTPFetcher
    from robot import Robot, ResponseHandler
    from options import parse_command_line

    parse_command_line()

    class TestHandler(ResponseHandler):
        def on_get(self):
            print self.response

    rb = Robot(HTTPFetcher(), [
        ('/.*', TestHandler),
    ])
    Worker(rb).start()
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
