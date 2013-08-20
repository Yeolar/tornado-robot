#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-24.  Yeolar <yeolar@gmail.com>
#

import logging
import os
import socket

import zmq
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

from message import *


class Master(object):

    def __init__(self, frontier,
            data_in_sock='ipc:///tmp/robot-data-w2m.sock',
            data_out_sock='ipc:///tmp/robot-data-m2w.sock',
            msg_in_sock='ipc:///tmp/robot-msg-w2m.sock',
            msg_out_sock='ipc:///tmp/robot-msg-m2w.sock',
            io_loop=None):
        self.identity = 'master:%s:%s' % (socket.gethostname(), os.getpid())

        context = zmq.Context()

        self._io_loop = io_loop or IOLoop.instance()

        self._in_socket = context.socket(zmq.SUB)
        self._in_socket.setsockopt(zmq.SUBSCRIBE, '')
        self._in_socket.bind(data_in_sock)
        self._in_stream = ZMQStream(self._in_socket, io_loop)

        self._out_socket = context.socket(zmq.PUSH)
        self._out_socket.bind(data_out_sock)
        self._out_stream = ZMQStream(self._out_socket, io_loop)

        self._online_workers = set()
        self._running = False

        self._updater = PeriodicCallback(self._send_next, 100, io_loop=io_loop)
        self._reloader = PeriodicCallback(self.reload, 1000, io_loop=io_loop)

        self.frontier = frontier
        self.messenger = ServerMessenger(msg_in_sock, msg_out_sock,
                context, io_loop)

    def start(self):
        logging.info('[%s] starting', self.identity)
        self.messenger.add_callback(CTRL_MSG_WORKER, self._on_worker_msg)
        self.messenger.start()

        self._in_stream.on_recv(self._on_receive_processed)
        self._updater.start()
        self._reloader.start()
        self._running = True

    def stop(self):
        self._running = False
        self._reloader.stop()
        self._updater.stop()
        self.messenger.stop()
#        self.messenger.publish(CTRL_MSG_WORKER, self.identity,
#                CTRL_MSG_WORKER_QUIT)

    def close(self):
        self._in_stream.close()
        self._in_socket.close()
        self._out_stream.close()
        self._out_socket.close()
        self.messenger.close()

    def reload(self):
        pass

    def _on_worker_msg(self, msg):
        if msg.data == CTRL_MSG_WORKER_ONLINE:
            self._online_workers.add(msg.identity)
            logging.info('[%s] append [%s]', self.identity, msg.identity)
            self._send_next()

#        if msg.data == CTRL_MSG_WORKER_QUIT_ACK:
#            if msg.identity in self._online_workers:
#                self._online_workers.remove(msg.identity)

    def _send_next(self):
        if not self._running:
            return

        worker_num = len(self._online_workers)

        if self._running and worker_num > 0:
            while self._out_stream._send_queue.qsize() < worker_num * 4:
                request = self.frontier.get_next_request()
                if not request:
                    break

                msg = RequestMessage(self.identity, request)
                self._out_stream.send_multipart(msg.serialize())
                logging.debug('[%s] send request(%s)',
                        self.identity, request.url)

                self.frontier.reload_request(request)

    def _on_receive_processed(self, zmq_msg):
        msg = ResponseMessage.deserialize(zmq_msg)
        request = msg.response.request
        logging.debug('[%s] receive response(%s)', self.identity, request.url)
        self._send_next()


def main():
    from frontier import Frontier
    from options import parse_command_line

    parse_command_line()

    ft = Frontier([
        ('http://localhost/', 1),
    ])
    Master(ft).start()
    IOLoop.instance().start()


if __name__ == '__main__':
    main()
