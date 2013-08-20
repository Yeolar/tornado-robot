#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-24.  Yeolar <yeolar@gmail.com>
#

from collections import defaultdict

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from util import Request, Response


CTRL_MSG_MASTER = 'robot.master.'
CTRL_MSG_MASTER_START = 'start'
CTRL_MSG_WORKER = 'robot.worker.'
CTRL_MSG_WORKER_ONLINE = 'online'
CTRL_MSG_WORKER_QUIT = 'quit'
CTRL_MSG_WORKER_QUIT_ACK = 'quit.ack'


class RequestMessage(object):

    def __init__(self, identity, request):
        self.identity = identity
        self.request = request

    def serialize(self):
        return [self.identity, self.request.serialize()]

    @staticmethod
    def deserialize(message):
        return RequestMessage(message[0], Request.deserialize(message[1]))


class ResponseMessage(object):

    def __init__(self, identity, response):
        self.identity = identity
        self.response = response

    def serialize(self):
        return [self.identity, self.response.serialize()]

    @staticmethod
    def deserialize(message):
        return ResponseMessage(message[0], Response.deserialize(message[1]))


class CtrlMessage(object):

    def __init__(self, topic, identity, data):
        self.topic = topic
        self.identity = identity
        self.data = data

    def __eq__(self, other):
        return (self.topic == other.topic and
                self.identity == other.identity and
                self.data == self.data)

    def serialize(self):
        return [self.topic, self.identity, self.data]

    @staticmethod
    def deserialize(message):
        return CtrlMessage(*message)


class _Messenger(object):

    def __init__(self, in_sock, out_sock, context, io_loop=None):
        self._context = context
        self._io_loop = io_loop or IOLoop.instance()

        self._create_socket(in_sock, out_sock)
        self._in_stream = ZMQStream(self._in_socket, io_loop)
        self._out_stream = ZMQStream(self._out_socket, io_loop)

        self._callbacks = defaultdict(list)

    def _create_socket(self, in_sock, out_sock):
        raise NotImplementedError()

    def start(self):
        self._in_stream.on_recv(self._on_receive)

    def stop(self):
        self._in_stream.stop_on_recv()
#        self._publish(CTRL_MSG_WORKER, None, CTRL_MSG_WORKER_QUIT_ACK)
#
    def close(self):
        self._in_stream.close()
        self._in_socket.close()
        self._out_stream.close()
        self._out_socket.close()

    def _on_receive(self, zmq_msg):
        msg = CtrlMessage.deserialize(zmq_msg)

        if msg.topic in self._callbacks:
            for callback in self._callbacks[msg.topic]:
                callback(msg)

#        if msg.data == CTRL_MSG_WORKER_QUIT:
#            self.stop()

    def add_callback(self, topic, callback):
        self._callbacks[topic].append(callback)

    def remove_callback(self, topic, callback):
        if topic in self._callbacks and callback in self._callbacks[topic]:
            self._callbacks[topic].remove(callback)

    def publish(self, topic, identity, data):
        msg = CtrlMessage(topic, identity, data)
        self._out_stream.send_multipart(msg.serialize())


class ServerMessenger(_Messenger):

    def _create_socket(self, in_sock, out_sock):
        self._in_socket = self._context.socket(zmq.SUB)
        self._in_socket.setsockopt(zmq.SUBSCRIBE, '')
        self._in_socket.bind(in_sock)

        self._out_socket = self._context.socket(zmq.PUB)
        self._out_socket.bind(out_sock)


class ClientMessenger(_Messenger):

    def _create_socket(self, in_sock, out_sock):
        self._in_socket = self._context.socket(zmq.SUB)
        self._in_socket.setsockopt(zmq.SUBSCRIBE, '')
        self._in_socket.connect(in_sock)

        self._out_socket = self._context.socket(zmq.PUB)
        self._out_socket.connect(out_sock)

