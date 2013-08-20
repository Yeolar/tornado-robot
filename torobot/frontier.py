#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 2013-04-25.  Yeolar <yeolar@gmail.com>
#

import datetime
import hashlib
import heapq
import time

from util import Request


class _Task(object):

    __slots__ = ['deadline', 'period', 'request']

    def __init__(self, deadline, period, request):
        if isinstance(deadline, (int, long, float)):
            self.deadline = deadline
        elif isinstance(deadline, datetime.timedelta):
            self.deadline = time.time() + deadline.total_seconds()
        else:
            raise TypeError('Unsupported deadline %r' % deadline)

        if isinstance(period, (type(None), int, long, float)):
            self.period = period
        elif isinstance(period, datetime.timedelta):
            self.period = period.total_seconds()
        else:
            raise TypeError('Unsupported period %r' % period)

        self.request = request

    def __lt__(self, other):
        return (self.deadline, id(self)) < (other.deadline, id(other))


class UniqueTaskFilter(object):

    def __init__(self, hash_method, depth=3):
        self._hash = hash_method
        self._depth = depth
        self._tasks = {}

    def _get_task_group_and_key(self, request):
        hash_method = hashlib.new(self._hash)
        hash_method.update(request.url)
        hash_value = hash_method.hexdigest()

        tasks = self._tasks
        for i in range(self._depth):
            tasks = tasks.setdefault(hash_value[i], {})
        return tasks, hash_value[self._depth:]

    def has(self, request, period=None, add=False):
        if isinstance(period, datetime.timedelta):
            period = period.total_seconds()
        elif not isinstance(period, (type(None), int, long, float)):
            raise TypeError('Unsupported period %r' % period)

        tasks, key = self._get_task_group_and_key(request)

        if key in tasks:
            return True
        else:
            if add:
                tasks[key] = period
            return False

    def update(self, request, period):
        if isinstance(period, datetime.timedelta):
            period = period.total_seconds()
        elif not isinstance(period, (type(None), int, long, float)):
            raise TypeError('Unsupported period %r' % period)

        tasks, key = self._get_task_group_and_key(request)
        tasks[key] = period

    def get_period(self, request):
        tasks, key = self._get_task_group_and_key(request)
        return tasks.get(key)


class Frontier(object):

    def __init__(self, requests=None):
        self._tasks = []
        self._unique_tasks = UniqueTaskFilter('sha1')
        if requests:
            self.add_requests(requests)

    def add_requests(self, requests):
        for url, period in requests:
            self.add_request(Request(url), period=period)

    def add_request(self, request, deadline=time.time(), period=None):
        assert not self._unique_tasks.has(request, period, add=True)
        self._add_task(_Task(deadline, period, request))

    def reload_request(self, request):
        period = self._unique_tasks.get_period(request)
        if period:
            deadline = time.time() + period
            self._add_task(_Task(deadline, period, request))

    def _add_task(self, task):
        heapq.heappush(self._tasks, task)

    def remove_task(self, task):
        task.request = None

    def get_next_request(self):
        now = time.time()
        while self._tasks:
            if self._tasks[0].request is None:
                heapq.heappop(self._tasks)
            elif self._tasks[0].deadline <= now:
                return heapq.heappop(self._tasks).request
            else:
                break
        return None

