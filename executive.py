#!/usr/bin/env python

import argparse
import logging
import os
import multiprocessing
import Queue
import time
from pprint import pformat


logging.basicConfig(level=logging.DEBUG)
main_logger = logging.getLogger(__name__)

VALID_ORDERS = ['ping', 'reverse', 'show_results']

class Executive(object):
    def __init__(self, queue=None):
        self.queue = queue
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info("Set up Executive instance with pid %s" % os.getpid())

    def send_order(self, order, params=None):
        msg = {'order': order, 'params': params}
        self.queue.put(msg)
        self.log.info("Sent order {0} with params {1}".format(order, params))

class Grunt(multiprocessing.Process):
    def __init__(self, queue=None, opts={}):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.timeout = opts.get('timeout', 5)
        self.wait_intv = opts.get('wait_intv', 5)
        self.results = []
        self.log = logging.getLogger(self.__class__.__name__)

        self.orders = self._setup_valid_orders()
        self.log.info("Set up Grunt instance")
        self.log.debug("Timeout: %i, Wait Interval: %i" % (self.timeout, self.wait_intv))

    def _setup_valid_orders(self):
        valid = {}
        for o in VALID_ORDERS:
            try:
                my_order = getattr(self, o)
            except AttributeError:
                continue
            else:
                if callable(my_order):
                    valid[o] = my_order
                    self.log.debug("Loaded valid order: %s" % o)
        return valid

    def recv_order(self):
        try:
            order = self.queue.get(True, self.timeout)
            self.log.debug("Got order: {0}".format(order))
            return order
        except Queue.Empty:
            self.log.warning("No task received after %i seconds" % self.timeout)
            return None

    def carry_out(self, msg):
        result = None
        order = msg['order']
        params = msg['params']
        if params and len(params) == 1:
            params = params[0]

        if order in self.orders:
            self.log.debug("Found valid order: %s" % order)
            if params:
                result = self.orders[order](params)
            else:
                result = self.orders[order]()
        else:
            self.log.error("Invalid order received: %s" % order)
            result = 'invalid_order'
        return {'order': order, 'time_done': time.time(), 'result': result}

    def run(self):
        self.log.info("Starting with pid %s" % self.pid)
        while True:
            order = self.recv_order()
            if order:
                self.log.debug("Carrying out order: %s" % order)
                result = self.carry_out(order)
                self.log.debug("Got result: {0}".format(result))
                self.results.append(result)
                self.queue.task_done()
            else:
                self.log.info("Waiting %i seconds for more orders" % self.wait_intv)
                time.sleep(self.wait_intv)

    # Orders
    def ping(self):
        return True

    def reverse(self, msg):
        return msg[::-1]

    def show_results(self):
        self.log.info("Results:\n{0}".format(pformat(self.results)))


def handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--orders', nargs='+', required=True,
                        help='Valid orders are: {0}'.format(VALID_ORDERS) +
                        '\nAdd parameters like: order:param1,param2,...')
    return parser.parse_args()

if __name__ == '__main__':
    args = handle_args()

    q = multiprocessing.JoinableQueue()
    grunt_worker = Grunt(queue=q)
    executive = Executive(queue=q)

    grunt_worker.daemon = True
    grunt_worker.start()
    for order in args.orders:
        params = order.split(':')
        if params and len(params) > 1:
            executive.send_order(params[0], params[1:])
        else: 
            executive.send_order(order)
    main_logger.info("Sent %i orders!" % len(args.orders))

    q.join()
    executive.send_order('show_results')
    q.join()
