import argparse
import logging
import os
import multiprocessing
import Queue
import time
from pprint import pformat


logging.basicConfig(level=logging.DEBUG)
main_logger = logging.getLogger(__name__)

VALID_ORDERS = ['ping', 'show_results']

class Executive(object):
    def __init__(self, queue=None):
        self.queue = queue
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info("Set up Executive instance with pid %s" % os.getpid())

    def send_order(self, order=None):
        if order:
            self.queue.put(order)
            self.log.info("Sent order: %s" % order)
            return True
        else:
            self.log.warning("No order to send!")
            return False

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
            self.log.debug("Got order: %s" % order)
            return order
        except Queue.Empty:
            self.log.warning("No task received after %i seconds" % self.timeout)
            return None

    def carry_out(self, order=None):
        result = None
        if order:
            if order in self.orders:
                self.log.debug("Found valid order: %s" % order)
                result = self.orders[order]()
            else:
                self.log.error("Invalid order received: %s" % order)
                result = 'invalid_order'
        else:
            self.log.warning("No order received!")
        return {'order': order, 'time_done': time.time(), 'result': result}

    def ping(self):
        return True

    def show_results(self):
        self.log.info("Results:\n{0}".format(pformat(self.results)))

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

def handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--orders', nargs='+', required=True,
                        help='orders to send the grunt')
    return parser.parse_args()

if __name__ == '__main__':
    args = handle_args()

    q = multiprocessing.JoinableQueue()
    grunt_worker = Grunt(queue=q)
    executive = Executive(queue=q)

    grunt_worker.daemon = True
    grunt_worker.start()
    for order in args.orders:
        executive.send_order(order)
    main_logger.info("Sent %i orders!" % len(args.orders))

    q.join()
    executive.send_order('show_results')
    q.join()
