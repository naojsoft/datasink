#
# worker.py -- data sink worker
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#

import sys
import time
import json
import functools
import queue
import threading

from g2base import ssdlog

import pika

from datasink.initialize import read_config


class JobSink:

    def __init__(self, logger, name):
        self.logger = logger
        self.name = name
        self.work_queue = queue.Queue()
        self.config = dict()

        self.action_tbl = {'ping': self.ping,
                           'window': self.window,
                           'sleep': self.sleep,
                           }

    def add_action(self, aname, method):
        self.action_tbl[aname] = method

    def _ack_message(self, ack_flag, channel, delivery_tag, requeue=False):
        # Note that `channel` must be the same pika channel instance via which
        # the message being ACKed was retrieved (AMQP protocol constraint).
        if channel.is_open:
            if ack_flag:
                channel.basic_ack(delivery_tag)
            else:
                channel.basic_nack(delivery_tag, requeue=requeue)
        else:
            # Channel is already closed, so we can't ACK this message
            self.logger.error("Whups! channel is closed--can't ACK")

    def handle_message(self, ch, method, properties, body, args):
        self.logger.debug("received %r" % body)
        channel, connection, queue_name = args
        try:
            job = json.loads(body)

        except Exception as e:
            msg = "JSON loading error for job: %r:\n%r" % (body, e)
            self.logger.error(msg)
            self._ack_message(False, channel, method.delivery_tag)
            return

        work_unit = dict(channel=channel, connection=connection,
                         delivery_tag=method.delivery_tag, job=job)
        self.work_queue.put(work_unit)

    def do_work(self, i, work_unit):
        job = work_unit['job']
        self.logger.info('worker {} handling job {}'.format(i, str(job)))

        action = job['action']
        method = self.action_tbl.get(action, self.no_such_action)

        # define acknowledgement function
        def ack(ack_flag, msg_txt, info):
            cb = functools.partial(self._ack_message, ack_flag,
                                   work_unit['channel'],
                                   work_unit['delivery_tag'])
            work_unit['connection'].add_callback_threadsafe(cb)

        try:
            method(work_unit, ack)

        except Exception as e:
            msg = "Error doing job: {}".format(e)
            self.logger.error(msg, exc_info=True)
            # TODO: include traceback
            ack(False, msg, {})

    def worker_loop(self, i, ev_quit):
        """N workers will run this method.  They pull jobs off of the work
        queue and perform those tasks until ev_quit event is set.
        """
        self.logger.info("starting worker {}...".format(i))
        while not ev_quit.is_set():
            try:
                work_unit = self.work_queue.get(block=True, timeout=1.0)
            except queue.Empty:
                continue

            self.do_work(i, work_unit)

        self.logger.info("ending worker loop...")

    def ping(self, work_unit, fn_ack):
        """ping job."""
        fn_ack(True, 'pong', {})

    def window(self, work_unit, fn_ack):
        """Outstanding work request size job."""
        # adjust the window of outstanding requests
        job = work_unit['job']
        num = job['size']
        work_unit['channel'].basic_qos(prefetch_count=num)
        fn_ack(True, '', {})

    def sleep(self, work_unit, fn_ack):
        """Test job (just sleeps for `duration` seconds)."""
        job = work_unit['job']
        secs = job['duration']
        time.sleep(secs)
        fn_ack(True, '', {})

    def no_such_action(self, work_unit, fn_ack):
        """This job runs if the job type is not recognized."""
        job = work_unit['job']
        msg = "No such job '{}' at worker '{}'".format(
            job['action'], self.name)
        self.logger.error(msg)
        fn_ack(False, msg, {})

    def read_config(self, configfile):
        self.config = read_config(configfile)

    def serve(self, config):
        # connect to queues
        auth = pika.PlainCredentials(username=config['username'],
                                     password=config['password'])
        connection = pika.BlockingConnection(
            #pika.ConnectionParameters(host=realm_host, port=port, credentials=auth))
            pika.ConnectionParameters(host=config['host']))
        channel = connection.channel()

        # start up consumer workers
        ev_quit = threading.Event()
        threads = []

        numworkers = config['numworkers']
        for i in range(numworkers):
            t = threading.Thread(target=self.worker_loop, args=[i, ev_quit])
            threads.append(t)
            t.start()

        self.logger.info("Waiting for messages. To exit press CTRL+C")
        channel.basic_qos(prefetch_count=numworkers)

        queue_names = config['queue_names']
        for queue_name in queue_names:
            callback_fn = functools.partial(self.handle_message,
                                            args=[channel, connection,
                                                  queue_name])
            channel.basic_consume(queue=queue_name,
                                  on_message_callback=callback_fn)

        self.logger.info("consuming on queues %s" % (', '.join(queue_names)))
        try:
            channel.start_consuming()

        except KeyboardInterrupt:
            self.logger.info("detected keyboard interrupt!")

        self.logger.info("Shutting down...")
        channel.stop_consuming()

        ev_quit.set()
        for t in threads:
            t.join()

        connection.close()
