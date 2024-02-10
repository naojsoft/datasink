#
# worker.py -- data sink worker
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.md for details.
#

import sys
import time
import json
import functools
import queue
import pprint
import threading

from g2base import ssdlog

import pika

from datasink.initialize import read_config, default_topic


class JobSink:

    def __init__(self, logger, name):
        self.logger = logger
        self.name = name
        self.work_queue = queue.Queue()
        self.config = dict()
        self.threads = []

        self.action_tbl = {'ping': self.ping,
                           'window': self.window,
                           'sleep': self.sleep,
                           'debug': self.debug,
                           }
        self.retry_interval = 60.0

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

        action = job.get('action', None)
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

    def debug(self, work_unit, fn_ack):
        """debug job."""
        job = work_unit['job']
        print("Received job:")
        pprint.pprint(job)
        fn_ack(True, '', {})

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
        action = job.get('action', None)
        msg = "No such job '{}' at worker '{}'".format(action, self.name)
        self.logger.error(msg)
        fn_ack(False, msg, {})

    def read_config(self, configfile):
        self.config = read_config(configfile)

        self.retry_interval = self.config.get('retry_interval', 60.0)

    def start_workers(self, ev_quit=None):
        numworkers = self.config['num_workers']
        for i in range(numworkers):
            t = threading.Thread(target=self.worker_loop, args=[i, ev_quit])
            self.threads.append(t)
            t.start()

    def serve(self, ev_quit=None, topic=None):
        config = self.config
        # connect to queues
        auth = pika.PlainCredentials(username=config['realm_username'],
                                     password=config['realm_password'])
        params = pika.ConnectionParameters(host=config['realm_host'],
                                           port=config.get('realm_port', 5672),
                                           credentials=auth)

        # start up consumer workers
        if ev_quit is None:
            ev_quit = threading.Event()

        while not ev_quit.is_set():
            try:
                connection = pika.BlockingConnection(params)
                channel = connection.channel()

                channel.basic_qos(prefetch_count=config['num_workers'])

                if topic is None:
                    topic = config.get('topic', default_topic)

                queue_names = config.get('queue_names', [self.name])
                for queue_name in queue_names:
                    channel.queue_bind(queue=queue_name,
                                       exchange=config['realm'],
                                       routing_key=topic)

                callback_fn = functools.partial(self.handle_message,
                                                args=[channel, connection,
                                                      queue_name])
                channel.basic_consume(queue=queue_name,
                                      on_message_callback=callback_fn)

                self.logger.info("consuming on queues: %s" % (', '.join(queue_names)))
                self.logger.info("Waiting for messages. To exit press CTRL+C")
                try:
                    channel.start_consuming()

                except KeyboardInterrupt:
                    self.logger.info("detected keyboard interrupt!")
                    channel.stop_consuming()
                    break

            except pika.exceptions.ConnectionClosedByBroker as e:
                self.logger.error("broker closed connection: {}".format(e))
                self.logger.info("retrying after {} sec interval".format(self.retry_interval))
                ev_quit.wait(self.retry_interval)
                continue

            except pika.exceptions.AMQPChannelError as e:
                self.logger.error("channel error: {}".format(e), exc_info=True)
                self.logger.info("retrying after {} sec interval".format(self.retry_interval))
                ev_quit.wait(self.retry_interval)
                continue

            except (pika.exceptions.AMQPConnectionError,
                    pika.exceptions.AMQPHeartbeatTimeout) as e:
                self.logger.error("connection error: {}".format(e), exc_info=True)
                self.logger.info("retrying after {} sec interval".format(self.retry_interval))
                ev_quit.wait(self.retry_interval)
                continue

        self.logger.info("Shutting down...")
        ev_quit.set()
        for t in self.threads:
            t.join()

        connection.close()
