#!/usr/bin/env python
#
# client.py -- job source
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#

import sys
import time
import json

import pika

from datasink.initialize import read_config, default_topic


class JobSource:

    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def read_config(self, configfile):
        self.config = read_config(configfile)

        self.realm = self.config['realm']
        self.realm_host = self.config['realm_host']

    def connect(self):
        auth = pika.PlainCredentials(username=self.config['realm_username'],
                                     password=self.config['realm_password'])
        params = pika.ConnectionParameters(host=self.realm_host,
                                           #port=config['realm_port'],
                                           # NOTE: necessary to keep RMQ
                                           # from disconnecting us if we
                                           # don't send anything for a while
                                           heartbeat=0,
                                           credentials=auth)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

    def shutdown(self):
        self.connection.close()

    def submit(self, job):

        try:
            pkt = dict()
            pkt.update(job)
            pkt.update(time_origin=time.time(),
                       source_origin=self.name)

            message = json.dumps(pkt)
            topic = job.get('topic', default_topic)

            # set up message properties
            kwargs = {}

            persist = self.config.get('message_persist', False)
            if persist:
                kwargs['delivery_mode'] = 2

            msg_ttl_sec = self.config.get('ttl_sec', None)
            if msg_ttl_sec is not None:
                # message TTL is in msec
                message_ttl = int(self.message_ttl_sec * 1000)
                kwargs['expiration'] = str(message_ttl)

            props = pika.BasicProperties(**kwargs)

            self.channel.basic_publish(exchange=self.realm,
                                       routing_key=topic,
                                       body=message,
                                       properties=props)

            self.logger.info("sent job: %r" % pkt)

        except Exception as e:
            self.logger.error("Error submitting job to '{}': {}".format(self.realm, e),
                              exc_info=True)
            raise e
