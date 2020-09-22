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

from datasink.initialize import read_config


class JobSource:

    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def read_config(self, configfile):
        self.config = read_config(configfile)

        self.realm = self.config['realm']
        self.realm_host = self.config['host']

    def connect(self):
        params = pika.ConnectionParameters(self.realm_host)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        durable = self.config.get('persist', False)
        self.channel.exchange_declare(exchange=self.realm,
                                      exchange_type='direct',
                                      durable=durable)

    def shutdown(self):
        self.connection.close()

    def submit_job(self, name, job):

        dct = self.config['keys'][name]
        key = dct['key']
        queue_name = key.split('-')[0]

        job.update(queue=queue_name, time_origin=time.time(),
                   source_origin=self.name)

        message = json.dumps(job)

        # set up message properties
        kwargs = {}

        persist = dct.get('persist', False)
        if persist:
            kwargs['delivery_mode'] = 2

        msg_ttl_sec = dct.get('msg_ttl_sec', None)
        if msg_ttl_sec is not None:
            # message TTL is in msec
            message_ttl = int(self.message_ttl_sec * 1000)
            kwargs['expiration'] = str(message_ttl)

        props = pika.BasicProperties(**kwargs)

        self.channel.basic_publish(exchange=self.realm,
                                   routing_key=key,
                                   body=message,
                                   properties=props)

        self.logger.info("sent message %r" % message)
