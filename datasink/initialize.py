#
# initialize.py -- initialize the data sink
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.md for details.
#
import yaml
import pika

default_topic = 'general'


def setup_queue(channel, queue_name, dct, config, bind=True):
    """Create queue if necessary and associates it with the exchange,
    so that it will receive messages sent to the exchange.
    """
    priority = dct.get('priority', config.get('default_priority', 1))

    # durable=True to make sure queue is persistent
    durable = dct.get('persist', False)
    auto_delete = dct.get('transient', True)

    args = {'x-priority': priority,
            'x-overflow': 'drop-head',
            'x-dead-letter-exchange': 'dlx',
            #'x-dead-letter-routing-key': queue_name,
            }
    if 'queue_length' in dct:
        args['x-max-length'] = dct['queue_length'],
    if 'ttl_sec' in dct:
        args['x-message-ttl'] = int(1000 * dct['ttl_sec'])

    # NOTE: if auto_delete==True, the queue is deleted when the
    #       client exits
    channel.queue_declare(queue=queue_name, durable=durable,
                          auto_delete=auto_delete, arguments=args)

    topic = dct.get('topic', default_topic)
    # NOTE: queue should be disabled before changing routing key (topic)
    # and then re-enabling
    if bind:
        channel.queue_bind(queue=queue_name,
                           exchange=config['realm'],
                           # NOTE: acts as a selector for messages to this queue
                           routing_key=topic)
    else:
        channel.queue_unbind(queue=queue_name,
                             exchange=config['realm'],
                             routing_key=topic)


def unlink_queue(channel, queue_name, dct, config):
    """Disassociates this queue from the exchange, so that it won't receive
       messages sent to the exchange.
    """
    channel.queue_unbind(queue=queue_name,
                         exchange=config['realm'],
                         routing_key=dct.get('topic', default_topic))

def purge_queue(channel, queue_name):
    """Purge all messages from the named queue.
    """
    channel.queue_purge(queue=queue_name)

def remove_queue(channel, queue_name):
    """Purge all messages from the named queue and delete it.
    """
    channel.queue_purge(queue=queue_name)
    channel.queue_delete(queue=queue_name)

def example_dlx_cb(ch, method, properties, body):
    print(" [x] %r" % (properties,))
    print(" [reason] : %s : %r" % (properties.headers['x-death'][0]['reason'], body))
    ch.basic_ack(delivery_tag=method.delivery_tag)

def handle_dlx(channel, config, callback):
    channel.basic_consume(queue=config['backlog_queue'],
                          on_message_callback=callback)
    channel.start_consuming()

def read_config(keys_file):

    if not keys_file.endswith('.yml'):
        keys_file = keys_file + '.yml'

    # read datasink config file
    with open(keys_file, 'r') as in_f:
        buf = in_f.read()
    config = yaml.safe_load(buf)

    return config

def configure_exchange(config):
    durable = config.get('persist', False)

    auth = pika.PlainCredentials(username=config['realm_username'],
                                 password=config['realm_password'])
    params = pika.ConnectionParameters(host=config['realm_host'],
                                       port=config.get('realm_port', 5672),
                                       # NOTE: necessary to keep RMQ
                                       # from disconnecting us if we
                                       # don't send anything for a while
                                       heartbeat=0,
                                       credentials=auth)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # this is our main exchange for publishing datasink requests on this realm
    channel.exchange_declare(exchange=config['realm'], exchange_type='topic',
                             durable=durable)

    # declare our "dead letter exchange" (DLX). Unacknowledged messages,
    # message timeouts, etc. end up getting routed here.
    # NOTE: exchange type should be "fanout" for the DLX
    channel.exchange_declare(exchange='dlx', exchange_type='fanout',
                             durable=durable)

    # declare the queue that will be bound to the DLX
    channel.queue_declare(queue=config['backlog_queue'], durable=durable)
    channel.queue_bind(exchange='dlx',
                       #routing_key='task_queue', # x-dead-letter-routing-key
                       queue=config['backlog_queue'])

    return connection, channel
