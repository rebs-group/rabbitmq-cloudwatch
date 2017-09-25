#!/usr/bin/env python
from __future__ import with_statement, print_function

from pyrabbit.api import Client
from boto.ec2.cloudwatch import connect_to_region
import os
from time import sleep

def get_queue_depths(host, username, password, vhost):
    #cl = Client('http://%s:15672/api' % host, username, password)
    cl = Client('%s:15672' % host, username, password)
    if not cl.is_alive(vhost):
        raise Exception("Failed to connect to rabbitmq")
    depths = {}
    queues = [q['name'] for q in cl.get_queues(vhost=vhost)]
    for queue in queues:
        if queue == "aliveness-test": #pyrabbit
            continue
        elif queue.endswith('.pidbox') or queue.startswith('celeryev.'): #celery
            continue

        depths[queue] = cl.get_queue_depth(vhost, queue)
    return depths


def publish_queue_depth_to_cloudwatch(cwc, queue_name, depth, namespace):
    print("Putting metric namespace=%s name=%s unit=Count value=%i" %
        (namespace, queue_name, depth))
    print(cwc.put_metric_data(namespace=namespace,
        name=queue_name,
        unit="Count",
        value=depth))


def publish_depths_to_cloudwatch(depths, namespace):
    cwc = connect_to_region('eu-west-1')
    for queue in depths:
        publish_queue_depth_to_cloudwatch(cwc, queue, depths[queue], namespace)


def get_queue_depths_and_publish_to_cloudwatch(host, username, password, vhost, namespace):
    print('Connecting to %s:%s@%s/%s' % (username, password, host, vhost))
    depths = get_queue_depths(host, username, password, vhost)
    import pprint
    pprint.pprint(depths)
    publish_depths_to_cloudwatch(depths, namespace)

if __name__ == "__main__":
    get_queue_depths_and_publish_to_cloudwatch(
        os.environ.get("RABBITMQ_HOST"),
        os.environ.get("RABBITMQ_USER"),
        os.environ.get("RABBITMQ_PASSWORD"),
        "crmrebs",
        "EC2/RabbitMQ")

