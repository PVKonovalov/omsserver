"""
    pathfinder helper_rabbit.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-02-15
"""

import json

import pika
from flask import current_app as app


def send_via_rabbit(user_from_id, user_from, user_to_id, message_guid, message_str, message_type):
    """
    Send message via RabbitMQ server
    :param user_from_id:
    :param user_from:
    :param user_to_id:
    :param message_guid:
    :param message_str:
    """
    if 'RABBITMQ_HOST' in app.config and 'RABBITMQ_USER' in app.config and 'RABBITMQ_PASSWORD' in app.config:
        rabbit_connection = app.config['RABBITMQ_HOST']
        credentials = pika.PlainCredentials(app.config['RABBITMQ_USER'], app.config['RABBITMQ_PASSWORD'])

        body = {
            "user_from_id": user_from_id,
            "user_from": user_from,
            "user_to_id": user_to_id,
            "message_guid": message_guid,
            "message": message_str,
            "message_type": message_type
        }

        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_connection, credentials=credentials))
        channel = connection.channel()

        channel.queue_declare(queue='', exclusive=True)

        channel.basic_publish(exchange='rsdu.message',
                              routing_key='user_{}'.format(user_to_id),
                              body=json.dumps(body)
                              )

        connection.close()
