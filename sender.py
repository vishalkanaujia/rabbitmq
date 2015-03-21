__author__ = 'vk'

#!/usr/bin/env python
import pika
import sys
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='tr1',
                         type='direct')

#p = sys.argv[1] if len(sys.argv) > 1 else 0
p = sys.argv[1]

#message = not (not ' '.join(sys.argv[2:]) and not '/tmp')
data = [{'name': '/tmp'}]
message = json.dumps(data)

channel.basic_publish(exchange='tr1',
                      routing_key=str(p),
                      body=message)

print " [x] Sent %r:%r" % (p, json.dumps(message))
connection.close()
