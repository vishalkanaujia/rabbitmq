from time import sleep

__author__ = 'vk'
#!/usr/bin/env python
import sys
import subprocess
import json
import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='tr1',
                         type='direct')

priorities = int(sys.argv[1])

if not priorities:
    print >> sys.stderr, "Usage: %s [number of priorities]" % \
                                 (sys.argv[0],)
    sys.exit(1)

names = []

for p in xrange(priorities):
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    names.append(queue_name)
    print "Seq no. %d Queue %s created:" %(p, names[p])


assert isinstance(priorities, int)
for p in xrange(priorities):
    queue_name = names[p]

    channel.queue_bind(exchange='tr1',
                          queue=queue_name,
                          routing_key=str(p))

    print 'Queue %s is now bounded priority=%d. Waiting for logs. To exit press CTRL+C' %(queue_name, p)

def callback(ch, method, properties, body):
    print " [x] %r:%r" % (method.routing_key, body,)
    cmd = json.loads(body)
    print cmd
    #print subprocess.check_output(["/bin/ls", cmd[0]['name']])
    #proc = subprocess.Popen(["/bin/ls", cmd[0]['name']], stdout=subprocess.PIPE)
    proc = subprocess.Popen(["/usr/bin/find", "/home"], stdout=subprocess.PIPE)
    print "proc poll= ", proc.poll()

    while proc.poll() is None:
        for line in proc.stdout:
            print line
        #print "hello"
        sleep(1)

    print "proc poll= ", proc.poll()


def callback2(ch, method, properties, body):
    callback(ch,method,properties, body)

def callback3(ch, method, properties, body):
    callback(ch,method,properties, body)

cb = [callback, callback2, callback3]

for p in xrange(priorities):
    queue_name = names[p]
    print "Queue %s: Started consuming" %(queue_name)

    channel.basic_consume(cb[p],
                          queue=queue_name,
                          no_ack=True)

channel.start_consuming()
