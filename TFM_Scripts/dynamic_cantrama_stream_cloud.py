#!/usr/bin/python3

import pika, sys, os, time
from datetime import datetime

filename = '/home/sebastian/Desktop/TestingCSVs/cantrama_df'+str(sys.argv[1])+'.csv'
rabbit_ip = str(sys.argv[2])
counter = 0

credentials_rabbit = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters(host = rabbit_ip, port = 5672, virtual_host = '/', credentials = credentials_rabbit))
channel = connection.channel()

channel.queue_declare(queue='cantrama', durable=True)

with open(filename, 'r') as file:
    next(file)

    for line in file:
        row_content = line.strip()
        time.sleep(1.73)
        channel.basic_publish(exchange='', routing_key='cantrama', body=row_content)
        counter+=1

now = datetime.now()
current_time = now.strftime("%H:%M:%S")

print(counter)
print("[x] Sent all rows in CSV")
print("Current Time =", current_time)
connection.close()
