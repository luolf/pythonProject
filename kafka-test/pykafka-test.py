#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 14:43
# @Author: zhangtao
# @File  : pykafka-test.py
# -* coding:utf8 *-
from pykafka import KafkaClient

host = '172.26.11.167:9092'
client = KafkaClient(hosts=host)
print(client.topics)
print(client.brokers)

topic = client.topics[b'test']  #指定topic,没有就新建
producer = topic.get_producer()
for i in range(11):
    producer.produce(('test message ' + str(i ** 2)).encode())

producer.stop()