#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 15:15
# @Author: zhangtao
# @File  : consumer.py
# -* coding:utf8 *-
from pykafka import KafkaClient

host = '172.26.11.167:9092'
client = KafkaClient(hosts=host)

print(client.topics)
print(client.brokers)

# 消费者
topic = client.topics[b'test']
consumer = topic.get_simple_consumer(consumer_group=b'test', auto_commit_enable=True, auto_commit_interval_ms=1,
                                     consumer_id=b'test')
for message in consumer:
    if message is not None:
        print(message.offset, message.value)
