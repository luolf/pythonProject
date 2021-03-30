#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2021/2/22 11:00
# @Author: zhangtao
# @File  : ins_app_spider.py
from ins_spider_from_swagger import *


def main():
    store = DbToMysql(config.EHCO_DB)
    host = '10.1.1.48:9092'
    client = KafkaClient(hosts=host)
    users_topic = client.topics[b'ins-user-id']  # 指定topic,没有就新建

    send_userid_2_topic(store, users_topic.get_producer())


if __name__ == '__main__':
    main()
