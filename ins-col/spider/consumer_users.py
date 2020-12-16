#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 15:41
# @Author: zhangtao
# @File  : consumer_users.py
import threading

from ins_spider_from_swagger import *

store1 = DbToMysql(config.EHCO_DB)
store2 = DbToMysql(config.EHCO_DB)
store3 = DbToMysql(config.EHCO_DB)
store4 = DbToMysql(config.EHCO_DB)
host = '172.26.11.167:9092'
client = KafkaClient(hosts=host)
topic = client.topics[b'ins-users']  # 指定topic,没有就新建
producer = topic.get_producer()
print(client.topics)
print(client.brokers)
topic_name = b'ins-users'
topic = client.topics[topic_name]  # 指定topic,没有就新建
consumer = topic.get_simple_consumer(consumer_group=topic_name, auto_commit_enable=True, auto_commit_interval_ms=1,
                                     consumer_id=topic_name)
try:
    for message in consumer:
        if message is not None:
            user = json.loads(message.value)
            sql = "select 1 from user_by_userid where uid ={}".format(user['uid'])
            rst = store1.query(sql)
            if len(rst) == 0:
                # print(message.offset, message.value)
                print('准备处理：uid={},username={}'.format(user['uid'], user['username']))
                thread1 = threading.Thread(target=spide_userFollowing_by_uid, args=(user['uid'], store1, producer))
                thread2 = threading.Thread(target=spide_user_by_uid, args=(user['uid'], store2, producer))
                thread3 = threading.Thread(target=spide_userFollowed_by_uid, args=(user['uid'], store3, producer))
                thread4 = threading.Thread(target=spide_user_by_acount, args=(user['username'], store4, producer))

                # 开启新线程
                thread1.start()
                thread2.start()
                thread3.start()
                thread4.start()
                threads = [thread1, thread2, thread3, thread4]
                # 添加线程到线程列表

                # 等待所有线程完成
                for t in threads:
                    t.join()
                print('处理结束：uid={},username={}'.format(user['uid'], user['username']))

                # spide_userFollowing_by_uid(user['uid'], store, producer)
                # spide_user_by_uid(user['uid'], store, producer)
                # spide_userFollowed_by_uid(user['uid'], store, producer)
                # spide_user_by_acount(user['username'], store, producer)
            else:
                print('接受消息user_by_userid 已存在：uid={},username={}'.format(user['uid'], user['username']))
except Exception as e:
    print('发生异常退出', e)
finally:
    producer.stop()
    store1.close()
    store2.close()
    store3.close()
    store4.close()

