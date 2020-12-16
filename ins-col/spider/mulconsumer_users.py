#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 15:41
# @Author: zhangtao
# @File  : consumer_users.py
import threading

from ins_spider_from_swagger import *
store = DbToMysql(config.EHCO_DB)
store1 = DbToMysql(config.EHCO_DB)
store2 = DbToMysql(config.EHCO_DB)
store3 = DbToMysql(config.EHCO_DB)
store4 = DbToMysql(config.EHCO_DB)
host = '172.26.11.167:9092'
client = KafkaClient(hosts=host)
users_topic = client.topics[b'ins-users']  # 指定topic,没有就新建
medias_topic = client.topics[b'ins-medias']  # 指定topic,没有就新建
users_1w_topic = client.topics[b'ins-users-1w-followed']  # 指定topic,没有就新建
producers = {'medias_producer': medias_topic.get_producer(), 'users_producer': users_topic.get_producer(),
             'users_1w_followed_pd': users_1w_topic.get_producer()}

following_consumer = users_topic.get_simple_consumer(consumer_group=b'ins-users', auto_commit_enable=True, auto_commit_interval_ms=5,
                                     consumer_id=b'ins-users')
userinfo_consumer = users_topic.get_simple_consumer(consumer_group=b'userinfo_consumer', auto_commit_enable=True,
                                              auto_commit_interval_ms=1, consumer_id=b'userinfo_consumer')
account_consumer = users_topic.get_simple_consumer(consumer_group=b'account_consumer', auto_commit_enable=True,
                                            auto_commit_interval_ms=1, consumer_id=b'account_consumer')
followed_consumer = users_topic.get_simple_consumer(consumer_group=b'followed_consumer', auto_commit_enable=True,
                                              auto_commit_interval_ms=5, consumer_id=b'followed_consumer')


def consumerMsg(store, producers, consumer, target):
    try:
        for message in consumer:
            if message is not None:
                user = json.loads(message.value)
                # print('准备处理：uid={},username={}'.format(user['uid'], user['username']))
                if target.__name__ == 'spide_user_by_acount':
                    target(user['username'], store, producers)
                else:
                    target(user['uid'], store, producers)
                # print('处理结束：uid={},username={}'.format(user['uid'], user['username']))
    except Exception as e:
        print(target.__name__, '发生异常退出', e)
        # producers['medias_producer'].stop()
        # producers['users_producer'].stop()
        # store.close()




def main():
    # spide_userFollowing_by_uid(user['uid'], store, producer)
    # spide_user_by_uid(user['uid'], store, producer)
    # spide_userFollowed_by_uid(user['uid'], store, producer)
    # spide_user_by_acount(user['username'], store, producer)
    try:
        minus = 60
        thread0 = threading.Thread(target=gl.monitor,  args=(minus,store))
        thread1 = threading.Thread(target=consumerMsg, args=(store1, producers, following_consumer, spide_userFollowing_by_uid))
        thread2 = threading.Thread(target=consumerMsg, args=(store2, producers, userinfo_consumer, spide_user_by_uid))
        thread3 = threading.Thread(target=consumerMsg, args=(store3, producers, followed_consumer, spide_userFollowed_by_uid))
        thread4 = threading.Thread(target=consumerMsg, args=(store4, producers, account_consumer, spide_user_by_acount))

        # 开启新线程
        thread0.start()
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        # threads = [thread0, thread1, thread2, thread3, thread4]
        # # 添加线程到线程列表
        #
        # # 等待所有线程完成
        # for t in threads:
        #     print('线程{}启动'.format(t.getName()))
        #     t.join()
    except Exception as e:
        print('发生异常退出', e)
    # finally:
        # producers['medias_producer'].stop()
        # producers['users_producer'].stop()
        # store1.close()
        # store2.close()
        # store3.close()
        # store4.close()


if __name__ == '__main__':
    main()

