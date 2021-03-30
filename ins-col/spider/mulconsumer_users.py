#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 15:41
# @Author: zhangtao
# @File  : consumer_users.py
import threading

from ins_spider_from_swagger import *
store = DbToMysql(config.EHCO_DB)
# store1 = DbToMysql(config.EHCO_DB)
# store2 = DbToMysql(config.EHCO_DB)
# store3 = DbToMysql(config.EHCO_DB)
# store4 = DbToMysql(config.EHCO_DB)
# store5_0 = DbToMysql(config.EHCO_DB)
# store5_1 = DbToMysql(config.EHCO_DB)
# store5_2 = DbToMysql(config.EHCO_DB)
# store5_3 = DbToMysql(config.EHCO_DB)
# store5_4 = DbToMysql(config.EHCO_DB)
# store5_5 = DbToMysql(config.EHCO_DB)
# store5_6 = DbToMysql(config.EHCO_DB)
# store5_7 = DbToMysql(config.EHCO_DB)
# store5_8 = DbToMysql(config.EHCO_DB)
# store5_9 = DbToMysql(config.EHCO_DB)
# store5_10 = DbToMysql(config.EHCO_DB)
# store5_11 = DbToMysql(config.EHCO_DB)
# store5_12 = DbToMysql(config.EHCO_DB)
# store5_13 = DbToMysql(config.EHCO_DB)
# store5_14 = DbToMysql(config.EHCO_DB)
# store5_15 = DbToMysql(config.EHCO_DB)
# store5_16 = DbToMysql(config.EHCO_DB)

host = '10.1.1.48:9092'
client = KafkaClient(hosts=host)
users_topic = client.topics[b'ins-users']  # 指定topic,没有就新建
useId_topic = client.topics[b'ins-user-id']

medias_topic = client.topics[b'ins-medias']  # 指定topic,没有就新建
users_1w_topic = client.topics[b'ins-users-1w-followed']  # 指定topic,没有就新建
username_only_topic = client.topics[b'username_only']  # 指定topic,没有就新建

producers = {'medias_producer': medias_topic.get_producer(), 'users_producer': users_topic.get_producer(),
             'users_1w_followed_pd': users_1w_topic.get_producer(), 'userId_producer': useId_topic.get_producer(),
             'username_only_topic': username_only_topic.get_producer(),
             }

following_consumer = users_topic.get_simple_consumer(consumer_group=b'ins-users', auto_commit_enable=True, auto_commit_interval_ms=5,
                                     consumer_id=b'ins-users')
userinfo_consumer = users_topic.get_simple_consumer(consumer_group=b'userinfo_consumer', auto_commit_enable=True,
                                              auto_commit_interval_ms=1, consumer_id=b'userinfo_consumer')
account_consumer = username_only_topic.get_simple_consumer(consumer_group=b'account_consumer', auto_commit_enable=True,
                                            auto_commit_interval_ms=1, consumer_id=b'account_consumer')
followed_consumer = users_topic.get_simple_consumer(consumer_group=b'followed_consumer', auto_commit_enable=True,
                                              auto_commit_interval_ms=5, consumer_id=b'followed_consumer')

app_mediaList_consumer = useId_topic.get_simple_consumer(consumer_group=b'app_mediaList_consumer',
                                                         auto_commit_enable=True,
                                            auto_commit_interval_ms=1, consumer_id=b'app_mediaList_consumer')

# SimpleConsumer无法扩展-如果您有两个 使用相同主题的SimpleConsumers，则它们将收到重复的消息。为了解决这个问题，您可以使用BalancedConsumer
# balanced_consumer = useId_topic.get_balanced_consumer(consumer_group='app_mediaList_balanced', auto_commit_enable=True,
#                                                       zookeeper_connect='10.1.1.48:2181 10.1.1.49:2181 10.1.1.50:2181')

# app_mediaList_consumer1 = useId_topic.get_simple_consumer(consumer_group=b'app_mediaList_consumer',
#                                                          auto_commit_enable=True,
#                                             auto_commit_interval_ms=1, consumer_id=b'app_mediaList_consumer1')

def consumerMsg(store, producers, consumer, target):
        print('hello')
        for message in consumer:
            try:
                if message is not None:
                    user = json.loads(message.value)
                    # print('准备处理：uid={},username={}'.format(user['uid'], user['username']))
                    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user['uid'])
                    if target.__name__ == 'spide_user_by_acount':
                        target(user['username'], store, producers)
                    else:
                        target(user['uid'], store, producers)
                    # print('处理结束：uid={},username={}'.format(user['uid'], user['username']))
            except Exception as e:
                print(target.__name__, '发生异常退出', e, message)
        # producers['medias_producer'].stop()
        # producers['users_producer'].stop()
        # store.close()


def balanced_consumer(my_store, my_producers):

    balanced_consumer = useId_topic.get_balanced_consumer(consumer_group=b'app_mediaList_balanced2',
                                                          auto_commit_enable=True,
                                                          zookeeper_connect='10.1.1.48:2181,10.1.1.49:2181,10.1.1.50:2181')
    for message in balanced_consumer:
        try:
            if message is not None:
                user = json.loads(message.value)
                # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user['uid'], threading.currentThread().name)
                # spide_insapp_medialist_by_uid(user['uid'], my_store, my_producers)
                spide_insapp_product_list_by_uid(user['uid'], my_store, my_producers)

        except Exception as e:
            print('balanced_consumer发生异常退出', e, message)


def main():
    # spide_userFollowing_by_uid(user['uid'], store, producer)
    # spide_user_by_uid(user['uid'], store, producer)
    # spide_userFollowed_by_uid(user['uid'], store, producer)
    # spide_user_by_acount(user['username'], store, producer)

    try:
        minus = 60
        threads = []
        for j in range(0, 1, 1):
            m_store = DbToMysql(config.EHCO_DB)
            thread = threading.Thread(target=balanced_consumer, args=(m_store, producers))
            threads.append(thread)
            thread.start()
        # # 等待所有线程完成
        # for t in threads:
        #     print('线程{}启动'.format(t.getName()))
        #     t.join()
        # thread0 = threading.Thread(target=gl.monitor,  args=(minus,store))
        # thread1 = threading.Thread(target=consumerMsg, args=(store1, producers, following_consumer, spide_userFollowing_by_uid))
        # thread2 = threading.Thread(target=consumerMsg, args=(store2, producers, userinfo_consumer, spide_user_by_uid))
        # thread3 = threading.Thread(target=consumerMsg, args=(store3, producers, followed_consumer, spide_userFollowed_by_uid))
        # thread4 = threading.Thread(target=consumerMsg, args=(store4, producers, account_consumer, spide_user_by_acount))
        # thread5_0 = threading.Thread(target=consumerMsg, args=(store5_0, producers, app_mediaList_consumer, spide_insapp_medialist_by_uid))
        # thread5_1 = threading.Thread(target=consumerMsg,
        #                            args=(store5_1, producers, app_mediaList_consumer1, spide_insapp_medialist_by_uid))

        # 开启新线程
        # thread0.start()
        # thread1.start()
        # thread2.start()
        # thread3.start()
        # thread5_0.start()

        # send_username_2_topic(store5, producers)
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

