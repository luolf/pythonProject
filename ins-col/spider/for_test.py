'''
爬取推荐流
并存入数据库
'''
from datetime import datetime

from ins_spider_from_swagger import *

store = DbToMysql(config.EHCO_DB)
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


user = dict()
user['uid'] = 2104845880
user['username'] = 'juanjaramilloe'

spide_user_by_acount(user['username'], store, producer)

sql = "select 1 from user_by_userid where uid ={}".format(user['uid'])
rst = store.query(sql)
c = len(rst)


if len(rst) == 0:




    sql = "select 1 from user_by_username where userName='_lannie____')"
    rst = store.query(sql)
    spide_userFollowing_by_uid(user['uid'], store, producer)
    if len(rst) == 0:
        # print(message.offset, message.value)
        # spide_userFollowing_by_uid(user['uid'], store, producer)
        # spide_user_by_uid(user['uid'], store, producer)
        # spide_userFollowed_by_uid(user['uid'], store, producer)
        spide_user_by_acount(user['username'], store, producer)
    else:
        print('接受消息user_by_userid 已存在：uid={},username={}'.format(user['uid'], user['username']))
    print('任务完成')
    producer.stop()
    store.close()
else:
    print('接受消息user_by_userid 已存在：uid={},username={}'.format(user['uid'], user['username']))


