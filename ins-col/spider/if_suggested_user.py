'''
爬取推荐流
并存入数据库
'''

from myspideCommon import *
import json
from pykafka import KafkaClient
from store import DbToMysql
import config

request_url = 'http://10.2.1.222:8091/api/Ins/SuggestedUsers?count=20'


# HEADERS = {
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36',
# }
#
# COOKIES = '''
# bid=i2jh3YGuvEo; ll="118163"; gr_user_id=f52deadb-5f46-491c-9b52-4294ab176b90; viewed="1430904_25806793_25901403"; ps=y; dbcl2="169273073:UOYgsqmzhSs"; ck=m7xV; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1509972471%2C%22https%3A%2F%2Fwww.douban.com%2Fsearch%3Fsource%3Dsuggest%26q%3D%25E9%259B%25B7%25E7%25A5%259E3%22%5D; ct=y; _vwo_uuid_v2=4EE59BFD9BF5C48E3E9020C6DE3564D4|d2deb1b903e06e32351743192190c582; ap=1; _pk_id.100001.4cf6=0377929d7299aea4.1508405769.14.1509975906.1509934106.; _pk_ses.100001.4cf6=*; __utma=30149280.341450299.1508041022.1509932565.1509972306.17; __utmb=30149280.32.10.1509972306; __utmc=30149280; __utmz=30149280.1509972306.17.13.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmv=30149280.16927; __utma=223695111.1000856564.1508405769.1509932565.1509972471.14; __utmb=223695111.0.10.1509972471; __utmc=223695111; __utmz=223695111.1509972471.14.10.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/search; push_noty_num=0; push_doumail_num=0
# '''


def main():
    host = '172.26.11.167:9092'
    client = KafkaClient(hosts=host)
    topic = client.topics[b'ins-users']  # 指定topic,没有就新建
    producer = topic.get_producer()
    store = DbToMysql(config.EHCO_DB)

    # sql = "select * from suggested_user where "
    # rst = store.query(sql)
    # print(rst)
    for i in range(0, 2000, 20):
        jsonStr = get_html_json(request_url)

        strDict = json.loads(jsonStr)

        if strDict['status'] == 'ok':
            result = list()
            try:
                edges = strDict['data']['user']['edge_suggested_users']['edges']

                for j in range(0, len(edges), 1):
                    # print(edges[j])
                    edges[j]['node']['user']
                    data = dict()
                    data['follower_count'] = edges[j]['node']['user']['edge_followed_by']['count']
                    data['uid'] = edges[j]['node']['user']['id']
                    data['full_name'] = edges[j]['node']['user']['full_name']
                    data['username'] = edges[j]['node']['user']['username']
                    data['is_private'] = edges[j]['node']['user']['is_private']
                    data['is_verified'] = edges[j]['node']['user']['is_verified']
                    data['pic_url'] = edges[j]['node']['user']['profile_pic_url']

                    sql = "select count(1) from suggested_user where uid ={}".format(data['uid'])
                    rst = store.query(sql)
                    # print(json.dumps(data).encode())

                    if len(rst) == 0:
                        store.save_one_data('suggested_user', data)
                        msg = json.dumps(data).encode()
                        producer.produce(msg)
                        result.append(data)
                    else:
                        print('该用户已存在：uid={},username={}'.format(data['uid'], data['username']))

            except AttributeError as e:
                print('数据有问题', e)
            print('第{}页保存完毕'.format(i))
            producer.stop()
            print('over')
            return result
        #     res_list = parse_detail(html)
        #     if res_list != -1:
        #         for data in res_list:
        #             store.save_one_data('suggested_user', data)
        #         print('第{}页保存完毕'.format(i))
    # store.close()


if __name__ == '__main__':
    main()
