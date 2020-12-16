#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/14 15:22
# @Author: 关注的人
# @File  : ins_spider_from_swagger.py

import json
from datetime import datetime

from pykafka import KafkaClient
from store import DbToMysql
import config

from myspideCommon import *
import gl

html_str = '<!DOCTYPE'


# 通过播主账号查找信息
def spide_user_by_acount(user_name, store, producers):
    request_url = 'http://10.2.1.222:8091/api/Ins/UserInfoByUserName?userName={user_name}'


    try:
        sql = "select 1 from user_by_username where userName='{}'".format(user_name)
        rst = store.query(sql)
        if len(rst) > 0:
            return None
        jsonStr = get_html_json(request_url.format(user_name=user_name))
        gl.if_username_invokes = gl.if_username_invokes + 1
        if len(jsonStr) == 0:
            # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_user_by_acount 返回空')
            gl.if_username_empty = gl.if_username_empty + 1
            return None

        strDict = json.loads(jsonStr)
        result = save_user_ext_edges(strDict['graphql']['user'], store)
        gl.if_username_rows = gl.if_username_rows + 1
        # send kafka
        # cnt = 0
        # while cnt < len(result):
        #     msg = json.dumps(result[cnt]).encode()
        #     producer.produce(msg)
        #     cnt = cnt + 1
    except AttributeError as e:
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e, 'spide_user_by_acount数据有问题，user_name={},jsonStr={}'.format(user_name, jsonStr))
    # finally:
    #     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_user_by_acount数据完成，user_name={}'.format(user_name))


# 通过播主id查找信息
def spide_user_by_uid(uid, store, producers):
    try:
        request_url = 'http://10.2.1.222:8091/api/Ins/UserInfoByUserId?userId={uid}'
        sql = "select 1 from user_by_userid where uid={}".format(uid)
        rst = store.query(sql)
        if len(rst) > 0:
            return None
        jsonStr = get_html_json(request_url.format(uid=uid))
        gl.if_user_invokes = gl.if_user_invokes + 1
        if len(jsonStr) == 0:
            # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_user_by_uid 返回空')
            gl.if_user_empty = gl.if_user_empty + 1
            return None

        strDict = json.loads(jsonStr)
        if strDict['status'] != 'ok':
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_user_by_uid status is not ok，uid={}'.format(uid))
            return
        result = save_user_edges(strDict['user'], store)
        gl.if_user_rows = gl.if_user_rows + 1

        # send kafka 万粉以上播主
        # cnt = 0
        # while cnt < len(result):
        #     msg = json.dumps(result[cnt]).encode()
        #     producers['users_1w_topic'].produce(msg)
        #     cnt = cnt + 1

    except Exception as e:
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e, 'spide_user_by_uid数据有问题，uid={},url={},jsonStr={}'.format(uid, request_url.format(uid=uid), jsonStr))
    # finally:
    #     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_user_by_uid数据完成，uid={}'.format(uid))


# 关注的人列表
def spide_userFollowing_by_uid(uid, store, producers):
    request_url = 'http://10.2.1.222:8091/api/Ins/UserFollowing?userId={uid}&end_cursor={cursor}'
    try:
        end_cursor = ''
        sql = "select 1 from user_follow where owner_uid={}".format(uid)
        rst = store.query(sql)
        if len(rst) > 0:
            return None

        flag = 1  # 定义一个退出循环的标志

        while flag:
            try:

                # if not firstPage:
                jsonStr = get_html_json(request_url.format(uid=uid, cursor=end_cursor))
                gl.if_following_invokes = gl.if_following_invokes + 1
                if jsonStr.find(html_str) >= 0:
                    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowing_by_uid 返回html结构,url={}'.format(request_url.format(uid=uid, cursor=end_cursor)))
                    return None
                if len(jsonStr) == 0:
                    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowing_by_uid 返回空')
                    gl.if_following_empty = gl.if_following_empty + 1
                    return None

                strDict = json.loads(jsonStr)
                if strDict['status'] != 'ok':
                    return
                # firstPage = 0

                count = strDict['data']['user']['edge_follow']['count']
                page_info = strDict['data']['user']['edge_follow']['page_info']
                edges = strDict['data']['user']['edge_follow']['edges']
                if count > 0:
                    result = save_user_follow_edges(uid, edges, store)
                    # send kafka
                    cnt = 0
                    while cnt < len(result):
                        msg = json.dumps(result[cnt]).encode()
                        producers['users_producer'].produce(msg)
                        cnt = cnt + 1

                if not page_info['has_next_page']:
                    flag = 0
                # else:
                #     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowing_by_uid uid={}还有下一页'.format(uid))
                end_cursor = page_info['end_cursor']
                gl.if_following_rows = gl.if_following_rows + cnt
            except Exception as e:
                flag = 0
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e, 'spide_userFollowing_by_uid数据有问题uid={},end_cursor={},url={},jsonStr={}'.format(uid, end_cursor, request_url.format(uid=uid, cursor=end_cursor), jsonStr))
    except Exception as e:
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowing_by_uid数据有问题uid={}'.format(uid), e)


# 我的粉丝
def spide_userFollowed_by_uid(uid, store, producers):
    request_url = 'http://10.2.1.222:8091/api/Ins/UserFollowedBy?userId={uid}&end_cursor={cursor}'
    try:
        end_cursor = ''
        total = 0
        sql = "select 1 from user_followed where owner_uid={}".format(uid)
        rst = store.query(sql)
        if len(rst) > 0:
            return None
        flag = 1  # 定义一个退出循环的标志


        while flag:
            try:
                # if not firstPage:
                jsonStr = get_html_json(request_url.format(uid=uid, cursor=end_cursor))
                gl.if_followed_invokes = gl.if_followed_invokes + 1
                if jsonStr.find(html_str) >= 0:
                    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowed_by_uid 返回html结构,url={}'.format(request_url.format(uid=uid,cursor=end_cursor)))
                    return None
                if len(jsonStr) == 0:
                    # print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowed_by_uid 返回空')
                    gl.if_followed_empty = gl.if_followed_empty + 1
                    return None

                strDict = json.loads(jsonStr)

                if strDict['status'] != 'ok':
                    return None
                # firstPage = 0

                count = strDict['data']['user']['edge_followed_by']['count']
                page_info = strDict['data']['user']['edge_followed_by']['page_info']
                edges = strDict['data']['user']['edge_followed_by']['edges']
                if count > 0:
                    result = save_user_followed_edges(uid, edges, store)
                    # send kafka
                    cnt = 0
                    while cnt < len(result):
                        msg = json.dumps(result[cnt]).encode()
                        producers['users_producer'].produce(msg)
                        cnt = cnt + 1
                    total = total + cnt
                if not page_info['has_next_page']:
                    flag = 0
                # else:
                #     print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowed_by_uid uid={}还有下一页'.format(uid))
                if total > 1000:
                    flag = 0
                gl.if_followed_rows = gl.if_followed_rows + total
                end_cursor = page_info['end_cursor']
            except Exception as e:
                flag = 0
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e, 'spide_userFollowed_by_uid数据有问题uid={},end_cursor={},jsonStr={},url={}'.format(uid, end_cursor, request_url.format(uid=uid, cursor=end_cursor), jsonStr))
    except Exception as e:
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'spide_userFollowed_by_uid数据有问题 uid={}'.format(uid), e)
