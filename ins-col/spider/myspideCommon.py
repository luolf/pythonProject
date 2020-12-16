#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 15:50
# @Author: zhangtao
# @File  : myspideCommon.py
import time
import os

import requests
from bs4 import BeautifulSoup
from http.cookies import SimpleCookie
import json
from store import DbToMysql
import config


# 推荐流
def deal_save_user_edges(edges, store):
    # 实际处理的数据
    result = list()
    for j in range(0, len(edges), 1):
        edges[j]['node']
        data = dict()
        data['follower_count'] = edges[j]['node']['edge_followed_by']['count']
        data['uid'] = edges[j]['node']['id']
        data['full_name'] = edges[j]['node']['full_name']
        data['username'] = edges[j]['node']['username']
        data['is_private'] = edges[j]['node']['is_private']
        data['is_verified'] = edges[j]['node']['is_verified']
        data['pic_url'] = edges[j]['node']['profile_pic_url']

        # sql = "select count(1) from suggested_user where uid ={}".format(data['uid'])
        sql = "select 1 from suggested_user where uid ={}".format(data['uid'])
        rst = store.query(sql)
        if len(rst) == 0:
            store.save_one_data('suggested_user', data)
            result.append(data)
        else:
            print('该用户已存在：uid={},username={}'.format(data['uid'], data['username']))
    return result


# 用户的粉丝
def save_user_followed_edges(owner_uid, edges, store):
    # 实际处理的数据
    result = list()
    for j in range(0, len(edges), 1):
        data = dict()
        # data['follower_count'] = edges[j]['node']['edge_followed_by']['count']
        data['owner_uid'] = owner_uid
        data['uid'] = edges[j]['node']['id']
        data['full_name'] = edges[j]['node']['full_name']
        data['username'] = edges[j]['node']['username']
        data['is_private'] = edges[j]['node']['is_private']
        data['is_verified'] = edges[j]['node']['is_verified']
        data['pic_url'] = edges[j]['node']['profile_pic_url']
        sql = "select 1 from user_followed where owner_uid={} and uid ={}".format(owner_uid, data['uid'])
        rst = store.query(sql)
        if len(rst) == 0:
            store.save_one_data('user_followed', data)
            result.append(data)
        else:
            print('user_followed 已存在：owner_uid={},uid={},username={}'.format(owner_uid, data['uid'], data['username']))
    return result


# 保存通过用户名获取的信息
def save_user_ext_edges(user, store):
    # 实际处理的数据
    result = list()
    data = dict()
    data['uid'] = user['id']
    data['username'] = user['username']
    data['full_name'] = user['full_name']
    data['pic_url'] = user['profile_pic_url']
    data['external_url'] = user['external_url']
    data['biography'] = user['biography']
    data['edge_followed_by'] = user['edge_followed_by']['count']
    data['edge_follow'] = user['edge_follow']['count']
    data['edge_owner_to_timeline_media'] = user['edge_owner_to_timeline_media']['count']
    data['edge_felix_video_timeline'] = user['edge_felix_video_timeline']['count']
    data['fbid'] = user['fbid']
    data['is_business_account'] = user['is_business_account']
    data['business_category_name'] = user['business_category_name']
    data['overall_category_name'] = user['overall_category_name']
    data['category_enum'] = user['category_enum']
    data['category_name'] = user['category_name']
    data['is_private'] = user['is_private']
    data['is_verified'] = user['is_verified']

    sql = "select 1 from user_by_username where uid ={}".format(data['uid'])
    rst = store.query(sql)
    if len(rst) == 0:
        store.save_one_data('user_by_username', data)
        result.append(data)
    else:
        print('user_by_username 已存在：uid={},username={}'.format(data['uid'], data['username']))
    return result


# 保存通过用户id获取的信息
def save_user_edges(user, store):
    # 实际处理的数据
    result = list()
    data = dict()
    data['uid'] = user['pk']
    data['username'] = user['username']
    data['full_name'] = user['full_name']
    data['profile_pic_url'] = user['profile_pic_url']
    data['external_url'] = user['external_url']
    data['biography'] = user['biography']
    data['follower_count'] = user['follower_count']
    data['following_count'] = user['following_count']
    data['total_igtv_videos'] = user['total_igtv_videos']
    data['following_tag_count'] = user['following_tag_count']
    data['usertags_count'] = user['usertags_count']
    data['media_count'] = user['media_count']
    data['is_private'] = user['is_private']
    data['is_verified'] = user['is_verified']
    data['is_business'] = user['is_business']
    data['account_type'] = user['account_type']
    # try:
    #     data['category'] = user['category']
    #     data['city_name'] = user['city_name']
    #     data['city_id'] = user['city_id']
    #     data['contact_phone_number'] = user['contact_phone_number']
    #     data['public_email'] = user['public_email']
    #     data['public_phone_country_code'] = user['public_phone_country_code']
    #     data['public_phone_number'] = user['public_phone_number']
    #     data['latitude'] = user['latitude']
    #     data['longitude'] = user['longitude']
    # except Exception as e:
    #     print('save_user_edges 无此属性返回', e)
    # data['fbid'] = user['fbid']

    if 'category' in user:
        data['category'] = user['category']
    if 'city_name' in user:
        data['city_name'] = user['city_name']
    if 'city_id' in user:
        data['city_id'] = user['city_id']
    if 'contact_phone_number' in user:
        data['contact_phone_number'] = user['contact_phone_number']
    if 'public_email' in user:
        data['public_email'] = user['public_email']
    if 'public_phone_country_code' in user:
        data['public_phone_country_code'] = user['public_phone_country_code']
    if 'public_phone_number' in user:
        data['public_phone_number'] = user['public_phone_number']
    if 'latitude' in user:
        data['latitude'] = user['latitude']
    if 'longitude' in user:
        data['longitude'] = user['longitude']

    sql = "select 1 from user_by_userid where uid ={}".format(data['uid'])
    rst = store.query(sql)
    if len(rst) == 0:
        store.save_one_data('user_by_userid', data)
        result.append(data)
    else:
        print('user_by_userid 已存在：uid={},username={}'.format(data['uid'], data['username']))
    return result


# 关注的用户
def save_user_follow_edges(owner_uid, edges, store):
    # 实际处理的数据
    result = list()
    for j in range(0, len(edges), 1):
        data = dict()
        # data['follower_count'] = edges[j]['node']['edge_followed_by']['count']
        data['owner_uid'] = owner_uid
        data['uid'] = edges[j]['node']['id']
        data['full_name'] = edges[j]['node']['full_name']
        data['username'] = edges[j]['node']['username']
        data['is_private'] = edges[j]['node']['is_private']
        data['is_verified'] = edges[j]['node']['is_verified']
        data['pic_url'] = edges[j]['node']['profile_pic_url']
        sql = "select 1 from user_follow where owner_uid={} and uid ={}".format(owner_uid, data['uid'])
        rst = store.query(sql)
        if len(rst) == 0:
            store.save_one_data('user_follow', data)
            result.append(data)
        else:
            print('user_follow 已存在：owner_uid={},uid={},username={}'.format(owner_uid, data['uid'], data['username']))
    return result


def format_cookie(text):
    '''将字符串转换为字典形式的cookies'''
    cookie = SimpleCookie(text)
    return {i.key: i.value for i in cookie.values()}


def get_html_text(url, header={}, cookies={}):
    '''
    下载网页数据
    返回文本文件
    '''
    try:
        # 使用Session来最会话管理
        s = requests.Session()
        s.headers.update(header)
        s.cookies.update(cookies)
        r = s.get(url)
        r.raise_for_status
        return r.content
    except:
        return -1


def get_html_json(url, timeout=5):
    '模拟get请求，获取API接口数据'
    try:
        r = requests.get(url)
        r.raise_for_status
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return 'error'


def parse_detail(html):

    results = []
    try:
        soup = BeautifulSoup(html, 'lxml')
        comments = soup.find_all('div', class_='comment-item')
        for comment in comments:
            info = comment.find('span', class_='comment-info')
            name = info.contents[1].get_text().strip()
            try:
                # 针对没有评星的情况特殊处理
                star = info.contents[5]['title']
                time = info.contents[7].get_text().strip()
            except:
                star = '暂无评分'
                time = info.contents[5].get_text().strip()
            vote = comment.find('span', class_='votes').text.strip()
            content = comment.find('p').get_text().strip()
            results.append({
                'name': name,  # 作者名
                'star': star,  # 推荐程度
                'time': time,  # 时间
                'vote': vote,  # 赞同数
                'content': content  # 影评内容
            })
        return results
    except:
        print('内容解析错误')
        return -1


def cached_url(url):
    '''将访问过的url缓存到本地'''
    folder = 'cached_url'
    filename = url.split('?')[1].split('&')[0].split('=')[1] + '.html'
    path = os.path.join(folder, filename)
    # 如果文件缓存过了，读文件并返回
    if os.path.exists(path):
        with open(path, 'rb') as f:
            s = f.read()
            return s
    else:
        # 建立文件夹用于保存网页
        if not os.path.exists(folder):
            os.mkdir(folder)
        html = get_html_text(url, HEADERS, format_cookie(COOKIES))
        if html != -1:
            with open(path, 'wb') as f:
                f.write(html)
            return html
        else:
            print('{}下载失败'.format(filename))
            return -1
