#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/15 15:50
# @Author: zhangtao
# @File  : myspideCommon.py
import time
import os
import traceback
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

    sql = "select 1 from user_by_username where username ='{}'".format(data['username'])
    rst = store.query(sql)
    if len(rst) == 0:
        store.save_one_data('user_by_username', data)
        result.append(data)
    else:
        print('user_by_username 已存在：username={}'.format(data['username']))
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


# 来自app接口的播主media列表
def save_medias_from_app(owner_uid, items, store):
    # 实际处理的数据
    result = list()
    t = time.time()
    curr = int(t)
    seconds_30day = 30*24*3600
    for j in range(0, len(items), 1):
        try:
            data = dict()

            data['taken_at'] = items[j]['taken_at']
            ttt = curr - data['taken_at']
            if ttt > seconds_30day:
                return 0
            data['pk'] = items[j]['pk']
            data['id'] = items[j]['id']
            data['owner_uid'] = owner_uid
            data['media_type'] = items[j]['media_type']
            data['code'] = items[j]['code']

            data['should_request_ads'] = items[j]['should_request_ads']
            data['is_paid_partnership'] = items[j]['is_paid_partnership']
            if 'comment_count' in items[j]:
                data['comment_count'] = items[j]['comment_count']
            if 'inline_composer_display_condition' in items[j]:
                data['inline_composer_display_condition'] = items[j]['inline_composer_display_condition']
            if 'inline_composer_imp_trigger_time' in items[j]:
                data['inline_composer_imp_trigger_time'] = items[j]['inline_composer_imp_trigger_time']
            data['like_count'] = items[j]['like_count']
            data['can_see_insights_as_brand'] = items[j]['can_see_insights_as_brand']
            if 'caption' in items[j] and items[j]['caption'] != 'null' and items[j]['caption'] is not None:
                data['caption_pk'] = items[j]['caption']['pk']
                data['caption_user_id'] = items[j]['caption']['user_id']
                data['caption_text'] = items[j]['caption']['text']
                data['caption_type'] = items[j]['caption']['type']
                data['caption_created_at'] = items[j]['caption']['created_at']
                data['caption_content_type'] = items[j]['caption']['content_type']
                data['caption_status'] = items[j]['caption']['status']
                data['caption_bit_flags'] = items[j]['caption']['bit_flags']
                data['caption_private_reply_status'] = items[j]['caption']['private_reply_status']
            data['user_pk'] = items[j]['user']['pk']
            data['user_username'] = items[j]['user']['username']
            data['user_full_name'] = items[j]['user']['full_name']
            data['user_pic_url'] = items[j]['user']['profile_pic_url']
            data['user_is_private'] = items[j]['user']['is_private']
            data['user_is_verified'] = items[j]['user']['is_verified']
            data['user_is_favorite'] = items[j]['user']['is_favorite']
            data['user_is_unpublished'] = items[j]['user']['is_unpublished']
            if 'show_shoppable_feed' in items[j]['user']:
                data['user_show_shoppable_feed'] = items[j]['user']['show_shoppable_feed']
            if 'shoppable_posts_count' in items[j]['user']:
                data['user_shoppable_posts_count'] = items[j]['user']['shoppable_posts_count']
            if 'can_be_reported_as_fraud' in items[j]['user']:
                data['user_can_be_reported_as_fraud'] = items[j]['user']['can_be_reported_as_fraud']
            if 'merchant_checkout_style' in items[j]['user']:
                data['user_merchant_checkout_style'] = items[j]['user']['merchant_checkout_style']
            if 'seller_shoppable_feed_type' in items[j]['user']:
                data['user_seller_shoppable_feed_type'] = items[j]['user']['seller_shoppable_feed_type']
            if 'account_badges' in items[j]['user']:
                data['user_account_badges'] = json.dumps(items[j]['user']['account_badges'])
            # data['product_tags'] = json.dumps(items[j]['product_tags'])
            data['product_tags'] = ''
            if 'product_tags' in items[j]:
                for kk in items[j]['product_tags']:
                    data['product_tags'] = data['product_tags'] + ","+kk
                if 'in' in items[j]['product_tags']:
                    carousel_null = dict()
                    save_product_tag(items[j]['product_tags']['in'], store, carousel_null, data)
            if 'carousel_media_count' in items[j]:
                data['carousel_media_count'] = items[j]['carousel_media_count']

            if 'carousel_media' in items[j]:
                for c in range(0, len(items[j]['carousel_media']), 1):
                    carousel = dict()
                    carousel['carousel_id'] = items[j]['carousel_media'][c]['id']
                    carousel['carousel_media_type'] = items[j]['carousel_media'][c]['media_type']
                    carousel['carousel_pk'] = items[j]['carousel_media'][c]['pk']
                    carousel['carousel_parent_id'] = items[j]['carousel_media'][c]['carousel_parent_id']
                    carousel['carousel_can_see_insights_as_brand'] = items[j]['carousel_media'][c]['can_see_insights_as_brand']
                    if 'product_tags' in items[j]['carousel_media'][c]:
                        data['product_tags'] = data['product_tags'] + "#"
                        for kk in items[j]['carousel_media'][c]['product_tags']:
                            data['product_tags'] = data['product_tags'] + "," + kk
                        if 'in' in items[j]['carousel_media'][c]['product_tags']:
                            save_product_tag(items[j]['carousel_media'][c]['product_tags']['in'], store, carousel, data)

            # sql = "select 1 from InsApp_MediaListByUserId where code='{}'".format(data['code'])
            # rst = store.query(sql)
            # if len(rst) == 0:
            store.save_one_data('InsApp_MediaListByUserId', data)
                # result.append(data)
            # else:
            #     print('InsApp_MediaListByUserId 已存在：code={code},uid={uid},username={username}'.format(code=data['code'], uid=data['user_pk'], username=data['user_username']))

        except Exception as ie:
            print(':INSAPP save_medias_from_app uid={uid} ,idx={idx}'.format(uid=owner_uid, idx=j), traceback.print_exc())
            # print(ie, ':INSAPP save_medias_from_app uid={uid} ,idx={idx}'.format(uid=uid, idx=j))
            # print('traceback.print_exc():', traceback.print_exc())
            # print('traceback.format_exc():\n%s' , traceback.format_exc())
    return 1


# 来自app接口的播主产品列表
def save_products_from_app(owner_uid, items, store):
    for j in range(0, len(items), 1):
        try:
            if 'product' in items[j]:
                product = dict()
                product['owner_uid'] = owner_uid
                product['product_in_name'] = items[j]['product']['name']
                product['product_in_price'] = items[j]['product']['price']
                product['product_in_current_price'] = items[j]['product']['current_price']
                product['product_in_full_price'] = items[j]['product']['full_price']
                product['product_in_id'] = items[j]['product']['product_id']
                product['product_in_merchant_pk'] = items[j]['product']['merchant']['pk']
                product['product_in_merchant_username'] = items[j]['product']['merchant']['username']
                product['product_in_merchant_profile_pic_url'] = items[j]['product']['merchant']['profile_pic_url']
                product['product_in_compound_product_id'] = items[j]['product']['compound_product_id']
                product['product_in_description'] = items[j]['product']['description'][0:1000]
                if 'product_in_retailer_id' in items[j]['product']:
                    product['product_in_retailer_id'] = items[j]['product']['retailer_id']
                product['product_in_can_share_to_story'] = items[j]['product']['can_share_to_story']
                product['product_in_full_price_stripped'] = items[j]['product']['full_price_stripped']
                product['product_in_current_price_stripped'] = items[j]['product']['current_price_stripped']
                if 'checkout_style' in items[j]['product']:
                    product['product_in_checkout_style'] = items[j]['product']['checkout_style']
                if 'external_url' in items[j]['product']:
                    product['product_in_external_url'] = items[j]['product']['external_url']
                if 'main_image' in items[j]['product'] and 'image_versions2' in items[j]['product']['main_image']\
                        and 'candidates' in items[j]['product']['main_image']['image_versions2']:
                    if len(items[j]['product']['main_image']['image_versions2']['candidates']) >1:
                        product['product_in_main_image_url'] = items[j]['product']['main_image']['image_versions2']['candidates'][1]['url']

                store.save_one_data('insapp_product_list', product)
                print(':INSAPP save_products_from_app uid={uid} ,pid={pid}'.format(uid=owner_uid, pid=product['product_in_id']))
        except Exception as ie:
            print(':INSAPP save_products_from_app uid={uid} '.format(uid=owner_uid), traceback.print_exc())
    return 1
def save_product_tag(product_tags_in, store, carousel, media):
    for j in range(0, len(product_tags_in), 1):
        product = dict()
        if 'carousel_id' in carousel:
            product['carousel_id'] =carousel['carousel_id']
            product['carousel_media_type'] = carousel['carousel_media_type']
            product['carousel_pk'] = carousel['carousel_pk']
            product['carousel_parent_id'] = carousel['carousel_parent_id']
            product['carousel_can_see_insights_as_brand'] = carousel['carousel_can_see_insights_as_brand']

        product['item_id'] = media['id']
        product['item_pk'] = media['pk']
        product['item_user_pk'] = media['user_pk']
        product['owner_uid'] = media['owner_uid']
        product['item_code'] = media['code']
        product['item_media_type'] = media['media_type']

        product['product_in_name'] = product_tags_in[j]['product']['name']
        product['product_in_price'] = product_tags_in[j]['product']['price']
        product['product_in_current_price'] = product_tags_in[j]['product']['current_price']
        product['product_in_full_price'] = product_tags_in[j]['product']['full_price']
        product['product_in_id'] = product_tags_in[j]['product']['product_id']
        product['product_in_merchant_pk'] = product_tags_in[j]['product']['merchant']['pk']
        product['product_in_merchant_username'] = product_tags_in[j]['product']['merchant']['username']
        product['product_in_merchant_profile_pic_url'] = product_tags_in[j]['product']['merchant']['profile_pic_url']
        product['product_in_compound_product_id'] = product_tags_in[j]['product']['compound_product_id']
        product['product_in_description'] = product_tags_in[j]['product']['description'][0:1000]
        if 'product_in_retailer_id' in product_tags_in[j]['product']:
            product['product_in_retailer_id'] = product_tags_in[j]['product']['retailer_id']
        product['product_in_can_share_to_story'] = product_tags_in[j]['product']['can_share_to_story']
        product['product_in_full_price_stripped'] = product_tags_in[j]['product']['full_price_stripped']
        product['product_in_current_price_stripped'] = product_tags_in[j]['product']['current_price_stripped']
        # if product_tags_in[j]
        # product['product_in_position'] = json.dumps(product_tags_in[j]['position'])

        # sql = "select 1 from InsApp_product_tags_in where owner_uid={owner_uid} and item_code='{item_code}' and product_in_id={pid}"\
        #     .format(owner_uid=media['owner_uid'], item_code=media['code'], pid=product['product_in_id'])
        # rst = store.query(sql)
        # if len(rst) == 0:
        #     print('InsApp_product_tags_in save：item_code={item_code},owner_uid={owner_uid},product_in_id={pid}'
        #           .format(item_code=media['code'], owner_uid=media['owner_uid'], pid=product['product_in_id']))
        store.save_one_data('InsApp_product_tags_in', product)
            # result.append(data)
        # else:
        #     print('InsApp_product_tags_in 已存在：item_code={item_code},owner_uid={owner_uid},product_in_id={pid}'
        #           .format(item_code=media['code'], owner_uid=media['owner_uid'], pid=product['product_in_id']))



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
