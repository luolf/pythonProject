#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2020/12/16 10:46
# @Author: zhangtao
# @File  : gl.py
from datetime import datetime
import time

if_following_invokes = 0
if_following_invoke_times = 0
if_followed_invokes = 0
if_followed_invoke_times = 0
if_user_invokes = 0
if_user_invoke_times = 0
if_username_invokes = 0
if_username_invoke_times = 0

if_following_rows = 0
if_followed_rows = 0
if_user_rows = 0
if_username_rows = 0

if_following_empty = 0
if_followed_empty = 0
if_user_empty = 0
if_username_empty = 0

if_following_return_html = 0
if_followed_return_html = 0
if_user_return_html = 0
if_username_return_html = 0

def monitor(seconds,store):
    # 每n秒执行一次
    global if_user_invokes, if_following_invoke_times, if_following_invokes, if_followed_invoke_times, if_followed_invokes, if_user_invoke_times, if_username_invokes, if_username_invoke_times, if_following_rows, if_followed_rows, if_user_rows, if_username_rows, if_user_empty, if_following_empty, if_username_empty, if_followed_empty, if_user_return_html, if_username_return_html, if_following_return_html, if_followed_return_html
    while True:
        data = {
            'if_user_invokes': if_user_invokes,
            'if_user_rows': if_user_rows,
            'if_user_empty': if_user_empty,
            'if_user_return_html': if_user_return_html,

            # 'if_user_invoke_times': if_user_invoke_times,
            'if_username_invokes': if_username_invokes,
            'if_username_rows': if_username_rows,
            'if_username_empty': if_username_empty,
            'if_username_return_html': if_username_return_html,

            'if_following_invokes': if_following_invokes,
            'if_following_rows': if_following_rows,
            'if_following_empty': if_following_empty,
            'if_following_return_html': if_following_return_html,
            # 'if_following_invoke_times': if_following_invoke_times,
            'if_followed_invokes': if_followed_invokes,
            'if_followed_rows': if_followed_rows,
            'if_followed_empty': if_followed_empty,
            'if_followed_return_html': if_followed_return_html,
            'event_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 'if_followed_invoke_times': if_followed_invoke_times,

            # 'if_username_invoke_times': if_username_invoke_times,

        }
        store.save_one_data('mn_if_spider', data)
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), data)
        if_following_invokes = 0
        if_following_invoke_times = 0
        if_followed_invokes = 0
        if_followed_invoke_times = 0
        if_user_invokes = 0
        if_user_invoke_times = 0
        if_username_invokes = 0
        if_username_invoke_times = 0
        if_following_rows = 0
        if_followed_rows = 0
        if_user_rows = 0
        if_username_rows = 0

        if_following_empty = 0
        if_followed_empty = 0
        if_user_empty = 0
        if_username_empty = 0

        if_following_return_html = 0
        if_followed_return_html = 0
        if_user_return_html = 0
        if_username_return_html = 0

        time.sleep(seconds)


