# -*- coding: utf-8 -*-

import base64
import datetime
import hashlib
import json
import os.path
import traceback
from datetime import datetime

import requests


# 上传图片，解析bytes
class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        """
        只要检查到了是bytes类型的数据就把它转为str类型
        :param obj:
        :return:
        """
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)

class WeChat(object):

    """use to alert"""

    corpid = 'ww738c853'
    corpsecret = '-D6NlbOgo9AC6tpvjQTBM'
    agentid = 100

    def __init__(self, init_message, webhook_url, proxies=True):
        self.proxies = "http://localhost:1080" if proxies else None
        self.header = {'Content-Type': 'application/json'}
        self.url = webhook_url
        self.last_send = ''
        self.init_message =init_message
        self.send(
            f'========INFO======\n{init_message}\nEvent:ProgramStart\nTime:{str(datetime.datetime.now())[:-7]}\n')

    def send(self, *mssg):
        text = ''
        for i in mssg:
            text += str(i)
        try:
            data = {
                "msgtype": "text",
                "text": {
                    "content": text + '\n' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            r = requests.post(self.url, data=json.dumps(data), timeout=10, proxies=self.proxies)
            print(f'调用企业微信接口返回： {r.text}')
            print('成功发送企业微信')
        except Exception as e:
            print(f"发送企业微信失败:{e}")
            print(traceback.format_exc())

    # 企业微信发送图片
    def send_img(self, file_path):
        if not os.path.exists(file_path):
            print('找不到图片')
            return
        try:
            with open(file_path, 'rb') as f:
                image_content = f.read()
            image_base64 = base64.b64encode(image_content).decode('utf-8')
            md5 = hashlib.md5()
            md5.update(image_content)
            image_md5 = md5.hexdigest()
            data = {
                'msgtype': 'image',
                'image': {
                    'base64': image_base64,
                    'md5': image_md5
                }
            }
            # 服务器上传bytes图片的时候，json.dumps解析会出错，需要自己手动去转一下
            r = requests.post(self.url, data=json.dumps(data, cls=MyEncoder, indent=4), timeout=10, proxies=self.proxies)
            print(f'调用企业微信接口返回： {r.text}')
            print('成功发送企业微信')
        except Exception as e:
            print(f"发送企业微信失败:{e}")
            print(traceback.format_exc())
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    def error_report(self,error_message):
        """

        :param error_message:
        """
        self.send(
            f'========BREAK=======\n{self.init_message}nEvent:ProgramBreak\nTime:{str(datetime.datetime.now())[:-7]}\nExitlog:{error_message}')
