#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#
# Copyright (c) 2019 ***.com, Inc. All Rights Reserved
# The NSH Anti-Plugin Project
################################################################################
"""
NSH主线挂自动迭代项目 -- 序列拉取模块

Usage: python dataloader.py --end_grade 41 --ds_start 20181215 --ds_num 7
Authors: Zhou Jialiang
Email: zjl_sempre@163.com
Date: 2019/02/13
"""

import os
import sys
import json
import requests
import argparse
import logging
import logging.handlers
from time import sleep
from queue import Queue
from threading import Thread, Lock
from datetime import datetime, timedelta
from config import SAVE_DIR_BASE, THREAD_NUM, HBASE_URL


# 获取锁对象
lock = Lock()


# 时间统计格式
TIME_FORMAT = '%Y-%m-%d'
# LOG配置
LOG_FILE = 'logs/dataloader_hbase.log'  # 日志文件
SCRIPT_FILE = 'dataloader_hbase'  # 脚本文件
LOG_LEVEL = logging.DEBUG  # 日志级别
LOG_FORMAT = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'  # 日志格式


def parse_args():
    parser = argparse.ArgumentParser('Pulling data sequneces'
                                     'Usage: python dataloader.py --end_grade 41 --ds_start 20181215 --ds_num 7')
    parser.add_argument('--end_grade', type=str)
    parser.add_argument('--ds_start', type=str)
    parser.add_argument('--ds_num', type=int)
    parser.add_argument('--ts_pred_start', type=str, default='')

    return parser.parse_args()


def init_log():
    handler = logging.handlers.RotatingFileHandler(LOG_FILE)  # 实例化handler
    formatter = logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter
    logger = logging.getLogger(SCRIPT_FILE)  # 获取logger
    logger.addHandler(handler)  # 为logger添加handler
    logger.setLevel(LOG_LEVEL)
    return logger


def get_ids(path_ids):
    """从 trigger file 获取样本id

    Args:
        path_ids: trigger file 的地址
    """
    ids = list()
    if os.path.exists(path_ids):
        with open(path_ids, 'r') as f:
            ids = json.load(f)
    else:
        print('No ban_ids file')
    return ids


class SequenceDataReader(Thread):
    """序列数据读取类

    Attributes:
        logger: 日志
        queue: 多线程队列
        start_grade: 开始等级
        end_grade: 结束等级
        save_dir: 序列保存路径
    """
    def __init__(self, logger, queue, start_grade, end_grade, save_dir):
        threading.Thread.__init__(self)
        self.logger = logger
        self.queue = queue
        self.start_grade = start_grade
        self.end_grade = end_grade
        self.save_dir = save_dir

    def read_data(self, role_id):
        """从hbase拉取数据

        Args:
            role_id: 样本用户ID

        Return:
            seq: 样本用户行为序列
        """
        url = HBASE_URL.format(sg=self.start_grade, eg=self.end_grade, ids=role_id)
        response = requests.post(url, timeout=600)
        results = response.json()
        result = results[0]

        logids = result['role_seq']
        seq = [k['logid'] for k in logids]

        return seq

    def save_to_file(self, role_id, seq):
        """保存行为序列

        Args:
            role_id: 样本用户ID
            seq: 样本用户行为序列
        """

        filename = os.path.join(self.save_dir, role_id)
        with open(filename, 'w') as f:
            json.dump(seq, f, indent=4, sort_keys=True)
    
    def run(self):
        """多线程拉取运行接口
        
        线程功能定义
        遍历队列中的样本ID，拉取行为序列，并保存至相应目录
        """
        
        # 全局锁
        global lock

        # 循环读取queue中数据
        while True:
            if self.queue.qsize() % 1000 == 0:
                self.logger.info('{} id left'.format(self.queue.qsize()))
            
            # 临界区：从队列获取role_id，需锁定
            lock.acquire()
            if self.queue.empty():
                lock.release()
                return
            role_id = self.queue.get()
            lock.release()


            try:
                # 读取数据
                seq = self.read_data(role_id)
                sleep(5)
                # 存入文件
                self.save_to_file(role_id, seq)
            except Exception as e:
                self.logger.error('error with id = {}, error = {}'.format(role_id, e))
                
                # 临界区：若失败则重新放入队列，需锁定
                lock.acquire()
                self.queue.put(role_id)
                lock.release()


# 主函数
def main(argv):
    """主函数

    拉取指定用户ID对应的0-41级行为序列并保存
    """
    
    # 输入参数：行为截止等级，开始和结束日期
    args = parse_args()
    start_grade = 0
    end_grade = args.end_grade
    ds_start = '{}-{}-{}'.format(args.ds_start[:4], args.ds_start[4:6], args.ds_start[6:])
    ts_pred_start = args.ts_pred_start


    # 遍历日期，按天拉取序列并保存
    for ds_delta in range(args.ds_num):

        # 数据日期
        ds_data = (datetime.strptime(ds_start, '%Y-%m-%d') + timedelta(days=ds_delta)).strftime('%Y-%m-%d')
        print('Start pulling total sequence on date: {}'.format(ds_data))

        # id文件
        if ts_pred_start == '':
            path_ids = os.path.join(SAVE_DIR_BASE, 'trigger', '{ds_data}_total'.format(ds_data=ds_data.replace('-', '')))
        else:
            path_ids = os.path.join(SAVE_DIR_BASE, 'trigger', '{ts_pred_start}'.format(ts_pred_start=ts_pred_start))
            print(path_ids)

        # logger
        _logger = init_log()

        # 队列，用于多线程
        queue = Queue()

        # 创建数据目录
        souce_dir = os.path.join(SAVE_DIR_BASE, 'data', ds_data.replace('-', ''))
        if not os.path.exists(souce_dir):
            os.mkdir(souce_dir)

        # 记录已保存序列的id，避免中断后重复拉取
        filenames = os.listdir(souce_dir)
        ids_exists = set([filename.split('_')[0] for filename in filenames])

        # 需拉取序列id放入队列
        for role_id in get_ids(path_ids):
            if role_id not in ids_exists:
                queue.put(role_id)

        # 线程
        thread_list = []
        thread_num = THREAD_NUM
        for i in range(thread_num):
            _logger.info('init thread = {}'.format(i))
            thread = SequenceDataReader(_logger, queue, start_grade, end_grade, souce_dir)
            thread_list.append(thread)
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

        print('Finish pulling total sequence on date: {}'.format(ds_data))


if __name__ == '__main__':
    main(sys.argv)
