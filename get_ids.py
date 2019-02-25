#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#
# Copyright (c) 2019 ***.com, Inc. All Rights Reserved
# The NSH Anti-Plugin Project
################################################################################
"""
NSH主线挂自动迭代项目 -- Hive拉取ids
从Hive获取指定等级、开始和结束日期的正负样本ID

Usage: python get_ids.py pos --end_grade 41 --ds_start 20181215 --ds_num 7
Authors: Zhou Jialiang
Email: zjl_sempre@163.com
Date: 2019/02/13
"""

import os
import argparse
import json
import logging
from datetime import datetime, timedelta
import log
from config import SAVE_DIR_BASE, PROJECT_DIR
from config import QUERY_DICT
from HiveUtils import get_ids


def parse_args():
    parser = argparse.ArgumentParser("Run trigger_sanhuan"
                                     "Usage: python get_ids.py pos --end_grade 41 --ds_start 20181215 --ds_num 7")
    parser.add_argument('label', help='\'pos\' or \'total\'')
    parser.add_argument('--end_grade', type=str)
    parser.add_argument('--ds_start', type=str)
    parser.add_argument('--ds_num', type=int)

    return parser.parse_args()


if __name__ == '__main__':

    # 输入参数：等级，开始和结束日期
    args = parse_args()
    label = args.label
    start_grade = 0
    end_grade = args.end_grade
    ds_start = datetime.strptime(args.ds_start, '%Y%m%d').strftime('%Y-%m-%d')

    # logging
    log.init_log(os.path.join(PROJECT_DIR, 'logs', 'get_ids'))

    # trigger目录
    trigger_dir = os.path.join(SAVE_DIR_BASE, 'trigger')
    if not os.path.exists(trigger_dir):
        os.mkdir(trigger_dir)


    # 遍历日期，按天拉取获取封停id
    for ds_delta in range(args.ds_num):

        # 数据日期
        ds_data = (datetime.strptime(ds_start, '%Y-%m-%d') + timedelta(days=ds_delta)).strftime('%Y-%m-%d')
        logging.info('Start pulling {} ids on date: {}'.format(label, ds_data))

        # query
        sql = QUERY_DICT[label].format(ds_portrait=ds_data, end_grade=end_grade, ds_start=ds_data, ds_end=ds_data)

        # 保存路径
        filename_ids = '{ds_data}_{label}'.format(ds_data=ds_data.replace('-', ''), label=label)
        ids_path = os.path.join(trigger_dir, filename_ids)
        if os.path.exists(ids_path):
            with open(ids_path, 'r') as f:
                ids = json.load(f)
                if len(ids) > 0:
                    logging.info('File {} already exists, skip pulling ids'.format(ids_path))
                else:
                    logging.info('File {} is empty, restart pulling ids'.format(ids_path))
                    ids = get_ids(sql, ids_path)
                    logging.info('Finish pulling {} ids on date: {}'.format(label, ds_data))
        else:
            ids = get_ids(sql, ids_path)
            logging.info('Finish pulling {} ids on date: {}'.format(label, ds_data))
