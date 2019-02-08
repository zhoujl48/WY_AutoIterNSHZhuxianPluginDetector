#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "zhoujialiang"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "Data模块: Hive拉取ids"
__usage1__ = "python get_ids.py pos --end_grade 41 --ds_start 20181215 --ds_num 7"

import os
import argparse
import json
from  datetime import datetime, timedelta
from config import SAVE_DIR_BASE
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

    # trigger目录
    trigger_dir = os.path.join(SAVE_DIR_BASE, 'trigger')
    if not os.path.exists(trigger_dir):
        os.mkdir(trigger_dir)


    # 遍历日期，按天拉取获取封停id
    for ds_delta in range(args.ds_num):

        # 数据日期
        ds_data = (datetime.strptime(ds_start, '%Y-%m-%d') + timedelta(days=ds_delta)).strftime('%Y-%m-%d')
        print('Start pulling {} ids on date: {}'.format(label, ds_data))

        # query
        sql = QUERY_DICT[label].format(ds_portrait=ds_data, end_grade=end_grade, ds_start=ds_data, ds_end=ds_data)

        # 保存路径
        filename_ids = '{ds_data}_{label}'.format(ds_data=ds_data.replace('-', ''), label=label)
        ids_path = os.path.join(trigger_dir, filename_ids)
        if os.path.exists(ids_path):
            with open(ids_path, 'r') as f:
                ids = json.load(f)
                if len(ids) > 0:
                    print('File {} already exists, skip pulling ids'.format(ids_path))
                else:
                    print('File {} is empty, restart pulling ids'.format(ids_path))
                    ids = get_ids(sql, ids_path)
                    print('Finish pulling {} ids on date: {}'.format(label, ds_data))
        else:
            ids = get_ids(sql, ids_path)
            print('Finish pulling {} ids on date: {}'.format(label, ds_data))
