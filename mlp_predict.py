#!/usr/bin/python
# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2019 ***.com, Inc. All Rights Reserved
# The NSH Anti-Plugin Project
################################################################################
"""
NSH主线挂自动迭代项目 -- MLP模型预测脚本

Usage: python mlp_predict.py --end_grade 41 --ds_pred 20190122 --ts_pred_start 20190122#13:20:00 --ds_start 20181212 --ds_num 28
Authors: Zhou Jialiang
Email: zjl_sempre@163.com
Date: 2019/02/13
"""

import os
import argparse
import json
import gc
import logging
import requests
from datetime import datetime, timedelta
import log
from MLPModel import MLPModel
from FeatureEngineering import EvfreqLoader_hbase_pred
from config import SAVE_DIR_BASE, PROJECT_DIR
from config import FETCH_ID_URL, INSERT_SQL, TIME_FORMAT, MINUTE_DELTA
from config import MySQL_HOST_IP, MySQL_HOST_PORT, MySQL_HOST_USER, MySQL_HOST_PASSWORD, MySQL_TARGET_DB
from MySQLUtils import MysqlDB


def fetch_id(grade, ts_start, ts_end):
    '''实时接口
    实时接口获取id
    需到达42级的才能确保41级的行为序列完整

    Args:
        grade: 结束等级
        ts_start: 预测行为开始时间
        ts_end: 预测行为结束时间

    Returns:
        待预测用户ID
    '''
    url = FETCH_ID_URL.format(st=ts_start, ed=ts_end)
    try:
        r = requests.post(url, timeout=600)
        result = r.json()['result']
    except Exception as e:
        print('fetch_id error, url={}, e={}'.format(url, e))
        return []

    ids = [i['role_id'] for i in result if i['level'] == (grade + 1)]
    return ids


if __name__ == '__main__':

    # 参数设置，ds_range选择模型，ds_pred指定预测日期
    parser = argparse.ArgumentParser('MLP Model Train, feature generation and model train. \n'
                                     'Usage: python MLPModel --ds_range --ds_pred ..')
    parser.add_argument('--end_grade', type=int)
    parser.add_argument('--ds_start', type=str)
    parser.add_argument('--ds_num', type=int)
    parser.add_argument('--ds_pred', help='data', type=str)
    parser.add_argument('--ts_pred_start', help='data', type=str)
    parser.add_argument('--method', help='\'mlp_41_baseline\' or \'mlp_41_incre\' or \'mlp_41_window\'')
    args = parser.parse_args()
    method = args.method
    end_grade = args.end_grade
    ds_start = args.ds_start
    ds_num = args.ds_num
    ds_pred = args.ds_pred
    ts_pred_start = args.ts_pred_start.replace('#', ' ')
    ts_pred_end = (datetime.strptime(ts_pred_start, TIME_FORMAT) + timedelta(minutes=MINUTE_DELTA)).strftime(TIME_FORMAT)

    # logging
    log.init_log(os.path.join(PROJECT_DIR, 'logs', 'mlp_predict'))

    # logid
    logid_path = os.path.join(PROJECT_DIR, 'logid', '41')
    data_path = os.path.join(SAVE_DIR_BASE, 'data', ds_pred)
    logging.info('Data source path: {}'.format(data_path))

    # 获取待预测实时id
    ts_start = ts_pred_start[:4] + '-' + ts_pred_start[4:6] + '-' + ts_pred_start[6:]
    ts_end = ts_pred_end[:4] + '-' + ts_pred_end[4:6] + '-' + ts_pred_end[6:]
    ids_to_pred = fetch_id(grade=end_grade, ts_start=ts_start, ts_end=ts_end)
    logging.info('Num of ids to predict: {}'.format(len(ids_to_pred)))
    with open(os.path.join(SAVE_DIR_BASE, 'trigger', ts_pred_start.replace(' ', '_').replace('-', '').replace(':', '')), 'w') as f:
        json.dump(ids_to_pred, f, indent=4, sort_keys=True)

    # 获取行为序列
    cmd = '/usr/bin/python3 {PROJECT_DIR}/dataloader.py --end_grade {end_grade} ' \
          '--ds_start {ds_start} --ds_num {ds_num} --ts_pred_start {ts_pred_start}'.format(
            PROJECT_DIR=PROJECT_DIR,
            end_grade=end_grade,
            ds_start=ts_pred_start.split()[0].replace('-', ''),
            ds_num=1,
            ts_pred_start=ts_pred_start.replace(' ', '_').replace('-', '').replace(':', '')
    )
    logging.info(cmd)
    os.system(cmd)

    # 读取数据，提取特征
    logging.info('label_tags: {}'.format(ts_pred_start.split(' ')[-1].replace(':', '')))
    data = EvfreqLoader_hbase_pred(source_path_list=[data_path], logid_path=logid_path, sampling_type='up', test_size=0.0, label_tags=[ts_pred_start.split(' ')[-1].replace(':', '')])
    data.run_load()

    # 加载模型，预测
    ds_start_num = '{}_{}'.format(ds_start, ds_num)
    model_name = sorted(os.listdir(os.path.join(SAVE_DIR_BASE, 'model', ds_start_num)))[-1]
    model_path = os.path.join(SAVE_DIR_BASE, 'model', ds_start_num, model_name)
    logging.info('Loading model: {}'.format(model_path))
    model = MLPModel(train_data=data.total_data, test_data=data.total_data, feature_type='freq', ids=data.ids)
    results = model.run_predict(model_path=model_path, ts_pred_start=ts_pred_start)
    logging.info('Done predicting ids on date: {}'.format(ds_pred))

    # 预测结果上传MySQL
    ids, scores = zip(*results)
    db = MysqlDB(host=MySQL_HOST_IP, port=MySQL_HOST_PORT, user=MySQL_HOST_USER, passwd=MySQL_HOST_PASSWORD, db=MySQL_TARGET_DB)
    db.upload_ids(sql_base=INSERT_SQL, ids=ids, scores=scores, method=method , ts_start=ts_start, ts_end=ts_end)

    del data, model
    gc.collect()
