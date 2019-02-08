#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = "Jialiang Zhou"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "调试模块: 单天预测模块，用于调试"
__usage1__ = "/usr/bin/python3 predict.py --ds_start 20181212 --ds_num 28 --ds_pred 20190109 --minute_delta 15"
import os
import argparse
from datetime import datetime, timedelta


DS_FORMAT = '%Y%m%d'
TS_FORMAT = '%Y-%m-%d#%H:%M:%S'
CMD_BASE = '/usr/bin/python3 mlp_predict.py --end_grade {end_grade} --ds_pred {ds_pred} --ts_pred_start {ts_pred_start} ' \
           '--ds_start {ds_start} --ds_num {ds_num} --method {method}'



parser = argparse.ArgumentParser('Time setting of prediction module')
parser.add_argument('--ds_start')
parser.add_argument('--ds_num', type=int)
parser.add_argument('--ds_pred')
parser.add_argument('--minute_delta', default=15 , type=int)
parser.add_argument('--method', help='\'mlp_41_baseline\' or \'mlp_41_incre\' or \'mlp_41_window\'')
args = parser.parse_args()
ds_start = args.ds_start
ds_num = args.ds_num
ds_pred = args.ds_pred
minute_delta = args.minute_delta
method = args.method


# 预测时间
ts_pred = (datetime.strptime(ds_pred, DS_FORMAT)).strftime(TS_FORMAT)
while ts_pred.split('#')[0].replace('-', '') == ds_pred:

    cmd = CMD_BASE.format(end_grade=41, ds_pred=ds_pred, ts_pred_start=ts_pred, ds_start=ds_start, ds_num=ds_num, method=method)
    print('Predicting on ts: {}'.format(ts_pred))
    os.system(cmd)

    ts_pred = (datetime.strptime(ts_pred, TS_FORMAT) + timedelta(minutes=minute_delta)).strftime(TS_FORMAT)

