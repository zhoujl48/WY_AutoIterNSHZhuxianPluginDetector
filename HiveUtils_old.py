#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Jialiang Zhou"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "Data模块: 用于从Hive获取数据ID（老接口）"
__usage__ = "供调用"

import os
import json
from impala.dbapi import connect
from hdfs import InsecureClient
from config import HIVE_HOST_IP, HIVE_HOST_PASSWORD, HIVE_HOST_PORT, HIVE_HOST_USER, HDFS_HOST_IP, HDFS_HOST_PORT


# HIVE类，获取数据
class HiveDataGetter(object):
    def __init__(self):
        self._conn = connect(host=HIVE_HOST_IP,
                             port=HIVE_HOST_PORT,
                             auth_mechanism='PLAIN',
                             user=HIVE_HOST_USER,
                             password=HIVE_HOST_PASSWORD)
        self._hdfs_client = InsecureClient('http://%s:%s' % (HDFS_HOST_IP, HDFS_HOST_PORT), user='')

    # 获取query结果
    def get_results(self, sql):
        print('Pulling data from HIVE...')
        with self._conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
        return results


# 若本地不存在，则从HIVE拉取，并保存至本地
def get_ids(sql, ids_path):
    id_getter = HiveDataGetter()
    ids = [str(item[0]) for item in id_getter.get_results(sql)[:-1]]
    with open(ids_path, 'w') as f:
        json.dump(ids, f, indent=4, sort_keys=True)
    return ids


if __name__ == '__main__':
    sql = """
        SELECT DISTINCT grade.role_id
        FROM luoge_nsh_ods.ods_nsh_upgrade grade 
        WHERE grade.role_level = 41
        AND grade.ds >= '2019-01-20'
    	AND grade.ds <= '2019-01-30'
        """

    ids = get_ids(sql, 'tmp_hive_old')
