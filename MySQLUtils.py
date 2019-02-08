#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = "Jialiang Zhou"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "Data模块: 用于从MySQL获取数据"
__usage__ = "供调用"
import argparse
import pymysql
from config import MySQL_HOST_IP, MySQL_HOST_PORT, MySQL_HOST_USER, MySQL_HOST_PASSWORD, MySQL_TARGET_DB


# MySQL类
class MysqlDB(object):
    def __init__(self, host, port, user, passwd, db):
        self._conn = pymysql.connect(host=host, port=port, user=user, password=passwd, database=db)
        print('Init MySQL connection')

    def __del__(self):
        self._conn.close()

    # 获取预测为外挂的id
    def get_results(self, sql):
        print('Pulling data from MySQL...')
        cursor = self._conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    # 单行插入操作
    def _insert_row(self, sql):
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            self._conn.commit()
        except:
            self._conn.rollback()

    # 批量上传
    def upload_ids(self, sql_base, ids, scores, method, ts_start, ts_end):
        print('Start uploading ids with [{ts_start}] ~ [{ts_end}] to MySQL...'.format(ts_start=ts_start, ts_end=ts_end))
        for role_id, score in zip(ids, scores):
            sql = sql_base.format(id=role_id, score=float(score), method=method, st=ts_start, ed=ts_end)
            self._insert_row(sql)
        print('{} ids uploaded...'.format(len(ids)))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('ds')
    args = parser.parse_args()
    ds = args.ds

    QUERY_SQL = """
    SELECT *
    FROM anti_plugin.nsh_zhuxiangua
    WHERE suspect_score >= 0.5 
        AND LEFT(start_time, 10) = '{ds}'
        AND method = 'mlp_41_baseline'
    ORDER BY id DESC
    LIMIT 10
    """

    db = MysqlDB(host=MySQL_HOST_IP, port=MySQL_HOST_PORT, user=MySQL_HOST_USER, passwd=MySQL_HOST_PASSWORD, db=MySQL_TARGET_DB)
    for row in db.get_results(QUERY_SQL.format(ds=ds)):
        print(row)