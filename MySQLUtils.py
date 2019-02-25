#!/usr/bin/python
# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2019 ***.com, Inc. All Rights Reserved
# The NSH Anti-Plugin Project
################################################################################
"""
NSH主线挂自动迭代项目 -- MySQLUtils模块

Usage: 供调用
Authors: Zhou Jialiang
Email: zjl_sempre@163.com
Date: 2019/02/13
"""
import argparse
import logging
import pymysql
from config import MySQL_HOST_IP, MySQL_HOST_PORT, MySQL_HOST_USER, MySQL_HOST_PASSWORD, MySQL_TARGET_DB


class MysqlDB(object):
    """MySQL类

    Attributes:
        _conn: MySQL连接

    """
    def __init__(self, host, port, user, passwd, db):
        self._conn = pymysql.connect(host=host, port=port, user=user, password=passwd, database=db)
        logging.info('Init MySQL connection')

    def __del__(self):
        self._conn.close()

    # 获取预测为外挂的id
    def get_results(self, sql):
        """获取预测为外挂的id

        Args:
            sql: 查询语句

        Return:
            results: 查询结果
        """
        logging.info('Pulling data from MySQL...')
        cursor = self._conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    def _insert_row(self, sql):
        """单行插入操作

        Args:
            sql: 插入语句
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            self._conn.commit()
        except:
            self._conn.rollback()

    def upload_ids(self, sql_base, ids, scores, method, ts_start, ts_end):
        """批量上传

        Args:
            sql_base: 插入语句
            ids: 需上传用户ID
            scores: 需上传用户外挂疑似度
            method: 自动迭代方案，'mlp_41_baseline' \ 'mlp_41_incre' \ 'mlp_41_window'
            ts_start: 预测用户开始时间
            ts_end: 预测用户结束时间
        """
        logging.info('Start uploading ids with [{ts_start}] ~ [{ts_end}] to MySQL...'.format(ts_start=ts_start, ts_end=ts_end))
        for role_id, score in zip(ids, scores):
            sql = sql_base.format(id=role_id, score=float(score), method=method, st=ts_start, ed=ts_end)
            self._insert_row(sql)
        logging.info('{} ids uploaded...'.format(len(ids)))


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
        logging.info(row)