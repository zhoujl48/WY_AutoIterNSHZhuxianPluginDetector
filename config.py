#!/usr/bin/python
# -*- coding:utf-8 -*-
################################################################################
#
# Copyright (c) 2019 ***.com, Inc. All Rights Reserved
# The NSH Anti-Plugin Project
################################################################################
"""
NSH主线挂自动迭代项目 -- 配置模块

Usage: 供调用
Authors: Zhou Jialiang
Email: zjl_sempre@163.com
Date: 2019/02/13
"""

################################################################################

# 项目根目录
PROJECT_DIR = '/home/zhoujialiang/online_zhuxian'

# 数据根目录
SAVE_DIR_BASE = '/srv/nsh-antiplugin-dataset/zhuxiangua/'

################################################################################

# Hive Config
HIVE_HOST_IP = '***.***.***.***'
HIVE_HOST_PORT = 10000
HIVE_HOST_USER = '***'
HIVE_HOST_PASSWORD = '***'
HIVE_TARGET_DB = '***'
HIVE_DB_DIR = '/user/hive/warehouse/%s.db/' % HIVE_TARGET_DB
HDFS_HOST_IP = '***.***.***.***'
HDFS_HOST_PORT = 14000

# Query，获取主线挂模型训练正样本id
QUERY_SQL_POS = """
SELECT ban.role_id, ban.ds_ban AS ds_ban, grade.ds AS ds_grade
FROM (
    SELECT portrait.role_id, max(ban_unban.ds) AS ds_ban
    FROM (
        SELECT role_id, role_account_name
        FROM luoge_nsh_mid.mid_role_portrait_all_d
        WHERE ds = '{ds_portrait}'
    ) portrait
    JOIN (
        SELECT ban.role_account_name, ban.ds
        FROM ( 
            SELECT role_account_name, ds
            FROM luoge_nsh_dwd.dwd_nsh_account_ban_add_d 
            WHERE opr_type = 'banLogin'
        ) ban
        LEFT JOIN (
            SELECT role_account_name
            FROM luoge_nsh_dwd.dwd_nsh_account_ban_add_d 
            WHERE opr_type = 'unbanLogin'
        ) unban
        ON ban.role_account_name = unban.role_account_name
        WHERE unban.role_account_name IS NULL
    ) ban_unban 
    ON portrait.role_account_name = ban_unban.role_account_name
    GROUP BY portrait.role_id
) ban
JOIN luoge_nsh_ods.ods_nsh_upgrade grade 
ON ban.role_id = grade.role_id
WHERE grade.role_level = {end_grade}
    AND grade.ds >= '{ds_start}'
	AND grade.ds <= '{ds_end}'
	AND datediff(ban.ds_ban, grade.ds) <= 2
	AND datediff(ban.ds_ban, grade.ds) >= 0
"""

# Query，获取主线挂模型训练负样本id
QUERY_SQL_NEG = """
SELECT DISTINCT grade.role_id
FROM luoge_nsh_ods.ods_nsh_upgrade grade 
LEFT JOIN (
    SELECT portrait.role_id
    FROM luoge_nsh_mid.mid_role_portrait_all_d portrait
    JOIN luoge_nsh_dwd.dwd_nsh_account_ban_add_d ban
    ON portrait.role_account_name = ban.role_account_name
    WHERE portrait.ds = '{ds_portrait}'
        AND ban.opr_type = 'banLogin'
        AND ban.ds <= '{ds_end}'
) ban_before
ON grade.role_id = ban_before.role_id
WHERE grade.role_level = {end_grade} 
    AND grade.ds >= '{ds_start}'
	AND grade.ds <= '{ds_end}'
    AND ban_before.role_id IS NULL
"""

# Query，获取主线挂某日全天样本，用于模型预测
QUERY_SQL_DATE = """
SELECT DISTINCT grade.role_id
FROM luoge_nsh_ods.ods_nsh_upgrade grade 
WHERE grade.role_level = {end_grade} 
    AND grade.ds >= '{ds_start}'
	AND grade.ds <= '{ds_end}'
"""

# Query字典
QUERY_DICT = {
    'pos': QUERY_SQL_POS,
    'neg': QUERY_SQL_NEG,
    'total': QUERY_SQL_DATE
}

################################################################################

# hbase拉取行为序列线程数
THREAD_NUM = 20
# hbase链接地址
HBASE_URL = 'http://***.***.***.***:8080/roleseq/grade?start_grade={sg}&end_grade={eg}&game=nsh&role_info={ids}'

################################################################################

# MySQL Config
MySQL_HOST_IP = '***.***.***.***'
MySQL_HOST_PORT = 3306
MySQL_HOST_USER = '***'
MySQL_HOST_PASSWORD = '***'
MySQL_TARGET_DB = '***'

# 预测模块获取实时id链接地址
FETCH_ID_URL = 'http://***.***.***.***:9290/common/shortterm/upgrade?start_ts={st}&end_ts={ed}&game=nsh'

# 预测模块插入预测结果至MySQL的query
INSERT_SQL = """
    INSERT INTO nsh_zhuxiangua(role_id, suspect_score, method, start_time, end_time) 
    VALUES ({id}, {score}, '{method}', '{st}', '{ed}')
"""

# time format
TIME_FORMAT = '%Y%m%d %H:%M:%S'

# 预测频率
MINUTE_DELTA = 15


