#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = "Jialiang Zhou"
__copyright__ = "Copyright 2019, The *** Project"
__version__ = "1.0.0"
__email__ = "***"
__phone__ = "***"
__description__ = "Data模块: 用于从Hive获取数据ID"
__usage__ = "供调用"
import os
import impala.dbapi
import json

hive_ip = '***'
hive_port = 36003


def connect_hive(hive_ip, hive_port, sql):
    # 验证
    os.system('kinit -kt /***/***/***.keytab ***@***-***-***')
    # 执行
    conn = impala.dbapi.connect(host=hive_ip, port=hive_port, auth_mechanism='GSSAPI',
                                kerberos_service_name='hive', database="default")
    cursor = conn.cursor()
    cursor.execute("show databases")
    # print(cursor.fetchall())

    cursor.execute(sql)
    # print(cursor.description)  # prints the result set's schema
    results = cursor.fetchall()


    return results

# 若本地不存在，则从HIVE拉取，并保存至本地
def get_ids(sql, ids_path):
    results = connect_hive(hive_ip=hive_ip, hive_port=hive_port, sql=sql)
    ids = [str(item[0]) for item in results]
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

    ids = get_ids(sql, 'tmp_hive')