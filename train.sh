#!/usr/bin/env bash
# Usage_1: bash train.sh

# 工作目录
WORK_DIR=/home/zhoujialiang/nsh_zhuxian_sl_auto

# 定义参数
ds_start=`date -d "wednesday -5 weeks" +%Y%m%d` # 距当日最近的第五个周三，滑窗模型数据开始日期
stamp_end=`date -d "-2 days" +%s`
stamp_start=`date -d "20181212" +%s`
stamp_diff=`expr $stamp_end - $stamp_start`
day_diff=`expr $stamp_diff / 86400`             # 增量模型的数据日期跨度

# 增量迭代模型
echo /usr/bin/python3 $WORK_DIR/MLPModel.py --ds_start 20181212 --ds_num $day_diff &&
/usr/bin/python3 $WORK_DIR/MLPModel.py --ds_start 20181212 --ds_num $day_diff &&

# 滑窗迭代模型
echo /usr/bin/python3 $WORK_DIR/MLPModel.py --ds_start $ds_start --ds_num 28 &&
/usr/bin/python3 $WORK_DIR/MLPModel.py --ds_start $ds_start --ds_num 28
