#!/usr/bin/env bash
# Usage_1(single day): bash data.sh 41 20181215 1
# Usage_2(ds range): bash data.sh 41 `date -d "-31 days" +%Y%m%d` 28

# 安装依赖
#apt-get update
#apt-get install -y libsasl2-dev cyrus-sasl2-heimdal-dbg python3-dev
#pip install -r requirements.txt

# 工作目录
WORK_DIR=/home/zhoujialiang/nsh_zhuxian_sl_auto

# 定义参数
grade=$1
ds_num=$2
ds_start=`date -d "-3 days" +%Y%m%d`

# 正样本id
echo /usr/bin/python3 $WORK_DIR/get_ids.py pos --end_grade $grade --ds_start $ds_start --ds_num $ds_num &&
/usr/bin/python3 $WORK_DIR/get_ids.py pos --end_grade $grade --ds_start $ds_start --ds_num $ds_num &&

# 全量样本id
echo /usr/bin/python3 $WORK_DIR/get_ids.py total --end_grade $grade --ds_start $ds_start --ds_num $ds_num &&
/usr/bin/python3 $WORK_DIR/get_ids.py total --end_grade $grade --ds_start $ds_start --ds_num $ds_num &&

# 拉取数据
echo /usr/bin/python3 $WORK_DIR/dataloader.py --end_grade $grade --ds_start $ds_start --ds_num $ds_num &&
/usr/bin/python3 $WORK_DIR/dataloader.py --end_grade $grade --ds_start $ds_start --ds_num $ds_num
