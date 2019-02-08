#!/usr/bin/env bash

# 工作目录
WORK_DIR=/home/zhoujialiang/nsh_zhuxian_sl_auto

# 定义参数
end_grade=$1                                                        # 结束等级
ds_pred=`date -d "0 days" +%Y%m%d`                                  # 当前日期
ts_pred_cur=`date -d "0 days" "+%Y%m%d %H:%M:%S"`                   # 当前时间
ts_pred_start=`date -d "$ts_pred_cur 45 minutes ago" +%Y%m%d#%H:%M` # 十五分钟前


# 定位到最近的周五（周五及之前用旧的模型，周六开始用新的模型）
last_friday=`date -d "friday -1 weeks" +%Y%m%d`
# 通过周五定位到当前滑窗模型的数据样本开始日期
ds_start=`date -d "$last_friday -30 days" +%Y%m%d`

# 计算日期差
stamp_end=`date -d "$last_friday -2 days" +%s`
stamp_start=`date -d "20181212" +%s`
stamp_diff=`expr $stamp_end - $stamp_start`
day_diff=`expr $stamp_diff / 86400`

# baseline
echo /usr/bin/python3 $WORK_DIR/mlp_predict.py --end_grade $end_grade --ds_pred $ds_pred --ts_pred_start ${ts_pred_start}:00 --ds_start 20181212 --ds_num 28 --method mlp_41_baseline;
/usr/bin/python3 $WORK_DIR/mlp_predict.py --end_grade $end_grade --ds_pred $ds_pred --ts_pred_start ${ts_pred_start}:00 --ds_start 20181212 --ds_num 28 --method mlp_41_baseline;

# incre
echo /usr/bin/python3 $WORK_DIR/mlp_predict.py --end_grade $end_grade --ds_pred $ds_pred --ts_pred_start ${ts_pred_start}:00 --ds_start 20181212 --ds_num $day_diff --method mlp_41_incre;
/usr/bin/python3 $WORK_DIR/mlp_predict.py --end_grade $end_grade --ds_pred $ds_pred --ts_pred_start ${ts_pred_start}:00 --ds_start 20181212 --ds_num $day_diff --method mlp_41_incre;

# window
echo /usr/bin/python3 $WORK_DIR/mlp_predict.py --end_grade $end_grade --ds_pred $ds_pred --ts_pred_start ${ts_pred_start}:00 --ds_start $ds_start --ds_num 28 --method mlp_41_window;
/usr/bin/python3 $WORK_DIR/mlp_predict.py --end_grade $end_grade --ds_pred $ds_pred --ts_pred_start ${ts_pred_start}:00 --ds_start $ds_start --ds_num 28 --method mlp_41_window
