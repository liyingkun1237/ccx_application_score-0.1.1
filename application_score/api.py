import json
import pandas as pd
import sys
from application_score.log import tn_log
import time
import numpy as np


# 1.获取Java传入的json数据
@tn_log('get_params')
def get_params():
    # print(len(sys.argv))
    # print('run pymodules:%s' %sys.argv[0])
    data_id = sys.argv[1]
    # print(sys.argv[1])
    data_path = [sys.argv[i] for i in range(2, len(sys.argv))]
    # print(data_path)
    return data_id, data_path


@tn_log('base_api')
def base_api(data_path):
    # 解析json,成为Dataframe
    data_path = data_path.replace('~', '"')
    # print(data_path)
    data = pd.DataFrame(json.loads(data_path))
    return data


# 0807修改，直接用于接收json字符串，而不是读写文件
# def base_api(data_path):
#    # 解析json,成为Dataframe
#    f = open(data_path, 'r')
#    data_json = json.load(f)
#    data = pd.DataFrame(json.loads(data_json))
#    return data


@tn_log('vars_calculation')
def vars_calculation(data, func, *args):
    vars = []
    times = []
    st = time.time()
    for i, row in data.iterrows():
        start = time.time()
        df = pd.DataFrame([row])
        vars.append(func(df, *args))
        times.append(time.time() - start)
        if (i + 1) % 100 == 0:
            print('已运行%s行,累计用时%0.2f s' % (i, time.time() - st))
    print('总计计算数据行数：%d,平均每人次用时为：%0.2fs' % (len(data), np.mean(times)))
    return pd.concat(vars)
