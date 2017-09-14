'''
调用application_score包的主函数
1.解析Java调用传入的json原始数据，即用户申请现金贷时提供的申请信息数据
2.计算变量，104个
3.调用模型，返回违约概率 ,申请评分
'''
import datetime
import json
import time
from application_score.api import get_params, base_api, vars_calculation
from application_score.get_basevar import get_basevars
import pandas as pd
from application_score.model import load_model, model_data, predict_score, score, get_model_path


def application_main():
    # 1.解析Java传入的数据
    data_id, data_path = get_params()
    base_data = base_api(data_path[0])
    # print(base_data)

    # 2.计算变量
    if len(data_path) >= 2:
        var = vars_calculation(base_data, get_basevars, pd.to_datetime(data_path[1]))
    else:
        curDate = datetime.date.today().strftime('%Y-%m-%d')
        var = vars_calculation(base_data, get_basevars, pd.to_datetime(curDate))

    var.index = var.application_uuid
    var.drop('application_uuid', axis=1, inplace=True)
    var = var.astype('float')
    # var.to_csv(r'C:\Users\liyin\Desktop\svn_beifeng\申请评分卡模型_0807\dist\base_0914_new.csv',index=False)
    # 3.加载模型
    model_path = get_model_path('model_2017-07-27.txt')
    # print('model_path:%s' % model_path)
    bst = load_model(model_path)
    p_value = predict_score(model_data(var), bst)
    pre_score = score(p_value)

    re = pd.DataFrame({'runID': data_id,
                       'application_ID': var.index,
                       'P_value': p_value,
                       'application_score': pre_score,
                       'runtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                       }, index=None).to_json(orient='records', force_ascii=False)

    return re


'''
测试用语句
'''
#
# if __name__ == '__main__':
#     data_id = 'lyk_test'
#     data_path = [r'C:\Users\Administrator\Desktop\ccx_tn\testdata\base_json.json']
#
#     print(application_main())
#
#     # 生成批量的数据用于测试批量的结果
#     dd = pd.read_csv(r'C:\Users\Administrator\Desktop\tn_0713\base_rawdata_0717.csv', encoding='gbk')
#     d_json = dd.to_json(orient='records', force_ascii=False, date_format='iso')
#
#     fp = open('../exdata/base_json_0726.json', 'w+')
#     json.dump(d_json, fp, ensure_ascii=False)
#     fp.close()
#
#     data_id = 'lyk_test_piliang'
#     data_path = [r'C:\Users\Administrator\Desktop\ccx_application_score\exdata\base_json_0726.json', '2017-06-11']
#     # var.to_csv(r'C:\Users\Administrator\Desktop\tn_0713\base_var_0727.csv', index=False)
#     var = pd.read_csv(r'C:\Users\Administrator\Desktop\tn_0713\base_var_0727.csv')
#     # re.to_csv(r'../base_score_0727.csv',index=False)
#     # re.application_score.plot(kind='kde',xlim=(500,900))
#
#     # fp = open('../exdata/base_score_0727.json', 'w+')
#     # json.dump(re, fp, ensure_ascii=False)
#     # fp.close()

if __name__ == '__main__':
    re = application_main()
    print(re)
