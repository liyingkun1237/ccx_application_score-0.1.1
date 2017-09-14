# """
# Created on Tue Jul  4 10:12:50 2017
#
# @author: cxh
#
# 此脚本为计算base相关变量的函数，创建人chuxiaohui@ccx.cn
#
# 原始脚本整理至：E:\hive-connect\cxh\tncodepack.py
# """
import os
import numpy as np
import pandas as pd
from application_score.log import tn_log
import re
import jieba
from datetime import datetime

# 读取省市地区对应表
# 保证读取目录的存在性
os.chdir(os.path.split(os.path.realpath(__file__))[0])
addr_dic = pd.read_csv(r'../exdata/prov_city_county_dic.txt', sep='\t',
                       encoding='gbk')

'''
df:含有全部base所需字段的dataframe
present_time：计算时间，外部调用时确定
'''


@tn_log('get_basevars')
def get_basevars(df, present_time, detail=False):
    basevar = pd.DataFrame()  # 空list，存放vars
    # basevar['id'] = df['application_uuid']
    basevar['application_uuid'] = df['application_uuid']  # 0711 lyk 修改
    # var1/2/3/4
    basevar['apply_dts'] = (
        (pd.to_datetime(df.audit_time) - pd.to_datetime(df.apply_time)).map(
            lambda x: x / np.timedelta64(1, 's'))).values
    basevar['is_night_apply'] = (
        (pd.to_datetime(df['audit_time'])).map(lambda x: x.hour).replace(list(range(6)), '1').replace(
            list(range(6, 24)),
            '0')).values
    basevar['is_workday_apply'] = (
        (pd.to_datetime(df['audit_time'])).map(lambda x: datetime.weekday(x)).replace([list(range(5))], '1').replace(
            list(range(5, 7)), '0')).values
    basevar['is_worktime_apply'] = (pd.to_datetime(df['audit_time'])).map(lambda x: x.hour).replace(list(range(9, 19)),
                                                                                                    '1').values
    basevar.ix[~basevar.is_worktime_apply.isin(list(range(9, 19))), 'is_worktime_apply'] = '0'

    # var6/7/8
    basevar['apply_amount'] = df['apply_amount'].values
    basevar['apply_month'] = df['apply_month'].values
    basevar['register_diffdays'] = (present_time - pd.to_datetime(df['register_time'])).map(
        lambda x: x / np.timedelta64(1, 'D')).astype(int).values

    # var9 推广渠道来源
    tg_location_list1 = ['xjbka', 'yingyongbao', 'wtnsu', 'dwcm', 'rongyitui', 'jieba']
    tg_location_list2 = ['ryt', 'vivo', 'laijieqian', 'xianshangdaikuan', 'tqb', 'anxinj', '51gjj', 'xiaoxinyong',
                         'xianjindai']
    tg_location_list3 = ['yuyongjinrong', 'yijiexianjindai', 'nalijie', 'jiandandaikuan', 'xiaomi', 'dagejietiao',
                         'haoxiangdai', 'anzhi', 'xdzx',
                         'huchilvxing', '_360', 'diyidaikuan', 'xinyxz', 'jieqianguanjia', 'tdk', 'jieqianyong']

    def get_tglocation(x):
        if pd.notnull(x):
            y = (re.match(r'\S+[a-z]|[^a-z]+[0-9]', 'sdzj2')).group()
        else:
            y = np.nan
        return y

    df['tg_location1'] = df['tg_location'].apply(get_tglocation)
    basevar['tg_location'] = df['tg_location1'].replace('sdzj', 1).replace('xygj', 2).replace(tg_location_list1,
                                                                                              3).replace(
        tg_location_list2, 4).replace(tg_location_list3, 5).values
    basevar.ix[~basevar['tg_location'].isin([1, 2, 3, 4, 5]), 'tg_location'] = 5  # cxh

    # var 10/11/15/18/20/21/22/23/26
    basevar['login_origin'] = df['login_origin'].values
    basevar['is_reapply'] = df['is_reapply'].values
    basevar['address_length'] = df['address_length'].values
    basevar['mobile_company'] = df['mobile_company'].values
    basevar.ix[~basevar['mobile_company'].isin([1, 2, 3]), 'mobile_company'] = 4

    basevar['mobile_segment'] = df['mobile'].astype(str).str[:2].replace('13', 1).replace('14', 2).replace('15',
                                                                                                           3).replace(
        '17', 4).replace('18', 5).values  # cxh
    basevar['net_age'] = df['net_age'].replace('未知', np.nan).values
    basevar['age'] = df['idno'].map(lambda x: present_time.year - int(x[6:10])).values
    basevar['gender'] = df['idno'].map(lambda x: int(x[16]) % 2).values
    basevar['regaud_diffdays'] = (
        (pd.to_datetime(df.audit_time) - pd.to_datetime(df.register_time)) / np.timedelta64(1, 's')).values

    # var13/14/16/17/24/25/28/29
    def get_prov(x):
        if pd.notnull(x):
            m = re.match(r'\D+(省|自治区|特别行政区)', x)
            if m:
                y = re.split('省|自治区|特别行政区', m.group(0))[0]
            else:
                mm = re.match(r'\D+市', x)
                if mm:
                    y = re.split('市', mm.group(0))[0]
                else:
                    y = x
        else:
            y = np.nan
        return y

    df['cid_prov'] = df['id_card_address'].apply(get_prov)
    basevar['cid_prov_lvl'] = df['cid_prov'].replace(['上海', '北京', '天津', '广东', '澳门', '香港'], 1).replace(
        ['江苏', '山东', '福建', '浙江', '安徽'], 2).replace(['湖北', '湖南', '河南', '河北', '江西', '山西', '陕西'], 3).replace(
        ['广西', '广西壮族', '海南', '重庆', '云南', '四川', '贵州'], 4).replace(
        ['黑龙江', '吉林', '辽宁', '内蒙古', '甘肃', '新疆', '新疆维吾尔', '青海', '宁夏', '宁夏回族', '西藏'], 5).values
    basevar.ix[~basevar['cid_prov_lvl'].isin([1, 2, 3, 4, 5]), 'cid_prov_lvl'] = 6
    basevar['addr_prov_lvl'] = df['user_province'].apply(get_prov).replace(['上海', '北京', '天津', '广东', '澳门', '香港'],
                                                                           1).replace(['江苏', '山东', '福建', '浙江', '安徽'],
                                                                                      2).replace(
        ['湖北', '湖南', '河南', '河北', '江西', '山西', '陕西'], 3).replace(['广西', '广西壮族', '海南', '重庆', '云南', '四川', '贵州'], 4).replace(
        ['黑龙江', '吉林', '辽宁', '内蒙古', '甘肃', '新疆', '新疆维吾尔', '青海', '宁夏', '宁夏回族', '西藏'], 5).values
    basevar.ix[~basevar['addr_prov_lvl'].isin([1, 2, 3, 4, 5]), 'addr_prov_lvl'] = 6
    basevar['mobile_prov_lvl'] = df['mobile_province'].replace(['上海', '北京', '天津', '广东', '澳门', '香港'], 1).replace(
        ['江苏', '山东', '福建', '浙江', '安徽'], 2).replace(['湖北', '湖南', '河南', '河北', '江西', '山西', '陕西'], 3).replace(
        ['广西', '广西壮族', '海南', '重庆', '云南', '四川', '贵州'], 4).replace(
        ['黑龙江', '吉林', '辽宁', '内蒙古', '甘肃', '新疆', '新疆维吾尔', '青海', '宁夏', '宁夏回族', '西藏'], 5).values
    basevar.ix[~basevar['mobile_prov_lvl'].isin([1, 2, 3, 4, 5]), 'mobile_prov_lvl'] = 6

    def get_city(x):
        if pd.notnull(x):
            m = re.match(r'\D+(省|自治区)\D*?市', x)
            if m:
                yy = re.split('省|自治区', m.group(0))[1]
                m1 = re.match(r'\D+地区\D+市', x)
                if m1:
                    y = re.split('地区', m1.group())[1]
                else:
                    y = yy
            else:
                mm = re.match(r'\D*?市', x)
                if mm:
                    y = mm.group(0)
                else:
                    mmm = re.match(r'\D+地区\D+县', x)
                    if mmm:
                        y = re.split('地区', mmm.group())[1]
                    else:
                        y = np.nan
        else:
            y = np.nan
        return (y)

    df['cid_city'] = df['id_card_address'].apply(get_city)

    # addr_dic = pd.read_csv(r'.\exdata\prov_city_county_dic.csv', encoding='gbk')

    def county_match_city(x):
        if pd.notnull(x[1]):
            m = re.match(r'\D+市', x[1])
            if m:
                y = x[1]
            else:
                try:
                    y = addr_dic.ix[(addr_dic['county'] == x[1]) & (addr_dic['prov'] == x[0]), 'city'].tolist()[0]
                except:
                    y = 'unknown'
        else:
            y = np.nan
        return (y)

    df['cid_city1'] = df[['cid_prov', 'cid_city']].apply(county_match_city, axis=1)

    def city_level(x):
        if pd.notnull(x):
            if x in ['北京市', '上海市', '广州市', '深圳市', '天津市']:
                y = '一线'
            elif x in ['杭州市', '南京市', '济南市', '重庆市', '青岛市', '大连市', '宁波市', '厦门市']:
                y = '二线发达'
            elif x in ['成都市', '武汉市', '哈尔滨市', '沈阳市', '西安市', '长春市', '长沙市', '福州市', '郑州市',
                       '石家庄市', '苏州市', '佛山市', '东莞市', '无锡市', '烟台市', '太原市']:
                y = '二线中等发达'
            elif x in ['合肥市', '南昌市', '南宁市', '昆明市', '温州市', '淄博市', '唐山市']:
                y = '二线发展较弱'
            elif x == 'unknown':
                y = np.nan
            else:
                y = '三线以下城市'
        else:
            y = np.nan
        return (y)

    basevar['cid_city_lvl'] = df['cid_city1'].apply(city_level).replace('一线', 1).replace('二线发达', 2).replace('二线中等发达',
                                                                                                            3).replace(
        '二线发展较弱', 4).replace('三线以下城市', 5).values
    basevar.ix[~basevar['cid_city_lvl'].isin([1, 2, 3, 4, 5]), 'cid_city_lvl'] = 6
    basevar['addr_city_lvl'] = df['user_city'].apply(city_level).replace('一线', 1).replace('二线发达', 2).replace('二线中等发达',
                                                                                                             3).replace(
        '二线发展较弱', 4).replace('三线以下城市', 5).values
    basevar.ix[~basevar['addr_city_lvl'].isin([1, 2, 3, 4, 5]), 'addr_city_lvl'] = 6

    def get_bank(x):
        m = jieba.lcut(x)
        if len(m) > 0:
            y = m[0][:2]
            if len(m) > 1:
                if m[1] != ' ':
                    z = m[1]
                else:
                    z = m[2]
            else:
                z = np.nan
        else:
            y = np.nan
        return y, z

    df['bank_prov'], df['bank_city'] = get_bank(list(df['bank_address'].values)[0])
    df['bank_prov'] = df['bank_prov'].replace('湖', '湖南').replace('请', np.nan).replace('内蒙', '内蒙古').replace('黑龙', '黑龙江')
    basevar['bank_prov_lvl'] = df['bank_prov'].replace(['上海', '北京', '天津', '广东', '澳门', '香港'], 1).replace(
        ['江苏', '山东', '福建', '浙江', '安徽'], 2).replace(['湖北', '湖南', '河南', '河北', '江西', '山西', '陕西'], 3).replace(
        ['广西', '广西壮族', '海南', '重庆', '云南', '四川', '贵州'], 4).replace(
        ['黑龙江', '吉林', '辽宁', '内蒙古', '甘肃', '新疆', '新疆维吾尔', '青海', '宁夏', '宁夏回族', '西藏'], 5).values
    basevar.ix[~basevar['bank_prov_lvl'].isin([1, 2, 3, 4, 5]), 'bank_prov_lvl'] = 6

    basevar['bank_city_lvl'] = df['bank_city'].apply(city_level).replace('一线', 1).replace('二线发达', 2).replace('二线中等发达',
                                                                                                             3).replace(
        '二线发展较弱', 4).replace('三线以下城市', 5).values
    basevar.ix[~basevar['bank_city_lvl'].isin([1, 2, 3, 4, 5]), 'bank_city_lvl'] = 6
    basevar['mobile_city_lvl'] = (df['mobile_city'] + '市').apply(city_level).replace('一线', 1).replace('二线发达',
                                                                                                      2).replace(
        '二线中等发达', 3).replace('二线发展较弱', 4).replace('三线以下城市', 5).values
    basevar.ix[~basevar['mobile_city_lvl'].isin([1, 2, 3, 4, 5]), 'mobile_city_lvl'] = 6

    # var30/31/33/34
    basevar['job_type'] = df['job'].replace(['公司受雇员工', '私企公司职工'], 1).replace(['国企事业单位职工', '公务员', '农林牧副渔人员', '军人'],
                                                                             2).replace(['自雇创业人员', '自主创业人员'],
                                                                                        3).replace(
        ['自由职业者', '家庭主妇', '失业人员', '退休人员'], 4).values
    basevar.ix[~basevar.job_type.isin([1, 2, 3, 4]), 'job_type'] = 1
    basevar['rela1_lvl'] = df['relation1'].replace('父亲', 1).replace('母亲', 2).replace('配偶', 3).replace('子女', 4).values
    basevar.ix[~basevar['rela1_lvl'].isin([1, 2, 3, 4]), 'rela1_lvl'] = 5
    basevar['rela2_lvl'] = df['relation2'].replace('其他亲属', 1).replace(['兄弟', '姐妹'], 2).replace('配偶', 3).replace(
        ['母亲', '父亲'], 4).values
    basevar.ix[~basevar['rela2_lvl'].isin([1, 2, 3, 4]), 'rela2_lvl'] = 5
    basevar['rela3_lvl'] = df['relation3'].replace('朋友', 1).replace('同事', 2).replace(['其他', '其他亲属'], 3).values
    basevar.ix[~basevar['rela3_lvl'].isin([1, 2, 3]), 'rela3_lvl'] = 4

    # var35/../39/41
    basevar['cidcity_bankcity'] = (df['cid_city'] == df['bank_city']).replace(True, '1').replace(False, '0').values
    basevar['addrcity_bankcity'] = (df['user_city'] == df['bank_city']).replace(True, '1').replace(False, '0').values
    basevar['mobile_cardmobile'] = (df['mobile'] == df['card_mobile']).replace(True, '1').replace(False, '0').values
    basevar['addrcity_mobilecity'] = (df['user_city'] == df['mobile_city'] + '市').replace(True, '1').replace(False,
                                                                                                             '0').values
    basevar['addrcity_compcity'] = (df['user_city'] == df['company_city']).replace(True, '1').replace(False, '0').values
    basevar['mobilecity_compcity'] = (df['mobile_city'] + '市' == df['company_city']).replace(True, '1').replace(False,
                                                                                                                '0').values

    dummyvar = ['tg_location', 'login_origin', 'mobile_company', 'mobile_segment', 'cid_prov_lvl', 'addr_prov_lvl',
                'mobile_prov_lvl', 'cid_city_lvl', 'addr_city_lvl', 'bank_prov_lvl', 'bank_city_lvl', 'mobile_city_lvl',
                'job_type', 'rela1_lvl', 'rela2_lvl', 'rela3_lvl']

    basevar1 = basevar.copy()  # cxh

    dummycnt = [4, 2, 3, 4, 5, 5, 5, 5, 5, 5, 5, 5, 3, 4, 4, 3]

    for i in np.arange(16):
        for j in np.arange(dummycnt[i] + 1):
            if list(basevar[dummyvar[i]].values)[0] == j:
                basevar[dummyvar[i] + '_' + str(j + 1)] = 1
            else:
                basevar[dummyvar[i] + '_' + str(j + 1)] = 0
        del basevar[dummyvar[i]]

    basevar.index = [0]

    if detail:
        return basevar, basevar1  # cxh
    else:
        return basevar
