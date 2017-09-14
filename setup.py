from distutils.core import setup

setup(
    name='ccx_application_score',
    version='0.1.2',
    packages=['application_score'],
    url='2017-09-14',
    license='ccx',
    author='lyk;cxh',
    author_email='liyingkun@ccx.cn',
    description='中诚信征信申请评分卡模型',
    package_data={'': ['*.py', 'exdata/prov_city_county_dic.txt', 'exdata/model_2017-07-27.txt']},
    data_files=[('', ['applymain.py', 'setup.py'])]
    #             ('exdata', ['exdata/prov_city_county_dic.txt',
    #                         'exdata/model_2017-07-27.txt'
    #                         ])]
)

# 20170914修改备注
# 优化了base_var的计算代码，调整了exdata为package文件
