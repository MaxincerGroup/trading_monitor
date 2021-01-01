"""
todo list 12.24
0. 获得真实post-trade数据 - 校验算法
1. 线程分开：先后考虑！防止一个错了全烂掉： 读取；标准化；运算；监控
分成三个class/三个文件：线程/进程； post-trade，read_raw, fmt 一整个线程一直跑（公司电脑）
做一个global的database远程相互调用数据
7. 配置阿里云； 等万得能用再用
3. 添加新功能： ID->source+type; 券商买券还券等区分标识； 直接从券商处下载？ read_raw时间判定
4. 初步封装，相似函数抽象化，所有变量“较短地”写在input里； 给添加新变量留出“裕量”！
2. flask
6. patch场外
5. 资金 - exposure比例，相对exposure； 策略相关...
"""
import pandas as pd
import pymongo
from WindPy import w
from trader_v1 import Trader
import codecs
import threading
from openpyxl import load_workbook
from xlrd import open_workbook
import datetime
import time
import functools
import warnings
import schedule

# global functions and objects (名字取好，唯一
# global functions and objects (名字取好，唯一
# 引用/生成全局变量，不用global声明; class 外调用/修改: ClassName.VarName
# 修改全局变量，需要使用global声明，但列表、字典等如果只是修改其中元素的值，可以直接使用，不需要global声明

# 全局变量存到数据库里， "global"数据库独立于其它所有项目，元数据
client_remote_main = pymongo.MongoClient(port=27017, host='192.168.22.41',
                                         username='admin', password='123456')
col_global_var = client_remote_main['global_var']['exposure_monitoring']
dt_test_day = None  # datetime.datetime(2020, 12, 30, 10,0,0)
is_trading_day = True   # 交易日人判断


# 最好放到某个Utils里
def ini_time_records():
    if dt_test_day:
        datetime_today = dt_test_day
    else:
        datetime_today = datetime.datetime.today()
    str_date = datetime_today.strftime('%Y%m%d')
    list_dict_time = list(col_global_var.find({'DataDate': str_date}))
    end_clearing = datetime.datetime.strptime(f"{str_date} 08:30:00", "%Y%m%d %H:%M:%S")  # 今早清算结束
    start_clearing = datetime.datetime.strptime(f"{str_date} 21:30:00", "%Y%m%d %H:%M:%S")  # 今晚清算开始
    is_trading_time = (start_clearing > datetime_today > end_clearing)
    raw_time, fmt_time, position_time = (None, '17:48:45', None)
    dict_time = {'IsTradeDay': is_trading_day, 'IsTradeTime': is_trading_time,
                 'RawUpdateTime': raw_time, 'FmtUpdateTime': fmt_time, 'PositionUpdateTime': position_time}
    if len(list_dict_time) == 0:
        dict_time.update({'DataDate': str_date})
        col_global_var.insert_one(dict_time)
    elif len(list_dict_time) == 1:
        col_global_var.update_one({'DataDate': str_date}, {'$set': dict_time})
    else:
        raise ValueError('Too many variables at date %s' % str_date)
    return [datetime_today, str_date, is_trading_day, is_trading_time]


def get_global_var(str_date, var_name):
    list_dict_time = list(col_global_var.find({'DataDate': str_date}))
    if len(list_dict_time) == 1:
        var_value = list_dict_time[0][var_name]
    else:
        raise ValueError('Global variable is not initialized or excessive')
    return var_value


def runtime_threading(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        def f():
            self.lock.acquire()
            func(self, *args, **kwargs)
            self.lock.release()
        if func.__name__ == 'update_fmtdata':
            schedule.every().day.at('22:10').do(f)
        if func.__name__ == 'update_all_rawdata':
            schedule.every().day.at('22:00').do(f)
        while True:
            if self.is_trading_day:
                if self.is_trading_time:  # 清算只跑一次; 只跑一次测试
                    if func.__name__ == 'update_fmtdata':
                        while get_global_var(self.str_day, 'RawUpdateTime') is None:  # 等待updateraw开始1s
                            time.sleep(1)
                        print('34, ', get_global_var(self.str_day, 'RawUpdateTime'))
                    if func.__name__ == 'update_position':
                        while get_global_var(self.str_day, 'FmtUpdateTime') is None:  # 等待fmt开始1s
                            time.sleep(1)
                        print('37, ', get_global_var(self.str_day, 'FmtUpdateTime'))
                    if func.__name__ == 'exposure_analysis':
                        while get_global_var(self.str_day, 'PositionUpdateTime') is None:  # 等待position开始1s
                            time.sleep(1)
                        print('40, ', get_global_var(self.str_day, 'PositionUpdateTime'))
                    self.lock.acquire()  # 只有上面三个变量可以大家都调用, 其余公共变量锁住
                    func(self, *args, **kwargs)   # 测试时注释掉
                    self.lock.release()
                    print('Function: ', func.__name__, 'finished')
                    time.sleep(60)
                    if func.__name__ == 'update_fmtdata':
                        # record_update_raw_time = "13:00:00"
                        pass
                else:
                    print(datetime.datetime.today().strftime("%d  %H:%M:%S"))
                    schedule.run_pending()
                    func(self, *args, **kwargs)
                    print('Function: ', func.__name__, 'finished, go to sleep')
                    time.sleep(60*60*6)     # 7200 睡2小时
            else:
                time.sleep(60*60*6)
                raise ValueError('今天不是交易日')     # 睡6小时
    return wrapper


# todo 放到某个 utils class里
def get_sectype_from_code(windcode):
    # todo adapt the cases!
    list_split_wcode = windcode.split('.')
    secid = list_split_wcode[0]
    exchange = list_split_wcode[1]
    if exchange in ['SH', 'SSE'] and len(secid) == 6:
        if secid in ['511990', '511830', '511880', '511850', '511660', '511810', '511690']:
            return 'CE'
        elif secid in ['204001']:
            return 'CE'
        elif secid[:3] in ['600', '601', '603', '688']:
            return 'CS'
        elif secid in ['510500', '000905', '512500']:
            return 'ETF'
        else:
            return 'IrrelevantItem'

    elif exchange in ['SZ', 'SZSE'] and len(secid) == 6:
        if secid[:3] in ['000', '001', '002', '003', '004', '300', '301', '302', '303', '304', '305', '306', '307',
                         '308', '309']:
            return 'CS'
        elif secid[:3] in ['115', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129']:
            return '可转债'
        elif secid[:3] in ['131']:
            return 'CE'
        elif secid in ['159001', '159005', '159003']:
            return 'CE'
        else:
            return 'IrrelevantItem'
    elif exchange in ['CFE', 'CFFEX']:
        return 'Index Future'

    elif exchange == 'ITN':
        sectype = secid.split('_')[0]
        return sectype

    else:
        raise ValueError(f'{windcode} has unknown exchange or digit number is not 6.')


class ReadRaw:     # 包含post
    def __init__(self):
        # todo 从全局变量里获得 - 改为从数据库里获得;改动完就存回去； 三个时间区分先后顺序/为了好寻找之后再讨论
        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.record_update_raw_time = None

        self.db_trddata = client_remote_main['trade_data']
        self.db_posttrddata = client_remote_main['post_trade_data']
        self.db_basicinfo = client_remote_main['basic_info']
        self.col_acctinfo = self.db_basicinfo['acctinfo']

        self.path_basic_info = 'data/basic_info.xlsx'
        self.upload_basic_info()
        self.event = threading.Event()
        self.lock = threading.Lock()
        self.running = True
        return

    def upload_basic_info(self):
        df = pd.read_excel(self.path_basic_info, index_col=False, sheet_name=None)

        for sheet_name in df.keys():
            list_records = []
            df[sheet_name] = df[sheet_name].where(df[sheet_name].notnull(), None)
            for i, row in df[sheet_name].iterrows():
                rec = dict(row)
                rec.update({'DataDate': self.str_day})
                list_records.append(rec)
            self.db_basicinfo[sheet_name].delete_many({'DataDate': self.str_day})
            self.db_basicinfo[sheet_name].insert_many(list_records)
        return

    def read_rawdata_from_trdclient(self, fpath, str_c_h_secloan_mark, data_source_type, accttype,
                                    acctidbybroker):
        """
        从客户端下载数据，并进行初步清洗。为字符串格式。
        tdx倒出的txt文件有“五粮液错误”，使用xls格式的可解决

        已更新券商处理格式：
            华泰: hexin, txt, cash, margin, fund, holding
            国君: 富易, csv
            海通: ehtc, xlsx, cash, fund, holding
            申宏: alphabee, txt
            建投: alphabee, txt
            中信: tdx, txt, vip, cash, fund, holding,
            民生: tdx, txt
            华福: tdx, txt

        :param acctidbybroker: 用于pb类文件对账户编号的过滤。
        :param fpath:
        :param accttype: c: cash, m: margin, f: future
        :param str_c_h_secloan_mark: ['fund', 'holding', 'secloan']
        :param data_source_type:

        :return: list: 由dict rec组成的list
        """
        # todo : 注释改进
        list_ret = []
        if str_c_h_secloan_mark == 'fund':
            dict_rec_fund = {}
            if data_source_type in ['huat_hx', 'hait_hx', 'zhes_hx', 'tf_hx', 'db_hx', 'wk_hx'] and accttype == 'c':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[0:6]
                    for dataline in list_datalines:
                        list_data = dataline.strip().split(b'\t')
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_fund[list_recdata[0].strip()] = list_recdata[1].strip()

            elif data_source_type in ['yh_hx'] and accttype in ['c']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[5].decode('gbk').split()
                    list_values = list_datalines[6].decode('gbk').split()
                    dict_rec_fund.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['yh_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_fund = df_read.to_dict('records')[0]

            elif data_source_type in ['huat_hx', 'hait_hx', 'wk_hx'] and accttype == 'm':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[5:14]
                    for dataline in list_datalines:
                        if dataline.strip():
                            list_data = dataline.strip().split(b'\t')
                        else:
                            continue
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split(':')
                            if len(list_recdata) != 2:
                                list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_fund[list_recdata[0].strip()] = \
                                (lambda x: x if x.strip() in ['人民币'] else list_recdata[1].strip())(list_recdata[1])

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(5)
                list_values = ws.row_values(6)
                dict_rec_fund.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                df_read = pd.read_excel(fpath, skiprows=1, nrows=1)
                dict_rec_fund = df_read.to_dict('records')[0]

            elif data_source_type in ['hait_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_fund = df_read.to_dict('records')[0]

            elif data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                # todo 存在五 粮 液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    dataline = list_datalines[0][8:]
                    list_recdata = dataline.strip().decode('gbk').split()
                    for recdata in list_recdata:
                        list_recdata = recdata.split(':')
                        dict_rec_fund.update({list_recdata[0]: list_recdata[1]})

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c',
                                                                                                              'm']:
                # 已改为xls版本，避免'五粮液错误'
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    list_values = list_datalines[1].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    dict_rec_fund.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    list_values = list_datalines[1].decode('gbk').split()
                    dict_rec_fund.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split(',')
                    list_values = list_datalines[1].decode('gbk').split(',')
                    dict_rec_fund.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['patch']:
                pass

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                # print(fpath)
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund_wealthcats)

            elif data_source_type in ['db_wealthcats']:
                # todo weathcats账户和basic_info里对不上
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund_wealthcats)

            elif data_source_type in ['ax_jzpb']:
                # todo 账户编号不稳定，求源
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户编号'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund_wealthcats)

            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'hait_xtpb']:   # 有改动
                # todo 更改路径中的日期？没看到日期YYYYMMDD,校验新加的
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账号'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund)
            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['fund_account'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund)    # 有改动
            elif data_source_type in ['gs_htpb']:    # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()

                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账户'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund)
            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()

                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['单元序号'] == acctidbybroker:
                                dict_rec_fund.update(dict_fund)
            else:
                print(data_source_type)
                raise ValueError('Field data_source_type not exist in basic info!')
            if dict_rec_fund:  # we do not upload {}, todo 新加的
                list_ret.append(dict_rec_fund)

        elif str_c_h_secloan_mark == 'holding':
            if data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                # todo 存在五粮液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_holding = None
                    for index, dataline in enumerate(list_datalines):
                        if '证券代码' in dataline.decode('gbk'):
                            start_index_holding = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_holding].strip().split()]
                    list_keys_2b_dropped = ['折算汇率', '备注', '历史成交', '资讯']
                    for key_2b_dropped in list_keys_2b_dropped:
                        if key_2b_dropped in list_keys:
                            list_keys.remove(key_2b_dropped)
                    i_list_keys_length = len(list_keys)

                    for dataline in list_datalines[start_index_holding + 1:]:
                        list_data = dataline.strip().split()
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c',
                                                                                                              'm']:
                # 避免五粮液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_list_data = [
                        dataline.decode('gbk').replace('=', '').replace('"', '').split('\t')
                        for dataline in list_datalines
                    ]
                    start_index_holding = None
                    for index, list_data in enumerate(list_list_data):
                        if '证券代码' in list_data:
                            start_index_holding = index
                    list_keys = list_list_data[start_index_holding]
                    i_list_keys_length = len(list_keys)
                    for list_values in list_list_data[start_index_holding + 1:]:
                        if '没有' in list_values[0]:
                            print(f'{acctidbybroker}: {list_values[0]}')
                        else:
                            if len(list_values) == i_list_keys_length:
                                dict_rec_holding = dict(zip(list_keys, list_values))
                                list_ret.append(dict_rec_holding)
                            else:
                                print(f'{acctidbybroker}_{data_source_type}_{list_values} not added into database')

            elif data_source_type in ['huat_hx', 'yh_hx', 'wk_hx', 'hait_hx',
                                      'zhes_hx', 'db_hx', 'tf_hx'] and accttype in ['c', 'm']:
                # 注： 证券名称中 有的有空格, 核新派以制表符分隔
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_holding = None
                    for index, dataline in enumerate(list_datalines):
                        if '证券代码' in dataline.decode('gbk'):
                            start_index_holding = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_holding].strip().split()]
                    list_keys_2b_dropped = ['折算汇率', '备注']
                    for key_2b_dropped in list_keys_2b_dropped:
                        if key_2b_dropped in list_keys:
                            list_keys.remove(key_2b_dropped)
                    i_list_keys_length = len(list_keys)

                    for dataline in list_datalines[start_index_holding + 1:]:
                        list_data = dataline.strip().split(b'\t')
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_datagrp', 'yh_datagrp']:
                df_read = pd.read_excel(
                    fpath,
                    skiprows=3,
                    dtype={'股东代码': str},
                    converters={'代码': lambda x: str(x).zfill(6), '证券代码': lambda x: str(x).zfill(6)}
                )
                list_dicts_rec_holding = df_read.to_dict('records')
                list_ret = list_dicts_rec_holding

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(8)
                for i in range(9, ws.nrows):
                    list_values = ws.row_values(i)
                    if '' in list_values:
                        continue
                    str_values = ','.join(list_values)
                    if '合计' in str_values:
                        continue
                    dict_rec_holding = dict(zip(list_keys, list_values))
                    if accttype == 'm':
                        if '证券代码' in dict_rec_holding:
                            secid = dict_rec_holding['证券代码']
                            if secid[0] in ['0', '1', '3']:
                                dict_rec_holding['交易市场'] = '深A'
                            else:
                                dict_rec_holding['交易市场'] = '沪A'
                    list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                wb_ehtc = load_workbook(fpath)
                ws = wb_ehtc.active
                i_target_row = 10
                for row in ws.rows:
                    for cell in row:
                        if cell.value == '持仓':
                            i_target_row = cell.row
                df_holding = pd.read_excel(fpath, skiprows=i_target_row)
                list_dicts_rec_holding = df_holding.to_dict('records')
                list_ret = list_dicts_rec_holding

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    for dataline in list_datalines[1:]:
                        list_values = dataline.decode('gbk').split()
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom'] and accttype in ['c', 'm']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[3].decode('gbk').split(',')
                    for dataline in list_datalines[4:]:
                        list_values = dataline.decode('gbk').split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            list_ret.append(dict_rec_holding)

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['SymbolFull'].split('.')[1] == 'SZ':
                                dict_rec_holding['交易市场'] = '深A'
                            elif dict_rec_holding['SymbolFull'].split('.')[1] == 'SH':
                                dict_rec_holding['交易市场'] = '沪A'
                            else:
                                raise ValueError('Unknown exchange mark.')
                            if dict_rec_holding['账户'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['ax_jzpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['账户编号'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'hait_xtpb']:   # 有改动
                # todo 更改文件中的路径
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账号'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['fund_account'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)
            elif data_source_type in ['gs_htpb']:    # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)
            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['单元序号'] == acctidbybroker:
                                list_ret.append(dict_rec_holding)

        elif str_c_h_secloan_mark == 'secloan':
            # postdata处理raw用，交易时不读
            if data_source_type in ['zhaos_tdx'] and accttype in ['m']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_secloan = None
                    for index, dataline in enumerate(list_datalines):
                        str_dataline = dataline.decode('gbk')
                        if '证券代码' in str_dataline:
                            start_index_secloan = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_secloan].strip().split()]
                    i_list_keys_length = len(list_keys)
                    for dataline in list_datalines[start_index_secloan + 1:]:
                        list_data = dataline.strip().split()
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_secloan = dict(zip(list_keys, list_values))
                            # todo 自定义： 根据证券代码推测交易市场
                            secid = dict_rec_secloan['证券代码']
                            if secid[0] in ['0', '1', '3']:
                                dict_rec_secloan['交易市场'] = '深A'
                            else:
                                dict_rec_secloan['交易市场'] = '沪A'
                            list_ret.append(dict_rec_secloan)
            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'hait_xtpb'] and accttype in ['m']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_secloan = dict(zip(list_keys, list_values))
                            if dict_rec_secloan['资金账号'] == acctidbybroker:
                                list_ret.append(dict_rec_secloan)
            elif data_source_type in ['huat_matic_tsi'] and accttype in ['m']:  # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_secloan = dict(zip(list_keys, list_values))
                            if dict_rec_secloan['fund_account'] == acctidbybroker:
                                list_ret.append(dict_rec_secloan)
            elif data_source_type in ['gtja_pluto'] and accttype in ['m']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_secloan = dict(zip(list_keys, list_values))
                            if dict_rec_secloan['单元序号'] == acctidbybroker:
                                list_ret.append(dict_rec_secloan)
        elif str_c_h_secloan_mark == 'order':
            # 先做这几个有secloan的（不然order没意义）:
            if data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'hait_xtpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['资金账号'] == acctidbybroker:
                                list_ret.append(dict_rec_order)
            elif data_source_type in ['huat_matic_tsi'] and accttype:  # 有改动
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['fund_account'] == acctidbybroker:
                                list_ret.append(dict_rec_order)
            elif data_source_type in ['gtja_pluto'] and accttype:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['单元序号'] == acctidbybroker:
                                list_ret.append(dict_rec_order)
        else:
            raise ValueError('Wrong str_c_h_secloan_mark input!')
        return list_ret

    @runtime_threading
    def update_all_rawdata(self):
        """
        1. 出于数据处理留痕及增强robust考虑，将原始数据按照原格式上传到mongoDB中备份
        2. 定义DataFilePath = ['fpath_fund_data'(source), 'fpath_holding_data'(source), 'fpath_trdrec_data(source)',]
        3. acctinfo数据库中DataFilePath存在文件路径即触发文件数据的上传。
        4. 添加：融券未平仓合约数据的上传
        """
        self.record_update_raw_time = datetime.datetime.today().strftime('%H:%M:%S')
        # UpdateTime 不用过于精确，只是方便format时查找（只更新最新版）
        if self.is_trading_time:  # update post-trade
            col_manually_rawdata_fund = self.db_trddata['trade_rawdata_fund']
            col_manually_rawdata_holding = self.db_trddata['trade_rawdata_holding']
            col_manually_rawdata_order = self.db_trddata['trade_rawdata_order']
            col_manually_rawdata_secloan = {}
            list_to_upload = ['fund', 'holding', 'order']
        else:   # update trd
            col_manually_rawdata_fund = self.db_posttrddata['post_trade_rawdata_fund']
            col_manually_rawdata_holding = self.db_posttrddata['post_trade_rawdata_holding']
            col_manually_rawdata_order = self.db_posttrddata['post_trade_rawdata_order']
            col_manually_rawdata_secloan = self.db_posttrddata['post_trade_rawdata_secloan']
            list_to_upload = ['fund', 'holding', 'order', 'secloan']

        for _ in self.col_acctinfo.find({'DataDate': self.str_day, 'RptMark': 1}):
            datafilepath = _['DataFilePath']
            if datafilepath:
                # todo 算法上可以再稍微改进‘加速’一下，比如同一个fpath的不同 acctid一起读（一次遍历多个账户，一个文件至多读一次）
                list_fpath_data = _['DataFilePath'][1:-1].split(',')
                acctidbymxz = _['AcctIDByMXZ']
                acctidbybroker = _['AcctIDByBroker']
                downloaddatafilter = _['DownloadDataFilter']
                data_source_type = _['DataSourceType']
                accttype = _['AcctType']
                # print(acctidbybroker)
                for ch in list_to_upload:
                    if ch == 'fund':
                        fpath_relative = list_fpath_data[0]
                        col_manually_rawdata = col_manually_rawdata_fund
                    elif ch == 'holding':
                        fpath_relative = list_fpath_data[1]
                        col_manually_rawdata = col_manually_rawdata_holding
                    elif ch == 'order':
                        if len(list_fpath_data) > 2:  # 如果有sec，order必须空置 '; ; ;'形式
                            fpath_relative = list_fpath_data[2]
                            if fpath_relative:
                                col_manually_rawdata = col_manually_rawdata_order
                            else:
                                continue
                        else:
                            continue
                    elif ch == 'secloan':
                        # print(len(list_fpath_data))
                        if len(list_fpath_data) > 3:  # 如果有sec，order必须空置 '; ; ;'形式
                            fpath_relative = list_fpath_data[3]
                            if fpath_relative:
                                col_manually_rawdata = col_manually_rawdata_secloan
                            else:
                                continue
                        else:
                            continue
                    else:
                        raise ValueError('Value input not exist in fund and holding.')

                    col_manually_rawdata.delete_many({'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                    # fpath_absolute = os.path.join(self.dirpath_data_from_trdclient, fpath_relative)
                    try:
                        if downloaddatafilter:      # gtjy_pluto只有交易单元，没有账户名
                            acctidbybroker = downloaddatafilter

                        list_dicts_rec = self.read_rawdata_from_trdclient(
                            fpath_relative, ch, data_source_type, accttype, acctidbybroker
                        )
                        # there are some paths that I do not have access
                        for dict_rec in list_dicts_rec:
                            # if data_source_type == 'zx_wealthcats':
                            #     print(_, fpath_relative)
                            dict_rec['DataDate'] = self.str_day
                            dict_rec['UpdateTime'] = self.record_update_raw_time
                            dict_rec['AcctIDByMXZ'] = acctidbymxz
                            dict_rec['AcctType'] = accttype
                            dict_rec['DataSourceType'] = data_source_type
                        if list_dicts_rec:
                            col_manually_rawdata.insert_many(list_dicts_rec)
                    except FileNotFoundError:
                        print(f'No type {data_source_type} of file or directory: {fpath_relative}')

        # 更新全局变量
        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {'RawUpdateTime': self.record_update_raw_time}})
        print('Update raw data finished.')

    @runtime_threading
    def update_trddata_f(self):
        cursor_find = list(self.col_acctinfo.find({'DataDate': self.str_day, 'AcctType': 'f', 'RptMark': 1}))
        for _ in cursor_find:
            list_future_data_fund = []
            list_future_data_holding = []
            list_future_data_trdrec = []
            prdcode = _['PrdCode']
            acctidbymxz = _['AcctIDByMXZ']
            acctidbyowj = _['AcctIDByOuWangJiang4FTrd']
            trader = Trader(acctidbyowj)
            dict_res_fund = trader.query_capital()
            if dict_res_fund:
                dict_fund_to_be_update = dict_res_fund
                dict_fund_to_be_update['DataDate'] = self.str_day
                dict_fund_to_be_update['AcctIDByMXZ'] = acctidbymxz
                dict_fund_to_be_update['AcctIDByOWJ'] = acctidbyowj
                dict_fund_to_be_update['PrdCode'] = prdcode
                list_future_data_fund.append(dict_fund_to_be_update)
                self.db_trddata['trade_future_api_fund'].delete_many(
                    {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz}
                )
                if list_future_data_fund:
                    self.db_trddata['trade_future_api_fund'].insert_many(list_future_data_fund)

            list_list_res_holding = trader.query_holding()
            list_keys_holding = [
                'exchange', 'instrument_id', 'direction', 'hedge', 'position', 'position_td', 'open_volume',
                'close_volume', 'unknown1', 'unknown2', 'unknown3'
            ]
            if len(list_list_res_holding):
                list_dicts_holding_to_be_update = list_list_res_holding
                for list_holding_to_be_update in list_dicts_holding_to_be_update:
                    dict_holding_to_be_update = dict(zip(list_keys_holding, list_holding_to_be_update))
                    dict_holding_to_be_update['DataDate'] = self.str_day
                    dict_holding_to_be_update['AcctIDByMXZ'] = acctidbymxz
                    dict_holding_to_be_update['AcctIDByOWJ'] = acctidbyowj
                    dict_holding_to_be_update['PrdCode'] = prdcode
                    list_future_data_holding.append(dict_holding_to_be_update)

                self.db_trddata['trade_future_api_holding'].delete_many({'DataDate': self.str_day,
                                                                   'AcctIDByMXZ': acctidbymxz})
                if list_future_data_holding:
                    self.db_trddata['trade_future_api_holding'].insert_many(list_future_data_holding)

            list_list_res_trdrecs = trader.query_trdrecs()
            if len(list_list_res_trdrecs):
                list_keys_trdrecs = ['instrument_id', 'direction', 'offset', 'volume', 'price', 'time', 'trader']
                for list_res_trdrecs in list_list_res_trdrecs:
                    dict_trdrec = dict(zip(list_keys_trdrecs, list_res_trdrecs))
                    dict_trdrec['DataDate'] = self.str_day
                    dict_trdrec['AcctIDByMXZ'] = acctidbymxz
                    dict_trdrec['AcctIDByOWJ'] = acctidbyowj
                    dict_trdrec['PrdCode'] = prdcode
                    list_future_data_trdrec.append(dict_trdrec)
                self.db_trddata['trade_future_api_order'].delete_many(
                    {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz}
                )
                if list_future_data_trdrec:
                    self.db_trddata['trade_future_api_order'].insert_many(list_future_data_trdrec)

    def run(self):
        thread_raw = threading.Thread(target=self.update_all_rawdata)
        thread_future = threading.Thread(target=self.update_trddata_f)
        thread_raw.start()
        thread_future.start()


class FmtData:       # 包含post
    def __init__(self):
        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.db_trddata = client_remote_main['trade_data']
        self.db_post_trddata = client_remote_main['post_trade_data']
        self.col_acctinfo = client_remote_main['basic_info']['acctinfo']
        self.record_fmt_time = None
        self.record_update_raw_time = None
        self.lock = threading.Lock()
        return

    def formulate_raw_data(self, acctidbymxz, accttype, str_f_h_o_s_mark, mongo_collection):

        list_dicts_fmtted = []

        if accttype in ['c', 'm', 'o']:
            # patchmark = dict_acctinfo['PatchMark']
            # todo 有的券商的secloan要补上 - PatchMark；有的则是场外交易；还得写Patch函数

            # ---------------  FUND 相关列表  ---------------------
            list_fields_af = ['可用', 'A股可用', '可用数', '现金资产', '可用金额', '资金可用金', '可用余额', 'T+1指令可用金额',
                              'enable_balance', 'fund_asset', '可用资金']
            # 新加：matic_tsi_RZRQ: fund_asset, gtja_pluto:可用资金
            list_fields_ttasset = ['总资产', '资产', '总 资 产', '实时总资产', '单元总资产', '资产总额', '账户总资产',
                                   '担保资产', 'asset_balance', 'assure_asset']
            # list_fields_cb = []     # 券商没义务提供，得从postdata里找
            # list_fields_mktvalue = []   # 券商没义务提供，得按long-short算

            # ---------------  Security 相关列表  ---------------------
            list_fields_secid = ['代码', '证券代码', 'stock_code']
            list_fields_symbol = ['证券名称', 'stock_name', '股票名称']
            list_fields_shareholder_acctid = ['股东帐户', '股东账号', '股东代码']
            list_fields_exchange = ['市场', '市场代码', '交易市场', '交易板块', '板块', '交易所', '交易所名称', '交易市场',
                                    'exchange_type']

            # 有优先级别的列表
            list_fields_longqty = [
                '股票余额', '拥股数量', '证券余额', '证券数量', '库存数量', '持仓数量', '参考持股', '持股数量', '当前持仓',
                '当前余额', '当前拥股', '实际数量', '实时余额', 'current_amount'
            ]

            # -------------  ORDER 相关列表  ---------------------
            # todo 新加了 ：hait_xtpb; huat_matic;gj_pluto无XQHQ仅方向;
            #  zxjt_xtpb, zhaos_xtpb只有deal无order
            list_fields_cumqty = ['成交数量', 'business_amount']
            list_fields_side_c = ['买卖标记', 'entrust_bs', '委托方向']
            list_fields_side_m = ['买卖标记', 'trade_name', '委托方向']  # entrust_bs在matic_m里有，但是不详细
            list_fields_orderqty = ['委托量', 'entrust_amount', '委托数量']  # XXX_deal 会给不了委托量，委托日期，委托时间，只有成交
            list_fields_tradedate = ['委托日期', 'init_date']  # matic_m无entrust_date
            list_fields_transtime = ['委托时间', 'entrust_time']
            list_fields_avgpx = ['成交均价', 'business_price', '成交价格']  # 以后算balance用， exposure不用
            dict_fmtted_side_name_c = {'buy': ['1', '买入'],  # 担保品=券； 限价去掉,含"...“即可
                                       'sell': ['2', '卖出']}
            dict_fmtted_side_name_m = {'buy': ['担保品买入', '1'],  # 担保品=券； 限价去掉,含"...“即可
                                       'sell': ['担保品卖出', '2'],
                                       'sell short': ['融券卖出'],  # 限价 limit-price
                                       'XQHQ': ['现券还券划拨', '34'],
                                       'MQHQ': ['买券还券划拨', '买券还券'],
                                       'cancel': ['撤单']}  # entrust_bs表方向时值为1，2
            list_date_format = ['%Y%m%d']
            list_time_format = ['%H%M%S', '%H:%M:%S']
            # -------------  SECURITY LOAN 相关列表  ---------------------
            # todo 加hait_xtpb; huat_matic参考其手册;
            #  pluto 合约类型，合约状态里的1和huat里的1指代一个吗？
            #  这块 有不少问题！！！目前只关注short暂不会出错
            list_fields_shortqty = ['未还合约数量', 'real_compact_amount', '未还负债数量']  # 未还合约数量一般是开仓数量
            # 合约和委托没有关系了，但是用contract还是compact(券商版）?
            list_fields_contractqty = ['合约开仓数量', 'business_amount', '成交数量']  # 国外sell short约为“融券卖出”
            list_fields_contracttype = ['合约类型', 'compact_type']  # 一定能分开 锁券与否
            list_fields_contractstatus = ['合约状态', 'compact_status']  # filled='完成'那不是委托？融资融券能用
            list_fields_opdate = ['合约开仓日期', 'open_date', '发生日期']  # FIX 合约: contract
            list_fields_sernum = []  # SerialNumber
            list_fields_compositesrc = []  # CompositeSource

            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                      '00': 'SZSE', '10': 'SSE',
                                      '0': 'SZSE', '1': 'SSE', '2': 'SZSE',
                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                      'SH': 'SSE', 'SZ': 'SZSE',
                                      '上交所A': 'SSE', '深交所A': 'SZSE',
                                      '上证所': 'SSE', '深交所': 'SZSE'}

            if str_f_h_o_s_mark == 'f':  # cash
                list_dicts_fund = list(mongo_collection.find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day,
                     'UpdateTime': {'$gte': self.record_update_raw_time}}
                ))  # 为啥之前find_one?
                # print(list_dicts_fund)
                if list_dicts_fund is None:
                    list_dicts_fund = []
                for dict_fund in list_dicts_fund:
                    avfund = None  # 'AvailableFund'
                    ttasset = None  # 'TotalAsset'

                    # 分两种情况： 1. cash acct: 至少要有cash 2. margin acct: 至少要有ttasset
                    if accttype in ['c']:
                        flag_check_new_name = True  # 用来弥补之前几个list的缺漏
                        for field_af in list_fields_af:
                            if field_af in dict_fund:
                                avfund = float(dict_fund[field_af])
                                # todo patchdata fund 处理 要Debt吗? - secloan 关联？
                                flag_check_new_name = False
                        if flag_check_new_name:
                            print(dict_fund)
                            raise KeyError('unknown available_fund name (cash)')

                    elif accttype == 'm':
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        if flag_check_new_name:
                            print(dict_fund)
                            raise KeyError('unknown total asset name')
                        flag_check_new_name = True
                        for field_avfund in list_fields_af:
                            if field_avfund in dict_fund:
                                avfund = float(dict_fund[field_avfund])
                                flag_check_new_name = False
                        if flag_check_new_name:
                            print(dict_fund)
                            raise KeyError('unknown available_fund name (margin)')

                        # flt_cash = flt_ttasset - stock_longamt - etf_longamt - ce_longamt

                    elif accttype == 'o':
                        # todo patch 里场外暂时放放
                        pass
                    else:
                        raise ValueError('Unknown accttype')

                    dict_fund_fmtted = {
                        'DataDate': self.str_day,
                        'UpdateTime': self.record_fmt_time,
                        'AcctIDByMXZ': acctidbymxz,
                        'CashBalance': None,
                        'AvailableFund': avfund,  # flt_approximate_na?
                        'TotalAsset': ttasset,
                        'TotalMarketValue': None  # 总股本*每股价值 = 证券市值, 之后补上
                    }
                    list_dicts_fmtted.append(dict_fund_fmtted)
            elif str_f_h_o_s_mark == 'h':  # holding
                # 2.整理holding
                # 2.1 rawdata(无融券合约账户)
                list_dicts_holding = list(mongo_collection.find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day,
                     'UpdateTime': {'$gte': self.record_update_raw_time}}
                ))

                for dict_holding in list_dicts_holding:  # 不必 list_dicts_holding.keys()
                    secid = None
                    secidsrc = None
                    symbol = None
                    longqty = 0
                    # shortqty = 0
                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_holding:
                            secid = str(dict_holding[field_secid])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_holding)
                        raise KeyError('unknown available_fund name')

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_holding:
                            shareholder_acctid = str(dict_holding[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_holding:
                            exchange = dict_holding[field_exchange]

                            secidsrc = dict_exchange2secidsrc[exchange]
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_holding)
                        raise KeyError('unknown security source name')

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_holding:
                            symbol = str(dict_holding[field_symbol])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_holding)
                        raise KeyError('unknown symbol name')

                    flag_check_new_name = True
                    for field_longqty in list_fields_longqty:
                        if field_longqty in dict_holding:
                            longqty = float(dict_holding[field_longqty])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_holding)
                        raise KeyError('unknown longqty name')

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    dict_holding_fmtted = {
                        'DataDate': self.str_day,
                        'UpdateTime': self.record_fmt_time,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': 0,
                        'LongAmt': None,
                        'ShortAmt': 0,
                        'NetAmt': None
                    }
                    list_dicts_fmtted.append(dict_holding_fmtted)

            elif str_f_h_o_s_mark == 'o':   # 3.order
                list_dicts_order = list(mongo_collection.find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day,
                     'UpdateTime': {'$gte': self.record_update_raw_time}}
                ))
                if accttype == 'c':
                    dict_fmtted_side_name = dict_fmtted_side_name_c
                    list_fields_side = list_fields_side_c
                else:
                    dict_fmtted_side_name = dict_fmtted_side_name_m
                    list_fields_side = list_fields_side_m

                for dict_order in list_dicts_order:
                    secid = None
                    secidsrc = None
                    symbol = None
                    cumqty = None
                    side = None
                    orderqty = None
                    tradedate = None
                    transtime = None
                    avgpx = None
                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_order:
                            secid = str(dict_order[field_secid])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_order)
                        raise KeyError('unknown secid name')

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_order:
                            shareholder_acctid = str(dict_order[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_order:
                            exchange = dict_order[field_exchange]
                            secidsrc = dict_exchange2secidsrc[exchange]
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_order)
                        raise KeyError('unknown exchange name')

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_order:
                            symbol = str(dict_order[field_symbol])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_order)
                        raise KeyError('unknown symbol name')

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    flag_check_new_name = True
                    for field_cumqty in list_fields_cumqty:
                        if field_cumqty in dict_order:
                            cumqty = dict_order[field_cumqty]
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_order)
                        raise KeyError('unknown cumqty name')

                    flag_check_new_name = True
                    for field_side in list_fields_side:
                        if field_side in dict_order:
                            side = dict_order[field_side]
                            for key in dict_fmtted_side_name.keys():
                                for side_name in dict_fmtted_side_name[key]:
                                    if side_name in side:  # ex: '担保品买入' in '限价担保品买入'
                                        side = key  # standardization
                                        flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_order)
                        raise KeyError('unknown side name')

                    flag_check_new_name = True
                    for field_orderqty in list_fields_orderqty:
                        if field_orderqty in dict_order:
                            orderqty = dict_order[field_orderqty]
                            flag_check_new_name = False
                    if flag_check_new_name:
                        if dict_order['DataSourceType'] in ['zxjt_xtpb', 'zhaos_xtpb']:  # 他们给不了委托量
                            pass
                        else:
                            print(dict_order)
                            raise KeyError('unknown orderqty name')

                    flag_check_new_name = True
                    for field_tradedate in list_fields_tradedate:
                        if field_tradedate in dict_order:
                            tradedate = dict_order[field_tradedate]
                            # 转化成统一时间格式
                            datetime_obj = None
                            for date_format in list_date_format:
                                try:
                                    datetime_obj = datetime.datetime.strptime(tradedate, date_format)
                                except ValueError:
                                    pass
                            if datetime_obj:
                                tradedate = datetime_obj.strftime('%Y%m%d')  # 统一成 str_day格式
                            else:
                                raise ValueError('Unrecognized trade date format')
                            flag_check_new_name = False

                    if flag_check_new_name:
                        if dict_order['DataSourceType'] in ['zxjt_xtpb', 'zhaos_xtpb']:  # 他们给不了委托时间
                            pass
                        else:
                            print(dict_order)
                            raise KeyError('unknown tradedate name')

                    flag_check_new_name = True
                    for field_transtime in list_fields_transtime:
                        if field_transtime in dict_order:
                            transtime = dict_order[field_transtime]
                            # 转化成统一时间格式
                            datetime_obj = None
                            for time_format in list_time_format:
                                try:
                                    datetime_obj = datetime.datetime.strptime(transtime, time_format)
                                except ValueError:
                                    pass
                            if datetime_obj:
                                transtime = datetime_obj.strftime('%H:%M:%S')
                            else:
                                raise ValueError('Unrecognized trade date format')
                            flag_check_new_name = False
                    if flag_check_new_name:
                        if dict_order['DataSourceType'] in ['zxjt_xtpb', 'zhaos_xtpb']:  # 他们给不了委托时间
                            pass
                        else:
                            print(dict_order)
                            raise KeyError('unknown transaction time name')

                    flag_check_new_name = True
                    for field_avgpx in list_fields_avgpx:
                        if field_avgpx in dict_order:
                            avgpx = dict_order[field_avgpx]
                            flag_check_new_name = False
                            if avgpx == 0:  # 撤单
                                cumqty = 0
                    if flag_check_new_name:
                        print(dict_order)
                        raise KeyError('unknown average price name')

                    dict_order_fmtted = {
                        'DataDate': self.str_day,
                        'UpdateTime': self.record_fmt_time,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SerialNumber': None,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'CumQty': cumqty,
                        'Side': side,
                        'OrdQty': orderqty,
                        'TradeDate': tradedate,
                        'TransactTime': transtime,
                        'AvgPx': avgpx
                    }

                    list_dicts_fmtted.append(dict_order_fmtted)
            elif str_f_h_o_s_mark == 's':
                list_dicts_secloan = list(mongo_collection.find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}
                ))

                for dict_secloan in list_dicts_secloan:
                    secid = None
                    secidsrc = None
                    symbol = None
                    # longqty = 0
                    shortqty = 0
                    contractstatus = None
                    contracttype = None
                    contractqty = None
                    opdate = None
                    sernum = None
                    compositesrc = None

                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_secloan:
                            secid = str(dict_secloan[field_secid])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field_secid name')

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_secloan:
                            shareholder_acctid = str(dict_secloan[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_secloan:
                            exchange = dict_secloan[field_exchange]
                            secidsrc = dict_exchange2secidsrc[exchange]
                            flag_check_new_name = False
                    if flag_check_new_name:
                        if dict_secloan['DataSourceType'] in ['gtja_pluto']:  # 不给交易所
                            # todo 加入security_id2src
                            secidsrc = 'SSE'  # 510500.SH
                        else:
                            print(dict_secloan)
                            raise KeyError('unknown field secidsrc name')

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_secloan:
                            symbol = str(dict_secloan[field_symbol])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field symbol name')

                    flag_check_new_name = True
                    for field_shortqty in list_fields_shortqty:
                        if field_shortqty in dict_secloan:
                            shortqty = float(dict_secloan[field_shortqty])
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field shortqty name')

                    flag_check_new_name = True
                    for field_contractqty in list_fields_contractqty:
                        if field_contractqty in dict_secloan:
                            contractqty = str(dict_secloan[field_contractqty])
                        flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field contractqty name')

                    flag_check_new_name = True
                    for field_sernum in list_fields_sernum:
                        if field_sernum in dict_secloan:
                            sernum = str(dict_secloan[field_sernum])
                            flag_check_new_name = False
                    if flag_check_new_name and list_fields_sernum:
                        print(dict_secloan)
                        raise KeyError('unknown field serum name')

                    flag_check_new_name = True
                    for field_contractstatus in list_fields_contractstatus:
                        if field_contractstatus in dict_secloan:
                            contractstatus = str(dict_secloan[field_contractstatus])
                            dict_contractstatus_fmt = {'部分归还': '部分归还', '未形成负债': None,
                                                       '0': '开仓未归还', '1': '部分归还', '5': None,
                                                       '2': '已归还/合约过期',
                                                       '未归还': '开仓未归还'}  # 有bug了...pluto vs matic
                            # todo: 其它名字’开仓未归还‘等得之后补上
                            contractstatus = dict_contractstatus_fmt[contractstatus]
                            # if contractstatus is None:
                            #     raise Exception('During Clearing, we can not have ambiguous status in the compact')
                            flag_check_new_name = False

                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field_contractstatus name')

                    flag_check_new_name = True
                    for field_contracttype in list_fields_contracttype:
                        if field_contracttype in dict_secloan:
                            contracttype = str(dict_secloan[field_contracttype])
                            dict_contracttype_fmt = {'融券': 'rq', '融资': 'rz',
                                                     '1': 'rq', '0': 'rz',
                                                     '2': '其它负债/？？？'}  # 一般没有融资, 其它负债（2）
                            # 遇到bug，pluto vs matic 2指代不一样的
                            # todo: 其它名字比如 私用融券（专项券池）得之后补上
                            contractstatus = dict_contracttype_fmt[contracttype]
                            flag_check_new_name = False
                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field_contracttype name')

                    flag_check_new_name = True
                    for field_opdate in list_fields_opdate:
                        if field_opdate in dict_secloan:
                            opdate = str(dict_secloan[field_opdate])
                            flag_check_new_name = False
                            datetime_obj = None
                            # 和order共用 date格式
                            for date_format in list_date_format:
                                try:
                                    datetime_obj = datetime.datetime.strptime(opdate, date_format)
                                except ValueError:
                                    pass
                            if datetime_obj:
                                opdate = datetime_obj.strftime('%Y%m%d')
                            else:
                                raise ValueError('Unrecognized trade date format')

                    if flag_check_new_name:
                        print(dict_secloan)
                        raise KeyError('unknown field opdate name')

                    flag_check_new_name = True
                    for field_compositesrc in list_fields_compositesrc:
                        if field_compositesrc in dict_secloan:
                            compositesrc = str(dict_secloan[field_compositesrc])
                            flag_check_new_name = False
                    if flag_check_new_name and list_fields_compositesrc:
                        print(dict_secloan)
                        raise KeyError('unknown field_compositesrc name')

                    # print(secidsrc)
                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    dict_secloan_fmtted = {
                        'DataDate': self.str_day,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'SerialNumber': sernum,
                        'OpenPositionDate': opdate,  # = tradeDate？loan是交易吗？感觉FIX里是
                        'ContractStatus': contractstatus,
                        'ContractType': contracttype,
                        'ContractQty': contractqty,
                        'CompositeSource': compositesrc,
                        'ShortQty': shortqty,
                        'ShortAmt': None
                    }
                    list_dicts_fmtted.append(dict_secloan_fmtted)
            else:
                raise ValueError('Unknown f_h_o_s_mark')
        elif accttype in ['f']:
            list_dicts_future_fund = list(mongo_collection.find(
                {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                # , 'UpdateTime': self.record_update_raw_time测试去掉
            )
            for dict_fund_future in list_dicts_future_fund:
                approximate_na = dict_fund_future['DYNAMICBALANCE']
                cash_balance = dict_fund_future['STATICBALANCE']
                acctidbymxz = dict_fund_future['AcctIDByMXZ']
                dict_future_fund_fmtted = {
                    'DataDate': self.str_day,
                    'UpdateTime': self.record_fmt_time,
                    'AcctIDByMXZ': acctidbymxz,
                    'CashBalance': cash_balance,
                    'AvailableFund': approximate_na,  # flt_approximate_na?
                    'TotalAsset': None,
                    'TotalMarketValue': None  # 总股本*每股价值 = 证券市值
                }
                list_dicts_fmtted.append(dict_future_fund_fmtted)
            # 期货holding直接放到 position里
        else:
            raise ValueError('Unknown account type in basic account info.')
        return list_dicts_fmtted

    @runtime_threading
    def update_fmtdata(self):

        # todo check下新加在list_fields里的
        self.record_fmt_time = datetime.datetime.today().strftime('%H:%M:%S')
        self.record_update_raw_time = get_global_var(self.str_day, 'RawUpdateTime')
        list_dicts_acctinfo = list(
            self.col_acctinfo.find({'DataDate': self.str_day, 'RptMark': 1}))  # {'_id': 0}隐藏

        for dict_acctinfo in list_dicts_acctinfo:
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            if self.is_trading_time:
                database = self.db_trddata
                if accttype == 'f':
                    dict_mark2raw_col = {'f': database['trade_future_api_fund']}
                    dict_mark2fmt_col = {'f': database['trade_fmtdata_fund']}
                else:
                    dict_mark2raw_col = {'f': database['trade_rawdata_fund'],
                                         'h': database['trade_rawdata_holding'],
                                         'o': database['trade_rawdata_order']}
                    dict_mark2fmt_col = {'f': database['trade_fmtdata_fund'],
                                         'h': database['trade_fmtdata_holding'],
                                         'o': database['trade_fmtdata_order']}
                for str_f_h_o_s_mark in dict_mark2raw_col.keys():
                    mongo_collection = dict_mark2raw_col[str_f_h_o_s_mark]
                    target_collection = dict_mark2fmt_col[str_f_h_o_s_mark]
                    list_dicts_fmtted = self.formulate_raw_data(acctidbymxz, accttype, str_f_h_o_s_mark, mongo_collection)
                    if list_dicts_fmtted:
                        target_collection.delete_many({'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day, 'UpdateTime': self.record_fmt_time})
                        target_collection.insert_many(list_dicts_fmtted)
            else:
                if accttype == 'f':
                    continue
                database = self.db_post_trddata
                dict_mark2raw_col = {'f': database['post_trade_rawdata_fund'],   # post
                                     'h': database['post_trade_rawdata_holding'],
                                     's': database['post_trade_rawdata_secloan']}  # sec改
                dict_mark2fmt_col = {'f': database['post_trade_fmtdata_fund'],
                                     'h': database['post_trade_fmtdata_holding'],
                                     's': database['post_trade_fmtdata_secloan']}
                for str_f_h_o_s_mark in dict_mark2raw_col.keys():
                    mongo_collection = dict_mark2raw_col[str_f_h_o_s_mark]
                    target_collection = dict_mark2fmt_col[str_f_h_o_s_mark]
                    list_dicts_fmtted = self.formulate_raw_data(acctidbymxz, accttype, str_f_h_o_s_mark, mongo_collection)
                    if list_dicts_fmtted:
                        target_collection.delete_many({'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day, 'UpdateTime': self.record_fmt_time})
                        target_collection.insert_many(list_dicts_fmtted)
        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {'RawUpdateTime': None}})
        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {'FmtUpdateTime': self.record_fmt_time}})
        return

    def run(self):
        fmt_threading = threading.Thread(target=self.update_fmtdata)
        fmt_threading.start()


class Position:
    def __init__(self):
        w.start()
        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.record_fmt_time = None
        self.record_position_time = None

        self.col_acctinfo = client_remote_main['basic_info']['acctinfo']
        self.db_trddata = client_remote_main['trade_data']
        self.db_posttrddata = client_remote_main['post_trade_data']
        self.dict_future2multiplier = {'IC': 200, 'IH': 300, 'IF': 300}

        self.event = threading.Event()
        self.lock = threading.Lock()
        self.running = True
        return

    def get_order_last_from_wind(self, list_secid_query):
        # we do query only for securities in our account, secid should be type of wind
        # w.wsq("600000.SH", "rt_last,rt_latest", func=DemoWSQCallback)
        if list_secid_query:
            docs = []
            dict_wcode2last = {}
            last_from_wind = w.wsq(list_secid_query, "rt_last")   # 实时快照现价
            if last_from_wind.ErrorCode == 0:
                dict_wcode2last = dict(zip(last_from_wind.Codes, last_from_wind.Data[0]))
                for key in dict_wcode2last:
                    dt = last_from_wind.Times[0]
                    doc = {'TransactTime': dt.strftime("%H:%M:%S"), 'DataDate': dt.strftime("%Y%m%d"),
                           'LastPx': dict_wcode2last[key], 'WindCode': key}
                    docs.append(doc)
            elif last_from_wind.ErrorCode == -40520010:
                pass
            else:
                raise Exception(last_from_wind.Data[0][0])  # Error Msg here
            if docs:
                self.db_trddata['wind_last'].insert_many(docs)
            return dict_wcode2last
        else:
            return {}

    @runtime_threading
    def update_position(self):

        self.record_fmt_time = get_global_var(self.str_day, 'FmtUpdateTime')
        if self.dt_day.weekday() == 0:   # 周一把周五当"昨日"
            yesterday = (self.dt_day - datetime.timedelta(days=3)).strftime("%Y%m%d")
        else:
            yesterday = (self.dt_day - datetime.timedelta(days=1)).strftime("%Y%m%d")
        # print(yesterday)
        list_dicts_position = []  # 取名改改...
        set_windcode_to_search = set()  # 防止重复
        dict_id2type = {}
        dict_pair2allcol = {}  # 为了只遍历一遍各个表格，不然特别慢！

        list_dicts_acctinfo = list(self.col_acctinfo.find({'DataDate': self.str_day, 'RptMark': 1}))
        for dict_acctinfo in list_dicts_acctinfo:
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            # print('acctidbymxz', acctidbymxz)
            accttype = dict_acctinfo['AcctType']
            dict_id2type.update({acctidbymxz: accttype})

        for col_name in ['trade_fmtdata_order', 'trade_fmtdata_holding', 'trade_future_api_holding']:
            list_to_add = list(self.db_trddata[col_name].find(
                {'DataDate': self.str_day, 'UpdateTime': {'$gte': self.record_fmt_time}}))
            for _ in list_to_add:
                if col_name != 'trade_future_api_holding':
                    pair = (_['AcctIDByMXZ'], _['SecurityID'], _['SecurityIDSource'],
                            _['SecurityType'], _['Symbol'])
                else:
                    pair = (_['AcctIDByMXZ'], _['instrument_id'], _['exchange'])
                # set_pair_secid = set_pair_secid | {pair}  # 并集
                all_doc = _.copy()
                try:
                    dict_pair2allcol[pair][col_name].append(all_doc)
                except KeyError:  # one key doesn't exist.
                    dict_pair2allcol.update({pair: {col_name: [all_doc]}})
        # post_col_name = ['fmtdata_holding', 'fmtdata_secloan']
        for col_name in ['post_trade_fmtdata_holding', 'post_trade_fmtdata_secloan']:
            list_to_add = list(self.db_posttrddata[col_name].find({'DataDate': yesterday}))
            for _ in list_to_add:
                if not ('SecurityType' in _):  # 老版post里无IDSource...
                    if not ('SecurityIDSource' in _):
                        _['SecurityIDSource'] = 'SZSE'  # 猜一猜...
                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[_['SecurityIDSource']]
                    try:  # todo 在Fmt里处理好这个没有sectype的问题
                        _['SecurityType'] = get_sectype_from_code(_['SecurityID'] + windcode_suffix)
                        if _['SecurityType'] == 'IrrelevantItem':
                            _['SecurityIDSource'] = 'SSE'
                            _['SecurityType'] = get_sectype_from_code(_['SecurityID'] + '.SH')
                    except ValueError:
                        _['SecurityIDSource'] = 'SSE'
                        _['SecurityType'] = get_sectype_from_code(_['SecurityID'] + '.SH')
                pair = (_['AcctIDByMXZ'], _['SecurityID'], _['SecurityIDSource'],
                        _['SecurityType'], _['Symbol'])
                # set_pair_secid = set_pair_secid | {pair}  # 并集
                all_doc = _.copy()
                col_name_ = col_name  # 'post_' + col_name
                try:
                    dict_pair2allcol[pair][col_name_].append(all_doc)
                except KeyError:  # one key doesn't exist.
                    dict_pair2allcol.update({pair: {col_name_: [all_doc]}})

        for pair in dict_pair2allcol:  # or pair in dict_pair2allcol.keys()
            acctidbymxz = pair[0]
            secid = pair[1]
            secidsrc = pair[2]
            sectype = None
            symbol = None
            try:    # RptMark != 1, 不读了
                accttype = dict_id2type[acctidbymxz]
            except KeyError:
                continue
            try:
                list_dicts_holding = dict_pair2allcol[pair]['trade_fmtdata_holding']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_holding = []
            try:
                list_dicts_post_holding = dict_pair2allcol[pair]['post_trade_fmtdata_holding']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_post_holding = []
            try:
                list_dicts_secloan = dict_pair2allcol[pair]['post_trade_fmtdata_secloan']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_secloan = []
            try:
                list_dicts_order = dict_pair2allcol[pair]['trade_fmtdata_order']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_order = []
            try:
                list_dicts_holding_future = dict_pair2allcol[pair]['trade_future_api_holding']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_holding_future = []

            if accttype in ['c', 'm', 'o'] and self.is_trading_time:
                if len(pair) == 5:
                    symbol = pair[3]
                    sectype = pair[4]
                windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                windcode = secid + windcode_suffix

                longqty = 0  # longqty可能准
                longqty_ref = 0
                shortqty = 0
                dict_holding_id = 'no reference'
                dict_secloan_id = 'no reference'
                dict_post_holding_id = 'no reference'

                if len(list_dicts_post_holding) == 1:
                    longqty = list_dicts_post_holding[0]['LongQty']
                    dict_post_holding_id = list_dicts_post_holding[0]['_id']
                elif len(list_dicts_post_holding) == 0:
                    pass
                else:
                    tmax = time.strptime('0:0:0', '%H:%M:%S')
                    post_holding_id = list_dicts_post_holding[0]['_id']
                    for d in list_dicts_post_holding:
                        t = time.strptime(d['UpdateTime'], '%H:%M:%S')
                        if tmax < t:
                            longqty = d['LongQty']
                            tmax = t
                            post_holding_id = d['_id']
                    print('The postholding has too many information', post_holding_id)

                if len(list_dicts_holding) == 1:
                    longqty_ref = list_dicts_holding[0]['LongQty']
                    dict_holding_id = list_dicts_holding[0]['_id']
                elif len(list_dicts_holding) == 0:
                    pass
                else:
                    tmax = time.strptime('0:0:0', '%H:%M:%S')
                    for d in list_dicts_holding:
                        t = time.strptime(d['UpdateTime'], '%H:%M:%S')
                        if tmax < t:
                            longqty_ref = d['LongQty']
                            dict_holding_id = d['_id']
                            tmax = t

                if len(list_dicts_secloan) > 0:
                    for d in list_dicts_secloan:
                        shortqty += d['ShortQty']  # 可能多个合约

                for dict_order in list_dicts_order:
                    # if self.str_day == dict_order['TradeDate']:
                    side = dict_order['Side']
                    cumqty = float(dict_order['CumQty'])  # todo 为啥是str?
                    if side == 'buy':
                        longqty += cumqty
                    if side == 'sell':
                        longqty -= cumqty
                    if side == 'sell short':
                        shortqty += cumqty
                    if side == 'XQHQ':
                        longqty -= cumqty
                        shortqty -= cumqty
                    if side == 'MQHQ':
                        shortqty -= cumqty
                    else:
                        continue

                if longqty < 0:  # 有的券商没有sell short说法, long就是net..
                    if abs(shortqty + longqty) > 0.01:  # 因为short仅仅来自postdata
                        warnings.warn("LongQty is Negative: short: %f, long: %f because "
                                      "postdata is not clean, id %s" % (shortqty, longqty, dict_secloan_id))
                        longqty = 0
                    # todo long小于0各种情况讨论。。c用户； m时有short， 无short

                if abs(longqty - longqty_ref) > 0.01:
                    warnings.warn("\n"
                                  "Please check fmtdata_holding: %s and the one in posttrade %s and order: %s \n"
                                  " The alogrithm to calculate longqty of account: %s is somehow wrong! \n"
                                  " longqty: %d; shortqty: %d; longqty_ref: %d"
                                  % (dict_holding_id, dict_post_holding_id, secid, acctidbymxz, longqty, shortqty, longqty_ref))
                    longqty = longqty_ref
                # todo 在做一个short_ref现在的票子...

                # 只监控有票子的
                if longqty != 0 or shortqty != 0:
                    set_windcode_to_search = set_windcode_to_search | {windcode}
                    dict_position = {
                        'DataDate': self.str_day,
                        'UpdateTime': None,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'LongAmt': None,
                        'ShortAmt': None,
                        'NetAmt': None,
                        'WindCode': windcode
                    }
                    list_dicts_position.append(dict_position)

            elif accttype in ['f'] and self.is_trading_time:
                # list_dicts_holding_future_exposure_draft = []
                future_longqty = 0
                future_shortqty = 0
                secid_first_part = secid[:-4]
                dict_future2spot_windcode = {'IC': '000905.SH', 'IH': '000016.SH', 'IF': '000300.SH'}
                windcode = dict_future2spot_windcode[secid_first_part]
                for dict_holding_future in list_dicts_holding_future:
                    qty = dict_holding_future['position']
                    direction = dict_holding_future['direction']

                    if direction == 'buy':
                        future_longqty = qty
                        # future_longamt = close * future_longqty * self.dict_future2multiplier[secid_first_part]
                    elif direction == 'sell':
                        future_shortqty = qty
                        # future_shortamt = close * future_shortqty * self.dict_future2multiplier[secid_first_part]
                    else:
                        raise ValueError('Unknown direction in future respond.')

                if future_longqty != 0 or future_shortqty != 0:
                    set_windcode_to_search = set_windcode_to_search | {windcode}
                    dict_position = {
                        'DataDate': self.str_day,
                        'UpdateTime': None,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': 'Index Future',  # todo 用函数获取类型
                        'Symbol': None,
                        'SecurityIDSource': secidsrc,
                        'LongQty': future_longqty,
                        'ShortQty': future_shortqty,
                        'LongAmt': None,
                        'ShortAmt': None,
                        'NetAmt': None,
                        'WindCode': windcode
                    }
                    list_dicts_position.append(dict_position)

        # 统一一次询问现价，节约时间，市价更加精确
        self.record_position_time = datetime.datetime.today().strftime("%H:%M:%S")
        # self.record_wind_query_time = (datetime.datetime.today() - datetime.timedelta(hours=1, seconds=10)).strftime("%H:%M:%S")
        dict_windcode2last = self.get_order_last_from_wind(list(set_windcode_to_search))
        # print('2230 get last finished')
        # print(dict_windcode2last)
        for dict_position in list_dicts_position:
            windcode = dict_position['WindCode']
            if dict_position['SecurityType'] == 'Index Future':
                secid_first_part = dict_position['SecurityID'][:-4]
                point = self.dict_future2multiplier[secid_first_part]
                dict_position['LongAmt'] = dict_position['LongQty'] * dict_windcode2last[windcode] * point
                dict_position['ShortAmt'] = dict_position['ShortQty'] * dict_windcode2last[windcode] * point
            else:
                dict_position['LongAmt'] = dict_position['LongQty'] * dict_windcode2last[windcode]
                dict_position['ShortAmt'] = dict_position['ShortQty'] * dict_windcode2last[windcode]
            dict_position['NetAmt'] = dict_position['LongAmt'] - dict_position['ShortAmt']
            dict_position['UpdateTime'] = self.record_position_time
            # print('2246', dict_position)
            # del dict_position['WindCode'] # 可删可不删

        # print(list_dicts_position)
        if list_dicts_position:
            self.db_trddata['trade_position'].delete_many(
                {'DataDate': self.str_day, 'UpdateTime': self.record_position_time})
            self.db_trddata['trade_position'].insert_many(list_dicts_position)

        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {'FmtUpdateTime': None}})
        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {'PositionUpdateTime': self.record_position_time}})
        print("Update Position finished")
        return

    def run(self):
        position_thread = threading.Thread(target=self.update_position)
        position_thread.start()


class Exposure:
    def __init__(self):
        """
        Incorporated inputs: (可以全局化...)
        1.MongoClient('host:port username@admin:pwd')
        2.path of basic_info
        3.database names and collection names : trddata, basicinfo
        4.date
        """

        # 时间判定：交易时间；清算时间；发呆时间讨论

        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.record_position_time = None

        self.db_trddata = client_remote_main['trade_data']
        self.col_acctinfo = client_remote_main['basic_info']['acctinfo']

        self.event = threading.Event()
        self.lock = threading.Lock()
        self.running = True

    # def update_close_from_wind(self):
    #     print(w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010000000000"))
    #     list_astock_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a001010100000000").Data[1]
    #     # list_bond_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a100000000000000").Data[1]
    #     list_futures_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010000000000").Data[1]
    #     # 期货sectorid CFFEX, SHFE, DCE, CZCE;能源期货  全部期货用不了了
    #     list_etf_codes = ['000300.SH', '000016.SH', '000905.SH']  # index ETF that we use
    #     list_bond_codes = []
    #     list_repo_codes = []
    #     list_mmf_codes = []   # 也是固定一下价值
    #     list_codes = list_astock_codes + list_bond_codes + list_futures_codes + list_etf_codes \
    #         + list_repo_codes + list_mmf_codes
    #     err, close_data_from_wind = w.wss(list_codes, "sec_name, close", f"tradeDate={self.str_day};priceAdj=U;cycle=D", usedf=True)
    #
    #     if err == 0:
    #         close_data_from_wind.rename(columns={'CLOSE': 'Close', 'SEC_NAME': 'Symbol'}, inplace=True)
    #         close_records = []
    #         for index, row in close_data_from_wind.iterrows():
    #             doc = dict(row)
    #             doc.update({'WindCode': index, 'DataDate': self.str_day})
    #             close_records.append(doc)
    #         self.db_trddata['wind_close'].delete_many({'DataDate': self.str_day})
    #         self.db_trddata['wind_close'].insert_many(close_records)
    #
    #         return close_data_from_wind['Close'].to_dict()
    #     else:
    #         print(err, close_data_from_wind)
    #         return {}

    @runtime_threading
    def exposure_analysis(self):
        # todo 按照产品汇总， 策略字段留出来， 相对敞口： amt/资金
        self.record_position_time = get_global_var(self.str_day, 'PositionUpdateTime')
        list_dicts_acctidbymxz = list(self.db_trddata['trade_position'].find(
            {'DataDate': self.str_day, 'UpdateTime': {'$gte': self.record_position_time}}))
        set_acctidbymxz = set()
        for _ in list_dicts_acctidbymxz:
            set_acctidbymxz = set_acctidbymxz | {_['AcctIDByMXZ']}
        list_acctidbymxz = list(set_acctidbymxz)
        dict_index = {'LongQty': 1, 'ShortQty': 1, 'LongAmt': 1, 'ShortAmt': 1, 'NetAmt': 1}
        exposure_df = pd.DataFrame(columns=list_acctidbymxz, index=dict_index)
        exposure_df = exposure_df.fillna(0)
        dict_index.update({'_id': 0})
        for acctidbymxz in list_acctidbymxz:
            list_dicts_position = list(self.db_trddata['trade_position'].find(
              {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day, 'UpdateTime': {'$gte': self.record_position_time}}, dict_index))
            # 加上 updatetime
            for dict_position in list_dicts_position:
                exposure_df[acctidbymxz] += list(dict_position.values())
                # dataFrame 更改会每行遍历非常慢！
                # print(exposure_df.loc[acctidbymxz, :])

        exposure_docs = []
        print(exposure_df)
        for index, row in exposure_df.T.iterrows():
            doc = dict(row)
            doc.update({'AcctIDByMXZ': index, 'DataDate': self.str_day, 'UpdateTime': self.record_position_time})
            exposure_docs.append(doc)
        if exposure_docs:
            self.db_trddata['trade_exposure'].delete_many({'DataDate': self.str_day, 'UpdateTime': self.record_position_time})
            self.db_trddata['trade_exposure'].insert_many(exposure_docs)
        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {'PositionUpdateTime': None}})
        print("Exposure Analysis Finished")
        return exposure_df

    def run(self):
        exposure_monitoring_thread = threading.Thread(target=self.exposure_analysis)
        exposure_monitoring_thread.start()


if __name__ == '__main__':
    # read_raw = ReadRaw()
    # fmt_data = FmtData()
    #
    # read_raw.run()
    # fmt_data.run()
    is_trading_time = ini_time_records()[3]
    if is_trading_time:
        position = Position()
        exposure = Exposure()
        position.run()
        exposure.run()
