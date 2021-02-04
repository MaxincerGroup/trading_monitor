"""
todo list
1. flask +/prdcode: 为啥报错undefined，还会自动刷新...要么直接for循环定义
3. 策略相关...分开
4. log 打不开？
2. secloan字段精确化
"""
import pandas as pd
import pymongo
# from WindPy import w
from trader_v1 import Trader
import codecs
import threading
from openpyxl import load_workbook
from xlrd import open_workbook
import datetime
import time
import functools
# import warnings
import schedule
import logging
from logging.handlers import RotatingFileHandler
from stock_utils import ID2Source, get_sectype_from_code
# import orjson
# import redis

# shift ctrl + a 搜索所有功能
client_local_main = pymongo.MongoClient(port=27017, host='localhost',
                                        username='admin', password='Ms123456')
col_global_var = client_local_main['global_var']['exposure_monitoring']

# 交易日人判断
is_trading_day_manual = True   # Note1.判断交易日
is_trading_time_manual = True  # Note1. 选择上传False = post数据/True = 交易数据

# 手动上传/自动上传
update_postdata_manually = True  # Note1.手动/自动上传选择，目前基本用手动 -> copy lastest.py
broker_c_without_postdata = ['cj_xtpb', 'dh_xtqmt', 'gd_xtpb', 'gf_tyt', 'gl_xtpb', 'hait_ehfz_api', 'hait_xtpb', 'hc_tradex', 'hengt_xtpb', 'hf_xtpb', 'yh_apama']
# 'gf', 'zx', 'hengt', 'hf', 'zhes', 'cj', 'gs', 'ax', 'gl', 'gy', 'swhy', 'yh', 'zxjt' checked， 加减order算出持仓正确
broker_m_without_postdata = ['hait_ehfz_api']  # 'zhaos', 'swhy', 'zxjt', huat, hait, gtja 已校验, 'yh'已patch todo（待验证）
# schedule
schedule_time = '09:05:00'  # Note1.自动上传
schedule_interval = 5  # s

logger_expo = logging.getLogger()
logger_expo.setLevel(logging.DEBUG)
fh = RotatingFileHandler('data/log/exposure.log', mode='w', maxBytes=2*1024, backupCount=0)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - line:%(lineno)d - %(levelname)s: %(message)s'))
logger_expo.addHandler(fh)
# logging.BasicConfig 仅适用于一个文件，多个程序运行容易写混
# level = logging.debug < .info < warning < error < critical 每次重写, formatters %()里的都是logging自带变量


def ini_time_records(initialize=True):
    global is_trading_time_manual
    global is_trading_day_manual
    datetime_today = datetime.datetime.today()   # + datetime.timedelta(days=0, hours=6, minutes=3)  # 假装跨日/清算前
    str_date = datetime_today.strftime('%Y%m%d')
    list_dict_time = list(col_global_var.find({'DataDate': str_date}))
    # 人工调；如果无人值守newday就自动判定
    is_new_day = (len(list_dict_time) == 0)
    if is_new_day:
        end_clearing = datetime.datetime.strptime(f"{str_date} 091500", '%Y%m%d %H%M%S')
        start_clearing = datetime.datetime.strptime(f"{str_date} 223000", '%Y%m%d %H%M%S')
        is_trading_time = start_clearing > datetime_today > end_clearing
        is_trading_time_manual = is_trading_time
        is_trading_day = datetime_today.weekday() in range(5)
        is_trading_day_manual = is_trading_day
    else:
        is_trading_time = is_trading_time_manual
        is_trading_day = is_trading_day_manual

    dict_time = {'IsTradeDay': is_trading_day, 'IsTradeTime': is_trading_time,
                 'RawFinished': False, 'FmtFinished': False, 'PosFinished': False}
    if col_global_var.find_one({'DataDate': str_date}) is None:
        dict_time.update({'RawUpdateTime': None, 'FmtUpdateTime': None, 'PositionUpdateTime': None})
        col_global_var.update_one({'DataDate': str_date}, {'$set': dict_time}, upsert=True)
    if initialize:
        # is newday有延迟，用is_newday多线程时会导致上传多个 （upsert -> insert)
        col_global_var.update_one({'DataDate': str_date}, {'$set': dict_time}, upsert=True)
    return [datetime_today, str_date, is_trading_day, is_trading_time]


def run_process(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        def f():
            if func.__name__ == 'update_fmtdata':
                while not col_global_var.find_one({'DataDate': self.str_day})['RawFinished']:  # 等待updateraw开始1s
                    time.sleep(1)
            self.lock.acquire()
            func(self, *args, **kwargs)
            self.lock.release()
            print('Function: ', func.__name__, 'finished, go to sleep')
        schedule.every().day.at(schedule_time).do(f)
        while True:
            self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records(initialize=False)
            if self.is_trading_day:
                if self.is_trading_time:  # 清算只跑一次; 只跑一次测试
                    if func.__name__ == 'update_fmtdata':
                        while not col_global_var.find_one({'DataDate': self.str_day})['RawFinished']:
                            time.sleep(1)
                        logger_expo.info('RawUpdateTime: ' +
                                     col_global_var.find_one({'DataDate': self.str_day})['RawUpdateTime'])
                    if func.__name__ == 'update_position':
                        while not col_global_var.find_one({'DataDate': self.str_day})['FmtFinished']:
                            time.sleep(1)
                        logger_expo.info('FmtUpdateTime: ' +
                                     col_global_var.find_one({'DataDate': self.str_day})['FmtUpdateTime'])
                    if func.__name__ == 'exposure_analysis':
                        while not col_global_var.find_one({'DataDate': self.str_day})['PosFinished']:
                            time.sleep(1)
                        logger_expo.info('PositionUpdateTime: ' +
                                     col_global_var.find_one({'DataDate': self.str_day})['PositionUpdateTime'])
                    self.lock.acquire()  # 只有上面三个变量可以大家都调用, 其余公共变量锁住
                    func(self, *args, **kwargs)   # 测试时注释掉
                    self.lock.release()
                    print('Function: ', func.__name__, 'finished')
                    if func.__name__ == 'exposure_analysis':
                        # record_update_raw_time = "13:00:00"
                        pass
                    time.sleep(60)
                else:    # 手动post放在最后
                    if update_postdata_manually:
                        if func.__name__ == 'update_fmtdata':
                            while not col_global_var.find_one({'DataDate': self.str_day})['RawFinished']:
                                time.sleep(1)
                        func(self, *args, **kwargs)
                        print('Function: ', func.__name__, 'finished, go to sleep')
                        time.sleep(60)
                    else:
                        print(self.str_day)
                        time.sleep(60*60)

            else:
                raise ValueError('今天不是交易日')     # 睡6小时
    return wrapper


class ReadRaw:     # 包含post
    def __init__(self):
        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.record_update_raw_time = None
        self.finish_upload_flag = False

        self.db_trddata = client_local_main['trade_data']
        self.db_posttrddata = client_local_main['post_trade_data']
        self.db_basicinfo = client_local_main['basic_info']
        self.col_acctinfo = self.db_basicinfo['acctinfo']

        self.path_basic_info = 'data/basic_info.xlsx'
        self.path_patch = 'data/data_patch.xlsx'
        self.upload_basic_info()

        self.list_warn = []

        self.event = threading.Event()
        self.lock = threading.Lock()
        return

    def upload_basic_info(self):
        df = pd.read_excel(self.path_basic_info, index_col=False, sheet_name=None, dtype=str)

        for sheet_name in df.keys():
            list_records = []
            df[sheet_name] = df[sheet_name].where(df[sheet_name].notnull(), None)
            for i, row in df[sheet_name].iterrows():
                rec = dict(row)
                rec.update({'DataDate': self.str_day})
                list_records.append(rec)
            self.db_basicinfo[sheet_name].delete_many({'DataDate': self.str_day})
            self.db_basicinfo[sheet_name].insert_many(list_records)

        df2 = pd.read_excel(self.path_patch, index_col=False, sheet_name=None, dtype=str)
        list_records = []
        for sheet_name in df2.keys():
            df2[sheet_name] = df2[sheet_name].where(df2[sheet_name].notnull(), None)
            if len(df2[sheet_name].index) == 1:
                rec = dict(df2[sheet_name].iloc[0])
                rec.update({'DataDate': self.str_day, 'SheetName': sheet_name})
                list_records.append(rec)
                continue
            for i, row in df2[sheet_name].iterrows():
                rec = dict(row)
                rec.update({'DataDate': self.str_day, 'SheetName': sheet_name})
                list_records.append(rec)
        self.db_basicinfo['data_patch'].delete_many({'DataDate': self.str_day})
        if len(list_records) == 1:
            self.db_basicinfo['data_patch'].insert_one(list_records[0])
        else:
            self.db_basicinfo['data_patch'].insert_many(list_records)

        return

    def read_rawdata_from_trdclient(self, fpath, sheet_type, data_source_type, accttype, idinfo):
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

        :param idinfo: dict broker - acctidbymxz
        :param fpath:
        :param accttype: c: cash, m: margin, f: future
        :param sheet_type: ['fund', 'holding', 'order', 'secloan']
        :param data_source_type:

        :return: list: 由dict rec组成的list
        """
        # todo : 注释改进, 有空再精简一下， 古老版本得加上 idinfo部分

        list_ret = []
        if sheet_type == 'fund':
            dict_rec_fund = {}
            if data_source_type in ['huat_hx', 'hait_hx', 'zhes_hx', 'tf_hx', 'db_hx', 'wk_hx'] and accttype == 'c':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[0:6]
                    for dataline in list_datalines:
                        list_data = dataline.strip().split(b'\t')
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_fund[list_recdata[0].strip()] = list_recdata[1].strip()
                        if dict_rec_fund:
                            list_ret.append(dict_rec_fund)

            elif data_source_type in ['yh_hx'] and accttype in ['c']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[5].decode('gbk').split()
                    list_values = list_datalines[6].decode('gbk').split()
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['yh_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_fund = df_read.to_dict('records')[0]
                if dict_rec_fund:
                    list_ret.append(dict_rec_fund)

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
                        if dict_rec_fund:
                            list_ret.append(dict_rec_fund)

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(5)
                list_values = ws.row_values(6)
                list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                df_read = pd.read_excel(fpath, skiprows=1, nrows=1)
                dict_rec_fund = df_read.to_dict('records')[0]
                if dict_rec_fund:
                    list_ret.append(dict_rec_fund)

            elif data_source_type in ['hait_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_fund = df_read.to_dict('records')[0]
                if dict_rec_fund:
                    list_ret.append(dict_rec_fund)

            elif data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    dataline = list_datalines[0][8:]
                    list_recdata = dataline.strip().decode('gbk').split()
                    for recdata in list_recdata:
                        list_recdata = recdata.split(':')
                        list_ret.append({list_recdata[0]: list_recdata[1]})

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c',
                                                                                                              'm']:
                # 已改为xls版本，避免'五粮液错误'
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    list_values = list_datalines[1].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    list_values = list_datalines[1].decode('gbk').split()
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split(',')
                    list_values = list_datalines[1].decode('gbk').split(',')
                    list_ret.append(dict(zip(list_keys, list_values)))

            elif data_source_type in ['patch']:
                pass

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s'%fpath)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] in idinfo:
                                dict_fund_wealthcats['AcctIDByMXZ'] = idinfo[dict_fund_wealthcats['账户']]
                                list_ret.append(dict_fund_wealthcats)

            elif data_source_type in ['db_wealthcats']:
                # todo weathcats账户和basic_info里对不上
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] in idinfo:
                                dict_fund_wealthcats['AcctIDByMXZ'] = idinfo[dict_fund_wealthcats['账户']]
                                list_ret.append(dict_fund_wealthcats)

            elif data_source_type in ['ax_jzpb']:    # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with open(fpath, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['账户编号'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['账户编号']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb', 'swhy_xtpb',
                                      'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb']:   # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账号'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['资金账号']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['hait_ehfz_api']:   # 有改动
                for acctidbybroker in idinfo:
                    try:
                        fpath_ = fpath.replace('YYYYMMDD', self.str_day).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_fund = dict(zip(list_keys, list_values))
                                    dict_fund['AcctIDByMXZ'] = idinfo[acctidbybroker]  # fpath里自带交易账户， idinfo仅一个
                                    list_ret.append(dict_fund)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                for acctidbybroker in idinfo:
                    try:
                        fpath_ = fpath.replace('<YYYYMMDD>', self.str_day).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_fund = dict(zip(list_keys, list_values))
                                    if dict_fund['fund_account'] == acctidbybroker:
                                        dict_fund['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                        list_ret.append(dict_fund)   # 有改动
                    except FileNotFoundError as e:
                        e = str(e)

                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['gy_htpb', 'gs_htpb', 'gj_htpb']:    # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账户'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['资金账户']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['单元序号'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['单元序号']]
                                list_ret.append(dict_fund)
            elif data_source_type in ['yh_apama'] and accttype == 'c':  # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '币种', '可用余额', '可取金额', '冻结金额', '总资产', '证券市值', '资金资产']
                    for dataline in list_datalines:
                        dataline = dataline.strip('\n')
                        split_line = dataline.split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind+1:])   # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            dict_fund['AcctIDByMXZ'] = list(idinfo.values())[0]  # fpath里自带交易账户， idinfo仅一个
                            list_ret.append(dict_fund)
                        else:
                            logger_expo.warning('strange fund keys of yh_apama %s'%fpath)
            elif data_source_type in ['yh_apama'] and accttype == 'm':
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '币种', '可用余额', '可取金额', '冻结金额', '总资产', '证券市值',
                                 '资金资产', '总负债', '融资负债', '融券负债', '融资息费', '融券息费', '融资可用额度',
                                 '融券可用额度', '担保证券市值', '维持担保比例', '实时担保比例']
                    for dataline in list_datalines:
                        dataline = dataline.strip('\n')
                        split_line = dataline.split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind + 1:])  # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            dict_fund['AcctIDByMXZ'] = list(idinfo.values())[0]  # fpath里自带交易账户， idinfo仅一个
                            list_ret.append(dict_fund)
                        else:
                            logger_expo.warning('strange fund key of yh_apama %s'%fpath)
            elif data_source_type in ['gf_tyt']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['projectid'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['projectid']]
                                list_ret.append(dict_fund)
            else:
                e = 'Field data_source_type not exist in basic info!'
                if e not in self.list_warn:
                    self.list_warn.append(e)
                    logger_expo.error(e)

        elif sheet_type == 'holding':
            if data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
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
                    acctidbybroker = list(idinfo.values())[0]   # 假定只有一个
                    for list_values in list_list_data[start_index_holding + 1:]:
                        if '没有' in list_values[0]:
                            print(f'{acctidbybroker}: {list_values[0]}')
                        else:
                            if len(list_values) == i_list_keys_length:
                                dict_rec_holding = dict(zip(list_keys, list_values))
                                list_ret.append(dict_rec_holding)
                            else:
                                logger_expo.warning(f'{acctidbybroker}_{data_source_type}_{list_values} not added into database')

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
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
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
                            if dict_rec_holding['账户'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['ax_jzpb']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with open(fpath, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['账户编号'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['账户编号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb',
                                      'swhy_xtpb', 'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb']:   # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账号'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['资金账号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_ehfz_api']:   # 有改动
                for acctidbybroker in idinfo:
                    fpath_ = fpath.replace('YYYYMMDD', self.str_day).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_holding = dict(zip(list_keys, list_values))
                                    dict_rec_holding['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_rec_holding)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                for acctidbybroker in idinfo:
                    fpath_ = fpath.replace('<YYYYMMDD>', self.str_day).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                                continue
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_holding = dict(zip(list_keys, list_values))
                                    # if dict_holding['fund_account'] == acctidbybroker:
                                    dict_holding['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_holding)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['gy_htpb', 'gs_htpb', 'gj_htpb']:    # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['资金账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['单元序号'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['单元序号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['yh_apama'] and accttype == 'm':  # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '股份可用', '当前持仓', '持仓成本', '最新价',
                                 '昨日持仓', '冻结数量', '买入冻结', '卖出冻结', '参考盈亏', '参考市值', '是否为担保品',
                                 '担保品折算率', '融资买入股份余额', '融资买入股份可用']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind+1:])   # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            dict_holding['AcctIDByMXZ'] = list(idinfo.values())[0]
                            list_ret.append(dict_holding)
                        else:
                            logger_expo.warning('strange holidng keys of yh_apama %s'%fpath)
            elif data_source_type in ['yh_apama'] and accttype == 'c':  # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '股份可用', '当前持仓', '持仓成本', '最新价',
                                 '昨日持仓', '股东代码', '买入冻结', '买入冻结金额', '卖出冻结', '卖出冻结金额']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind+1:])   # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            dict_holding['AcctIDByMXZ'] = list(idinfo.values())[0]
                            list_ret.append(dict_holding)
                        else:
                            logger_expo.warning('strange holidng keys of yh_apama %s'%fpath)
            elif data_source_type in ['gf_tyt']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            if dict_holding['projectid'] in idinfo:
                                dict_holding['AcctIDByMXZ'] = idinfo[dict_holding['projectid']]
                                list_ret.append(dict_holding)
        elif sheet_type == 'secloan':
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
                            secid = dict_rec_secloan['证券代码']
                            if secid[0] in ['0', '1', '3']:
                                dict_rec_secloan['交易市场'] = '深A'
                            else:
                                dict_rec_secloan['交易市场'] = '沪A'
                            list_ret.append(dict_rec_secloan)
            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb', 'swhy_xtpb',
                                      'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb'] and accttype in ['m']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_secloan = dict(zip(list_keys, list_values))
                            if dict_rec_secloan['资金账号'] in idinfo:
                                dict_rec_secloan['AcctIDByMXZ'] = idinfo[dict_rec_secloan['资金账号']]
                                list_ret.append(dict_rec_secloan)
            elif data_source_type in ['hait_ehfz_api'] and accttype in ['m']:
                for acctidbybroker in idinfo:
                    try:
                        fpath_ = fpath.replace('YYYYMMDD', self.str_day).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                                continue
                            else:
                                  list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_secloan = dict(zip(list_keys, list_values))
                                    dict_rec_secloan['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_rec_secloan)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi'] and accttype in ['m']:  # 有改动
                for acctidbybroker in idinfo:
                    fpath_ = fpath.replace('<YYYYMMDD>', self.str_day).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                  list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_secloan = dict(zip(list_keys, list_values))
                                    if dict_secloan['fund_account'] == acctidbybroker:
                                        dict_secloan['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                        list_ret.append(dict_secloan)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)
            elif data_source_type in ['gtja_pluto'] and accttype in ['m']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_secloan = dict(zip(list_keys, list_values))
                            if dict_rec_secloan['单元序号'] in idinfo:
                                dict_rec_secloan['AcctIDByMXZ'] = idinfo[dict_rec_secloan['单元序号']]
                                list_ret.append(dict_rec_secloan)
        elif sheet_type == 'order':
            # 先做这几个有secloan的（不然order没意义）:
            if data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb',
                                    'swhy_xtpb', 'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['资金账号'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['资金账号']]
                                list_ret.append(dict_rec_order)
            if data_source_type in ['hait_ehfz_api']:
                for acctidbybroker in idinfo:
                    try:
                        fpath_ = fpath.replace('YYYYMMDD', self.str_day).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                  list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_order = dict(zip(list_keys, list_values))
                                    dict_rec_order['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_rec_order)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)
            elif data_source_type in ['huat_matic_tsi']:  # 有改动
                for acctidbybroker in idinfo:
                    try:
                        fpath_ = fpath.replace('<YYYYMMDD>', self.str_day).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s'%fpath_)
                            else:
                                  list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_order = dict(zip(list_keys, list_values))
                                    dict_order['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_order)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)
            elif data_source_type in ['gtja_pluto']:     # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['单元序号'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['单元序号']]
                                list_ret.append(dict_rec_order)
            elif data_source_type in ['yh_apama']:    # 成交明细不是委托明细
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '委托序号', '买卖方向', '股东号', '成交时间',
                                 '成交编号', '成交价格', '成交数量', '成交金额', '成交类型', '委托数量', '委托价格']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        # for other_value in split_line[-1].split('&'):  # order暂无扩展字段
                        #     ind = other_value.find('=')
                        #     list_values.append(other_value[ind + 1:])
                        if len(list_values) == len(list_keys):
                            dict_order = dict(zip(list_keys, list_values))
                            dict_order['AcctIDByMXZ'] = list(idinfo.values())[0]
                            list_ret.append(dict_order)
                        else:
                            logger_expo.warning('strange order keys of yh_apama %s' % fpath)
            elif data_source_type in ['ax_jzpb']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with open(fpath, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if '信息初始化' in list_values:  # todo 最后一行莫名多出这个（标题和其他行还没有）得改
                            list_values = list_values[:-1]
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['账户编号'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['账户编号']]
                                list_ret.append(dict_rec_order)
            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['账户'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['账户']]
                                list_ret.append(dict_rec_order)
            elif data_source_type in ['gy_htpb', 'gs_htpb', 'gj_htpb']:    # 有改动
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['资金账户']]
                                list_ret.append(dict_rec_holding)
            elif data_source_type in ['gf_tyt']:
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s'%fpath)
                    else:
                          list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_order = dict(zip(list_keys, list_values))
                            if dict_order['projectid'] in idinfo:
                                dict_order['AcctIDByMXZ'] = idinfo[dict_order['projectid']]
                                list_ret.append(dict_order)
        else:
            raise ValueError('Wrong sheet name!')
        return list_ret

    @run_process
    def update_all_rawdata(self):
        """
        1. 出于数据处理留痕及增强robust考虑，将原始数据按照原格式上传到mongoDB中备份
        2. 定义DataFilePath = ['fpath_fund_data'(source), 'fpath_holding_data'(source), 'fpath_trdrec_data(source)',]
        3. acctinfo数据库中DataFilePath存在文件路径即触发文件数据的上传。
        4. 添加：融券未平仓合约数据的上传
        """
        if self.record_update_raw_time is None:  # It's possible that future thread attributes value before
            self.record_update_raw_time = datetime.datetime.today().strftime('%H%M%S')
        # UpdateTime 不用过于精确，只是方便format时查找（只更新最新版）
        if self.is_trading_time:  # update post-trade
            dict_col_rawdata = {'fund': self.db_trddata['trade_rawdata_fund'],
                                'holding': self.db_trddata['trade_rawdata_holding'],
                                'order': self.db_trddata['trade_rawdata_order'],
                                'secloan': self.db_trddata['trade_rawdata_secloan']}
            pathname = 'DataFilePath'
        else:   # update trd
            dict_col_rawdata = {'fund': self.db_posttrddata['post_trade_rawdata_fund'],
                                'holding': self.db_posttrddata['post_trade_rawdata_holding'],
                                'order': self.db_posttrddata['post_trade_rawdata_order'],
                                'secloan': self.db_posttrddata['post_trade_rawdata_secloan']}

            if update_postdata_manually:
                pathname = 'PostDataFilePath'
            else:
                pathname = 'DataFilePath'

        # 相同datafilepath一起读, basic info 里读取文件的地址一样归类（很多账户都在一个文件里）

        dict_filepath2acct = {}
        for _ in self.col_acctinfo.find({'DataDate': self.str_day, 'DataDownloadMark': '1'}):
            datafilepath = _[pathname]
            if datafilepath:
                if 'DownloadDataFilter' in _ and _['DownloadDataFilter']:
                    acctidbybroker = _['DownloadDataFilter']
                else:
                    acctidbybroker = _['AcctIDByBroker']
                if datafilepath in dict_filepath2acct:
                    dict_filepath2acct[datafilepath].update({acctidbybroker: _['AcctIDByMXZ']})
                else:
                    dict_filepath2acct[datafilepath] = {
                      acctidbybroker: _['AcctIDByMXZ'], 'AcctType': _['AcctType'], 'DataSourceType': _['DataSourceType']
                    }       # 同一地址 datasourcetype, accttype一样, 普通户和信用户肯定分开来存

        # Note2. 全部存成一个list，只上传一边，提高性能
        dict_list_upload_recs = {'fund': [], 'holding': [], 'order': [], 'secloan': []}
        for datafilepath in dict_filepath2acct:    # RptMark 是pretrade部分
            info = dict_filepath2acct[datafilepath]
            list_fpath_data = datafilepath[1:-1].split(',')
            data_source_type = info["DataSourceType"]
            accttype = info['AcctType']
            id_info = info.copy()
            del id_info['AcctType']
            del id_info['DataSourceType']
            for i in range(len(list_fpath_data)):
                fpath_relative = list_fpath_data[i]   # 如果有sec，order必须空置 '; ; ;'形式
                if fpath_relative == '':
                    continue
                sheet_name = ['fund', 'holding', 'order', 'secloan'][i]
                # fpath_absolute = os.path.join(self.dirpath_data_from_trdclient, fpath_relative)
                try:
                    list_dicts_rec = self.read_rawdata_from_trdclient(fpath_relative, sheet_name, data_source_type,
                                                                      accttype, id_info)
                    # there are some paths that I do not have access
                    for dict_rec in list_dicts_rec:
                        # if data_source_type == 'zx_wealthcats':
                        #     print(_, fpath_relative)
                        dict_rec['DataDate'] = self.str_day
                        dict_rec['UpdateTime'] = self.record_update_raw_time
                        dict_rec['AcctType'] = accttype
                        dict_rec['DataSourceType'] = data_source_type

                    if list_dicts_rec:
                        dict_list_upload_recs[sheet_name] += list_dicts_rec
                except FileNotFoundError as e:
                    e = str(e)
                    if e not in self.list_warn:
                        logger_expo.warning(e)
                        self.list_warn.append(e)

        for ch in dict_col_rawdata:
            if dict_list_upload_recs[ch]:
                dict_col_rawdata[ch].delete_many({'DataDate': self.str_day})
                dict_col_rawdata[ch].insert_many(dict_list_upload_recs[ch])
        # 更新全局变量

        # Note2.tell future thread this function has finished,因为fmt要在raw都完成才上传
        if self.finish_upload_flag:   # future has finished, only update once
            col_global_var.update_one({'DataDate': self.str_day},
                                      {'$set': {'RawFinished': True, 'RawUpdateTime': self.record_update_raw_time}})
            self.finish_upload_flag = False  # for the upload next time
        else:
            self.finish_upload_flag = True  # tell future thread this function has finished
            # 如果有多个函数可以设置 list_flag, 长度为Nthread -1, 每一个finish把里面一个false改为True, 来保证只上传一次
        print('Update raw data: ', self.record_update_raw_time)

    @run_process
    def update_trddata_f(self):
        if self.record_update_raw_time is None:
            self.record_update_raw_time = datetime.datetime.today().strftime('%H%M%S')
        if self.is_trading_time:
            cursor_find = list(self.col_acctinfo.find({'DataDate': self.str_day, 'AcctType': 'f', 'DataDownloadMark': '1'}))
            list_exceptions = []
            for _ in cursor_find:
                list_future_data_fund = []
                list_future_data_holding = []
                list_future_data_trdrec = []
                prdcode = _['PrdCode']
                acctidbymxz = _['AcctIDByMXZ']
                acctidbyowj = _['AcctIDByOuWangJiang4FTrd']
                data_source_type = _['DataSourceType']
                try:
                    trader = Trader(acctidbyowj)
                except Exception as e:
                    if not(str(e) in list_exceptions):
                        logger_expo.error(e)
                        list_exceptions.append(str(e))
                    if '连接不通' in str(e):  # api 关闭
                        break
                    else:  # 单个产品出问题
                        continue
                dict_res_fund = trader.query_capital()
                if dict_res_fund:
                    dict_fund_to_be_update = dict_res_fund
                    dict_fund_to_be_update['DataDate'] = self.str_day
                    dict_fund_to_be_update['AcctIDByMXZ'] = acctidbymxz
                    dict_fund_to_be_update['AcctIDByOWJ'] = acctidbyowj
                    dict_fund_to_be_update['PrdCode'] = prdcode
                    dict_fund_to_be_update['DataSourceType'] = data_source_type
                    dict_fund_to_be_update['UpdateTime'] = self.record_update_raw_time
                    list_future_data_fund.append(dict_fund_to_be_update)
                    if list_future_data_fund:
                        self.db_trddata['trade_future_api_fund'].delete_many({'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
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
                        dict_holding_to_be_update['DataSourceType'] = data_source_type
                        dict_holding_to_be_update['UpdateTime'] = self.record_update_raw_time
                        list_future_data_holding.append(dict_holding_to_be_update)

                    if list_future_data_holding:
                        self.db_trddata['trade_future_api_holding'].delete_many({'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
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
                        dict_trdrec['DataSourceType'] = data_source_type
                        dict_trdrec['UpdateTime'] = self.record_update_raw_time
                        list_future_data_trdrec.append(dict_trdrec)

                    if list_future_data_trdrec:
                        self.db_trddata['trade_future_api_order'].delete_many({'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                        self.db_trddata['trade_future_api_order'].insert_many(list_future_data_trdrec)

        if self.finish_upload_flag:   # update_all_raw has finished
            col_global_var.update_one({'DataDate': self.str_day},
                                      {'$set': {'RawFinished': True, 'RawUpdateTime': self.record_update_raw_time}})
            self.finish_upload_flag = False  # for the upload next time
        else:
            self.finish_upload_flag = True

    def run(self):
        thread_raw = threading.Thread(target=self.update_all_rawdata)
        thread_raw.start()
        if self.is_trading_time:
            thread_future = threading.Thread(target=self.update_trddata_f)
            # 要启动期货时记得 self.finish_upload_flag = False
            thread_future.start()
        else:
            self.finish_upload_flag = True


class FmtData:       # 包含post
    def __init__(self):
        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.db_trddata = client_local_main['trade_data']
        self.db_posttrddata = client_local_main['post_trade_data']
        self.col_acctinfo = client_local_main['basic_info']['acctinfo']
        self.id2source = ID2Source(client_local_main['basic_info'], 'data/security_id.xlsx')

        self.record_fmt_time = None
        self.record_update_raw_time = None

        self.list_warn = []

        self.lock = threading.Lock()
        return

    def formulate_raw_data(self, acctidbymxz, accttype, patchpath, sheet_type, raw_list):

        list_dicts_fmtted = []

        if accttype in ['c', 'm'] and patchpath is None:
            # patch 默认 fmt

            # ---------------  FUND 相关列表  ---------------------
            # 净资产 = 总资产-总负债 = NetAsset
            # 现金 = 总资产-总市值 普通户里= available_fund, 在资产负债表里
            # 可用资金 = 可用担保品交易资金， 有很多定义， 不在资产负债表里，交易用
            # 可取资金 = 总资产 - 当日交易股票市值-各种手续费-利息+分红， 不在资产负债表里，交易用
            list_fields_af = ['可用', 'A股可用', '可用数', '现金资产', '可用金额', '资金可用金', '可用余额', 'T+0交易可用金额',
                              'enable_balance', 'fund_asset', '可用资金', 'instravl']
            # 新加：matic_tsi_RZRQ: fund_asset, gtja_pluto:可用资金
            list_fields_ttasset = ['总资产', '资产', '总 资 产', '实时总资产', '单元总资产', '资产总额', '账户总资产',
                                   '担保资产', 'asset_balance', 'assure_asset', '账户资产', '资产总值']
            list_fields_na = ['netasset', 'net_asset', '账户净值', '净资产']   # 尽量避免 '产品净值' 等
            list_fields_kqzj = ['可取资金', '可取金额', 'fetch_balance', '沪深T+1交易可用',  '可取余额', 'T+1交易可用金额',
                                '可取数']   # 'T+1交易可用金额'不算可取
            list_fields_tl = ['总负债', 'total_debit']  #
            # list_fields_cb = []     # 券商没义务提供，得从postdata里找
            list_fields_mktvalue = ['总市值', 'market_value', '证券资产', '证券市值']   # 券商没义务提供，得按long-short算

            # ---------------  Security 相关列表  ---------------------
            list_fields_secid = ['代码', '证券代码', 'stock_code', 'stkcode']
            list_fields_symbol = ['证券名称', 'stock_name', '股票名称', '名称']
            list_fields_shareholder_acctid = ['股东帐户', '股东账号', '股东代码']
            list_fields_exchange = ['市场代码', '交易市场', '交易板块', '板块', '交易所', '交易所名称', '交易市场',
                                    'exchange_type', 'market']

            # 有优先级别的列表
            list_fields_longqty = [
                '当前拥股数量', '股票余额', '拥股数量', '证券余额', '证券数量', '库存数量', '持仓数量', '参考持股', '持股数量', '当前持仓',
                '当前余额', '当前拥股', '实际数量', '实时余额', 'current_amount', 'stkholdqty'
            ]
            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                      'SH': 'SSE', 'SZ': 'SZSE',
                                      '上交所A': 'SSE', '深交所A': 'SZSE',
                                      '上证所': 'SSE', '深交所': 'SZSE'}
            dict_ambigu_secidsrc = {'hait_ehfz_api': {'1': 'SZSE', '2': 'SSE'},
                                    'gtja_pluto': {'1': 'SSE', '2': "SZSE"},
                                    'huat_matic_tsi': {'1': 'SSE', '2': 'SZSE'},
                                    'yh_apama': {'0': 'SZSE', '2': 'SSE'},
                                    'ax_jzpb': {'0': 'SZSE', '1': 'SSE'},  # '市场; 市场代码'两个字段
                                    'gf_tyt': {'0': 'SZSE', '1': 'SSE'}}

            # -------------  ORDER 相关列表  ---------------------
            # order委托/entrust除了成交时间等信息最全，不是成交(trade,deal)（没有委托量等）
            #  zxjt_xtpb, zhaos_xtpb只有deal无order； deal/trade？
            # todo 撤单单独列出一个字段 + 买券还券等处理 （huat拆成两个如何合并？）
            #  带数字不明确的得再理一理
            #  OrdID 最好判断下是否有一样的，（数据源可能超级加倍...）
            # 撤单数+成交数=委托数 来判断终态, ordstatus ‘部撤’有时并非终态

            list_fields_cumqty = ['成交数量', 'business_amount', 'matchqty', '成交量']
            list_fields_leavesqty = ['撤单数量', '撤销数量', 'withdraw_amount', 'cancelqty', '撤单量', '已撤数量']
            # apama只有成交，委托待下，成交=终态
            list_fields_side = ['买卖标记', 'entrust_bs',  '委托方向', '@交易类型', 'bsflag', '交易', '买卖标识']
            list_fields_orderqty = ['委托量', 'entrust_amount', '委托数量', 'orderqty']  # XXX_deal 会给不了委托量，委托日期，委托时间，只有成交
            list_fields_ordertime = ['委托时间', 'entrust_time',  'ordertime ', '时间', '成交时间'] # yh
            list_fields_avgpx = ['成交均价', 'business_price', '成交价格', 'orderprice']  # 以后算balance用， exposure不用
            # list_fields_cumamt = ['成交金额', 'business_balance', 'matchamt', '成交额']
            dict_fmtted_side_name = {'买入': 'buy', '卖出': 'sell',
                                     '限价担保品买入': 'buy', '限价买入': 'buy', '担保品买入': 'buy', 'BUY': 'buy', # 担保品=券； 限价去掉,含"...“即可
                                     '限价卖出': 'sell', '限价担保品卖出': 'sell', '担保品卖出': 'sell', 'SELL': 'sell',
                                     '0B': 'buy', '0S': 'sell', '证券买入': 'buy', '证券卖出': 'sell',
                                     '限价融券卖出': 'sell short', '融券卖出': 'sell short',  # 快速交易的 hait=11
                                     '现券还券划拨': 'XQHQ',  '现券还券划拨卖出': 'XQHQ',# 快速交易的 hait=15, gtja=34??
                                     '买券还券划拨': 'MQHQ', '买券还券': 'MQHQ', '限价买券还券': 'MQHQ',  # 快速交易的 hait=13
                                     '撤单': 'cancel', 'ZR': 'Irrelevant', 'ZC': 'Irrelevant'}  # entrust_bs表方向时值为1，2
            dict_ambigu_side_name = {'hait_ehfz_api': {'1': 'buy', '2': 'sell', '12': 'sell short',
                                                   '15': 'XQHQ', '13': 'MQHQ', '0': 'cancel'},
                                     'gtja_pluto': {'1': 'buy', '2': 'sell', '34': 'MQHQ', '32': 'sell short',
                                                    '31': 'buy', '33': 'sell', '36': 'XQHQ'},  # 融资买入， 卖券还款
                                     'huat_matic_tsi': {'1': 'buy', '2': 'sell'}}  # 信用户在后面讨论（需要两个字段拼起来才行）
            # dict_datasource_ordstatus = {
            #     # 参考FIX：New已报； Partially Filled=部成待撤/部成，待撤=PendingCancel不算有效cumqty,中间态
            #     # 国内一般全成，部撤等都表示最终态，cumqty的数值都是有效的(Filled, Partially Canceled)，其他情况的cumqty不能算
            #     # 部撤 Partially Canceled(自己命名的）
            #     'hait_ehfz_api': {'5': 'Partially Canceled', '8': 'Filled', '6': 'Canceled'},
            #     'gtja_pluto': {'4': 'New', '6': 'Partially Filled', '7': 'Filled', '8': 'Partially Canceled',
            #                    '9': 'Canceled', '5': 'Rejected', '10': 'Pending Cancel', '2': 'Pending New'},
            #     'yh_apama': {'2': 'New', '5': 'Partially Filled', '8': 'Filled', '7': 'Partially Filled',  # todo 看表确认
            #                  '6': 'Canceled', '9': 'Rejected', '3': 'Pending Cancel', '1': 'Pending New'},
            #     'huat_matic_tsi': {'2': 'New', '7': 'Partially Filled', '8': 'Filled', '5': 'Partially Filled',
            #                        '6': 'Canceled', '9': 'Rejected', '4': 'Pending Cancel', '1': 'Pending New'},
            #     'zx_wealthcats': {'部撤': 'Partially Filled', '全成': 'Filled', '全撤': 'Canceled', '废单': 'Rejected'},
            #     'xtpb': {'部成': 'Partially Filled', '已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected', '部撤': 'Partially Filled'},
            #     'gt_tyt': {'8': 'Filled'},
            #     'ax_jzpb': {'已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected',
            #                 '部撤': 'Partially Filled', '已报': 'New'},
            #     'htpb': {'已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected', '部撤': 'Partially Filled'},
            #     }
            list_date_format = ['%Y%m%d']
            list_time_format = ['%H%M%S', '%H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S']
            # -------------  SECURITY LOAN 相关列表  ---------------------
            # todo 加hait_xtpb; huat_matic参考其手册;
            #  pluto 合约类型，合约状态里的1和huat里的1指代一个吗？
            #  这块 有不少问题！！！目前只关注short暂不会出错
            list_fields_shortqty = ['未还合约数量', 'real_compact_amount', '未还负债数量', '发生数量']  # 未还合约数量一般是开仓数量
            # 合约和委托没有关系了，但是用contract还是compact(券商版）?
            list_fields_contractqty = ['合约开仓数量', 'business_amount', '成交数量']  # 国外sell short约为“融券卖出”
            # list_fields_contracttype = ['合约类型', 'compact_type']  # 一定能分开 锁券与否
            # list_fields_contractstatus = ['合约状态', 'compact_status', '@负债现状']  # filled='完成'那不是委托？融资融券能用
            list_fields_opdate = ['合约开仓日期', 'open_date', '发生日期']  # FIX 合约: contract
            list_fields_sernum = ['成交编号', '合同编号', 'entrust_no', '委托序号', '合约编号', '合同号', 'instr_no', '成交序号',
                                  '订单号', '委托编号']
            # SerialNumber 券商不统一，目前方便区分是否传了两遍..然而entrust_no还是重复 (RZRQ里的business_no)可以
            list_fields_compositesrc = []  # todo CompositeSource

            # todo: 其它名字’开仓未归还‘，私用融券（专项券池）等得之后补上, 像上面做一个 ambigu区分
            #  遇到bug，pluto vs matic 2指代不一样的
            # Note3. contractstatus, contracttype 有些标准乱，以后有用处理
            # dict_contractstatus_fmt = {'部分归还': '部分归还', '未形成负债': None, '已归还': '已归还',
            #                            '0': '开仓未归还', '1': '部分归还', '5': None,
            #                            '2': '已归还/合约过期', '3': None,
            #                            '未归还': '开仓未归还', '自行了结': None}  # 有bug了...pluto vs matic
            #
            # dict_contracttype_fmt = {'融券': 'rq', '融资': 'rz',
            #                          '1': 'rq', '0': 'rz',
            #                          '2': '其它负债/？？？'}  # 一般没有融资, 其它负债（2）

            if sheet_type == 'fund':  # cash
                list_dicts_fund = raw_list
                # print(list_dicts_fund)
                if list_dicts_fund is None:
                    list_dicts_fund = []
                for dict_fund in list_dicts_fund:
                    data_source = dict_fund['DataSourceType']
                    cash = None
                    avlfund = None  # 'AvailableFund'
                    ttasset = None  # 'TotalAsset'
                    mktvalue = None
                    netasset = None
                    kqzj = None     # 可取资金
                    total_liability = None

                    # 分两种情况： 1. cash acct: 至少要有cash 2. margin acct: 至少要有ttasset

                    flag_check_new_name = True  # 用来弥补之前几个list的缺漏
                    for field_af in list_fields_af:
                        if field_af in dict_fund:
                            avlfund = float(dict_fund[field_af])
                            # todo patchdata fund 处理 有的券商负债的券不一样
                            flag_check_new_name = False
                    err = 'unknown available_fund name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_fund)
                            logger_expo.debug((err, dict_fund))

                    if accttype == 'm':
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        err = 'unknown total asset name %s'%data_source
                        if flag_check_new_name:
                            if data_source not in ['gy_htpb', 'gs_htpb']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                            else:
                                ttasset = float(dict_fund['产品总资产'])

                        flag_check_new_name = True
                        for field_mktv in list_fields_mktvalue:
                            if field_mktv in dict_fund:
                                mktvalue = float(dict_fund[field_mktv])
                                flag_check_new_name = False
                        err = 'unknown total market value name %s'%data_source
                        if flag_check_new_name:
                            if data_source not in ['gtja_pluto']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                        else:
                            cash = ttasset - mktvalue

                        # 读取净资产，总负债，或者两者之中推出另一个
                        for field_na in list_fields_na:
                            if field_na in dict_fund:
                                netasset = float(dict_fund[field_na])

                        for field_tl in list_fields_tl:
                            if field_tl in dict_fund:
                                total_liability = float(dict_fund[field_tl])

                        if total_liability and netasset:
                            delta = total_liability + netasset - ttasset
                            if abs(delta) > 1:
                                err = '券商%s数据错误：总资产 - 总负债 - 净资产 =%d'%(data_source, -delta)
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    logger_expo.error((err, dict_fund))
                                    print(err, dict_fund)
                                # 默认总资产正确：
                                netasset = ttasset - total_liability
                        else:
                            if data_source in ['gy_htpb', 'gs_htpb', 'gj_htpb']:
                                netasset = float(dict_fund['产品净值'])
                            elif data_source in []:  # 没有净资产等字段
                                pass
                            elif not(total_liability is None):
                                netasset = ttasset - total_liability
                            elif not(netasset is None):
                                total_liability = ttasset - netasset
                            else:
                                err = 'unknown net asset or liability name %s'%data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))

                    else:
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset + list_fields_na:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        err = 'unknown total asset name %s'%data_source
                        if flag_check_new_name:
                            if data_source not in ['gy_htpb', 'gs_htpb', 'gj_htpb']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                            else:
                                ttasset = float(dict_fund['产品总资产'])
                        netasset = ttasset
                        total_liability = 0
                        cash = avlfund

                    flag_check_new_name = True
                    for field_kqzj in list_fields_kqzj:
                        if field_kqzj in dict_fund:
                            kqzj = float(dict_fund[field_kqzj])
                            flag_check_new_name = False
                    err = 'unknown 可取资金 name %s'%data_source
                    if flag_check_new_name and data_source not in ['gf_tyt', 'zhaos_xtpb']:   # 他们没有可取
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_fund)
                            logger_expo.debug((err, dict_fund))
                        # flt_cash = flt_ttasset - stock_longamt - etf_longamt - ce_longamt

                    dict_fund_fmtted = {
                        'DataDate': self.str_day,
                        'UpdateTime': self.record_fmt_time,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'Cash': cash,
                        'NetAsset': netasset,
                        'AvailableFund': avlfund,  # flt_approximate_na?
                        'TotalAsset': ttasset,
                        'TotalLiability': total_liability,
                        'KQZJ': kqzj  # 总股本*每股价值 = 证券市值, 之后补上
                    }
                    list_dicts_fmtted.append(dict_fund_fmtted)
            elif sheet_type == 'holding':  # holding
                # 2.整理holding
                # 2.1 rawdata(无融券合约账户)
                list_dicts_holding = raw_list

                for dict_holding in list_dicts_holding:  # 不必 list_dicts_holding.keys()
                    secid = None
                    secidsrc = None
                    symbol = None
                    data_source = dict_holding['DataSourceType']
                    longqty = 0
                    # shortqty = 0
                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_holding:
                            secid = str(dict_holding[field_secid])
                            flag_check_new_name = False
                    err = 'unknown secid name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.debug((err, dict_holding))

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
                            try:
                                if data_source in dict_ambigu_secidsrc:
                                    digit_exchange = str(dict_holding[field_exchange])
                                    secidsrc = dict_ambigu_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_holding[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_holding)
                                    logger_expo.debug((err, dict_holding))
                            flag_check_new_name = False
                            break
                    err = 'unknown security source name %s'%data_source
                    if flag_check_new_name:
                        secidsrc = self.id2source.find_exchange(secid)
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_holding:
                            symbol = str(dict_holding[field_symbol])
                            flag_check_new_name = False
                    err = 'unknown symbol name %s'%data_source
                    if flag_check_new_name:
                        if data_source in ['hait_ehfz_api', 'yh_apama', 'gf_tyt']:
                            symbol = '???'  # 不管，需要可以用wind获取
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_holding)
                                logger_expo.debug((err, dict_holding))

                    flag_check_new_name = True
                    for field_longqty in list_fields_longqty:
                        if field_longqty in dict_holding:
                            longqty = float(dict_holding[field_longqty])
                            flag_check_new_name = False
                    err = 'unknown longqty name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.debug((err, dict_holding))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    dict_holding_fmtted = {
                        'DataDate': self.str_day,
                        'UpdateTime': self.record_fmt_time,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
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

            elif sheet_type == 'order':   # 3.order
                list_dicts_order = raw_list

                for dict_order in list_dicts_order:
                    secid = None
                    secidsrc = None
                    symbol = None
                    leavesqty = None
                    cumqty = None
                    side = None
                    orderqty = None
                    transtime = None
                    avgpx = None
                    sernum = None
                    data_source = dict_order['DataSourceType']

                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_order:
                            secid = str(dict_order[field_secid])
                            flag_check_new_name = False
                    err = 'unknown secid name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

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
                            try:
                                if data_source in dict_ambigu_secidsrc:
                                    digit_exchange = dict_order[field_exchange]
                                    secidsrc = dict_ambigu_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_order[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_order)
                                    logger_expo.debug(err, dict_order)
                            flag_check_new_name = False
                    err = 'unknown security source name %s'%data_source
                    if flag_check_new_name:
                        secidsrc = self.id2source.find_exchange(secid)
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_order:
                            symbol = str(dict_order[field_symbol])
                            flag_check_new_name = False
                    err = 'unknown symbol name %s'%data_source
                    if flag_check_new_name:
                        if data_source in ['hait_ehfz_api', 'yh_apama', 'gf_tyt']:
                            symbol = '???'  # 不管，他们不给symbol需要可以用wind获取
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_order)
                                logger_expo.debug((err, dict_order))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    flag_check_new_name = True
                    for field_cumqty in list_fields_cumqty:
                        if field_cumqty in dict_order:
                            cumqty = dict_order[field_cumqty]
                            flag_check_new_name = False
                    err = 'unknown cumqty name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_leavesqty in list_fields_leavesqty:
                        if field_leavesqty in dict_order:
                            leavesqty = dict_order[field_leavesqty]
                            flag_check_new_name = False
                    err = 'unknown leavesqty name %s'%data_source
                    if flag_check_new_name:
                        if data_source in ['yh_apama']:
                            pass
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_order)
                                logger_expo.debug((err, dict_order))

                    if data_source == 'huat_matic_tsi':
                        entrust_bs = int(dict_order['entrust_bs'])
                        entrust_type = int(dict_order['entrust_type'])
                        try:  # entrust_type: 0普通委托, 2撤单, 6融资, 7融券, 9信用交易
                            side = {(9, 1): 'buy', (9, 2): 'sell', (7, 2): 'sell short',   # 6: 融资买入/卖券还款
                                    (7, 1): 'MQHQ', (6, 1): 'buy', (6, 2): 'sell',
                                    (0, 1): 'buy', (0, 2): 'sell'}[(entrust_type, entrust_bs)]
                        except KeyError:
                            side = 'cancel'
                    else:
                        flag_check_new_name = True
                        for field_side in list_fields_side:
                            if field_side in dict_order:
                                if data_source in dict_ambigu_side_name:
                                    digit_side = dict_order[field_side]
                                    side = dict_ambigu_side_name[data_source][digit_side]
                                else:
                                    str_side = dict_order[field_side]
                                    side = dict_fmtted_side_name[str_side]
                            flag_check_new_name = False
                    err = 'unknown side name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_orderqty in list_fields_orderqty:
                        if field_orderqty in dict_order:
                            orderqty = dict_order[field_orderqty]
                            flag_check_new_name = False
                    err = 'unknown orderqty name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_transtime in list_fields_ordertime:
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
                                transtime = datetime_obj.strftime('%H%M%S')
                            else:
                                err = 'unknown transtime format %s'%data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_order)
                                    logger_expo.debug((err, dict_order))
                            flag_check_new_name = False
                    err = 'unknown transaction time name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_sernum in list_fields_sernum:
                        if field_sernum in dict_order:
                            sernum = str(dict_order[field_sernum])
                            flag_check_new_name = False
                    err = 'unknown serial number name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_avgpx in list_fields_avgpx:
                        if field_avgpx in dict_order:
                            avgpx = float(dict_order[field_avgpx])
                            flag_check_new_name = False
                    err = 'unknown average price name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    dict_order_fmtted = {
                        'DataDate': self.str_day,
                        'UpdateTime': self.record_fmt_time,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'SecurityID': secid,
                        'SerialNumber': sernum,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'CumQty': cumqty,
                        'Side': side,
                        'OrdQty': orderqty,
                        'LeavesQty': leavesqty,
                        'TransactTime': transtime,
                        'AvgPx': avgpx
                    }

                    list_dicts_fmtted.append(dict_order_fmtted)
            elif sheet_type == 'secloan':
                list_dicts_secloan = raw_list
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
                    data_source = dict_secloan['DataSourceType']

                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_secloan:
                            secid = str(dict_secloan[field_secid])
                            flag_check_new_name = False
                    err = 'unknown field_secid name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_secloan:
                            shareholder_acctid = str(dict_secloan[field_shareholder_acctid])
                            if len(shareholder_acctid) == 0:
                                continue
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_secloan:
                            try:
                                if data_source in dict_ambigu_secidsrc:
                                    digit_exchange = dict_secloan[field_exchange]
                                    secidsrc = dict_ambigu_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_secloan[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_secloan)
                                    logger_expo.debug(err, dict_secloan)
                            flag_check_new_name = False
                    err = 'unknown security source name %s'%data_source
                    if flag_check_new_name:
                        secidsrc = self.id2source.find_exchange(secid)
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_secloan:
                            symbol = str(dict_secloan[field_symbol])
                            flag_check_new_name = False
                    err = 'unknown field symbol name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_shortqty in list_fields_shortqty:
                        if field_shortqty in dict_secloan:
                            shortqty = float(dict_secloan[field_shortqty])
                            flag_check_new_name = False
                    err = 'unknown field shortqty name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_contractqty in list_fields_contractqty:
                        if field_contractqty in dict_secloan:
                            contractqty = str(dict_secloan[field_contractqty])
                        flag_check_new_name = False
                    err = 'unknown field contractqty name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_sernum in list_fields_sernum:
                        if field_sernum in dict_secloan:
                            sernum = str(dict_secloan[field_sernum])
                            flag_check_new_name = False
                    err = 'unknown field serum name %s'%data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    # flag_check_new_name = True
                    # for field_contractstatus in list_fields_contractstatus:
                    #     if field_contractstatus in dict_secloan:
                    #         contractstatus = str(dict_secloan[field_contractstatus])
                    #         if contractstatus in dict_contractstatus_fmt:
                    #             contractstatus = dict_contractstatus_fmt[contractstatus]
                    #         else:
                    #             logger_expo.debug('Unknown contractstatus %s'%contractstatus)
                    #         # if contractstatus is None:
                    #         #     raise Exception('During Clearing, we can not have ambiguous status in the compact')
                    #         flag_check_new_name = False
                    #
                    # if flag_check_new_name:
                    #     logger_expo.debug(('unknown field_contractstatus name', dict_secloan))

                    # flag_check_new_name = True
                    # for field_contracttype in list_fields_contracttype:
                    #     if field_contracttype in dict_secloan:
                    #         contracttype = str(dict_secloan[field_contracttype])
                    #         if contracttype in dict_contracttype_fmt:
                    #             contracttype = dict_contracttype_fmt[contracttype]
                    #         else:
                    #             logger_expo.debug('Unknown contractstatus %s'%contracttype)
                    #         flag_check_new_name = False
                    # if flag_check_new_name:
                    #     if data_source != 'hait_ehfz_api':
                    #         logger_expo.debug(('unknown field_contracttype name', dict_secloan))

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
                                err = 'Unrecognized trade date format %s'%data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_secloan)
                                    logger_expo.debug((err, dict_secloan))

                    if flag_check_new_name:
                        err = 'unknown field opdate name %s'%data_source
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_compositesrc in list_fields_compositesrc:
                        if field_compositesrc in dict_secloan:
                            compositesrc = str(dict_secloan[field_compositesrc])
                            flag_check_new_name = False
                    if flag_check_new_name and list_fields_compositesrc:
                        err = 'unknown field_compositesrc name %s'%data_source
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    # print(secidsrc)
                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    dict_secloan_fmtted = {
                        'DataDate': self.str_day,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'UpdateTime': self.record_fmt_time,
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
        elif accttype in ['f'] and patchpath is None:
            list_dicts_future_fund = raw_list
            for dict_fund_future in list_dicts_future_fund:
                avlfund = dict_fund_future['DYNAMICBALANCE']
                acctidbymxz = dict_fund_future['AcctIDByMXZ']
                kqzj = dict_fund_future['USABLECURRENT']
                dict_future_fund_fmtted = {
                    'DataDate': self.str_day,
                    'UpdateTime': self.record_fmt_time,
                    'AcctIDByMXZ': acctidbymxz,
                    'DataSourceType': 'trader_api',
                    'Cash': avlfund,   # 期货户里不能拿券当担保品，全是现金
                    'NetAsset': avlfund,
                    'AvailableFund': avlfund,
                    'TotalAsset': None,  # 总资产大致是LongAmt
                    'TotalLiability': None,
                    'KQZJ': kqzj  # 总股本*每股价值 = 证券市值, 之后补上
                }
                list_dicts_fmtted.append(dict_future_fund_fmtted)
            # 期货holding直接放到 position里
        elif patchpath:
            if accttype == 'o':
                # todo patch 里场外暂时放放
                pass
            else:
                df = pd.read_excel(patchpath, dtype=str, sheet_name=sheet_type)
                df = df.where(df.notnull(), None)
                for i, row in df.iterrows():
                    doc = dict(row)
                    doc['UpdateTime'] = self.record_fmt_time
                    doc['DataDate'] = self.str_day
                    list_dicts_fmtted.append(doc)
        else:
            logger_expo.debug('Unknown account type in basic account info.')
        return list_dicts_fmtted

    @run_process
    def update_fmtdata(self):
        self.record_fmt_time = datetime.datetime.today().strftime('%H%M%S')
        self.record_update_raw_time = col_global_var.find_one({'DataDate': self.str_day})['RawUpdateTime']
        list_dicts_acctinfo = list(
            self.col_acctinfo.find({'DataDate': self.str_day, 'DataDownloadMark': '1'}))  # {'_id': 0}隐藏
        list_dicts_patch = list(client_local_main['basic_info']['data_patch'].find({'DataDate': self.str_day}))
        dict_acct2patch = {}
        for _ in list_dicts_patch:
            acctid = _['AcctIDByMXZ']
            if acctid in dict_acct2patch:
                dict_acct2patch[acctid].append(_)
            else:
                dict_acct2patch[acctid] = [_]

        dict_raw_col = {}
        if self.is_trading_time:
            database = self.db_trddata
            dict_raw_col['future'] = {'fund': database['trade_future_api_fund']}
            dict_raw_col['stock'] = {'fund': database['trade_rawdata_fund'],
                                     'holding': database['trade_rawdata_holding'],
                                     'order': database['trade_rawdata_order'],
                                     'secloan': database['trade_rawdata_secloan']}

            dict_fmt_col = {'fund': database['trade_fmtdata_fund'],
                            'holding': database['trade_fmtdata_holding'],
                            'order': database['trade_fmtdata_order'],
                            'secloan': database['trade_fmtdata_secloan']}
            dict_shtype2listFmtted = {'fund': [], 'holding': [], 'order': [], 'secloan': []}
        else:
            dict_raw_col['future'] = {}
            database = self.db_posttrddata
            dict_raw_col['stock'] = {'fund': database['post_trade_rawdata_fund'],
                                     'holding': database['post_trade_rawdata_holding'],
                                     'secloan': database['post_trade_rawdata_secloan']}
            dict_fmt_col = {'fund': database['post_trade_fmtdata_fund'],
                            'holding': database['post_trade_fmtdata_holding'],
                            'secloan': database['post_trade_fmtdata_secloan']}
            dict_shtype2listFmtted = {'fund': [], 'holding': [], 'secloan': []}

        # Note3 只下载一遍， 根据future; stock分成不同词典， 通过dict_raw_col, 对每一种sheet type(fund....),都只下载一边，存成
        # 关于acctidbymxz的字典： 三层： {stock:{fund:{acctid: [准备format的list]}}}
        dict3d_acctid2rawList = {'future': {}, 'stock': {}}
        for general_type in dict_raw_col:
            for sheet_type in dict_raw_col[general_type]:
                col = dict_raw_col[general_type][sheet_type]
                dict_acctid2rawList = {}
                for _ in col.find({'DataDate': self.str_day}):
                    acctid = _["AcctIDByMXZ"]
                    if acctid in dict_acctid2rawList:
                        dict_acctid2rawList[acctid].append(_)
                    else:
                        dict_acctid2rawList[acctid] = [_]
                dict3d_acctid2rawList[general_type].update({sheet_type: dict_acctid2rawList})

        # dict_mark2listFmtted = {'future': {}, 'stock': {}}
        # dict_shtype2listFmtted = {'fund': [], 'holding': [], 'order': [],  'secloan': []}
        for dict_acctinfo in list_dicts_acctinfo:
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            general_type = {'f': 'future', 'c': 'stock', 'm': 'stock', 'o': 'stock'}[accttype]
            patchpaths = {}
            if dict_acctinfo['PatchMark'] == '1':
                for _ in dict_acct2patch[acctidbymxz]:
                    patchpaths[_['SheetName']] = _['DataFilePath']
            # time_cycle1 = time.time()
            for sheet_type in dict3d_acctid2rawList[general_type].keys():
                if sheet_type in patchpaths:
                    patchpath = patchpaths[sheet_type]
                else:
                    patchpath = None
                # 有patch就不从数据库里取
                if acctidbymxz in dict3d_acctid2rawList[general_type][sheet_type] and patchpath is None:
                    raw_list = dict3d_acctid2rawList[general_type][sheet_type][acctidbymxz]
                elif patchpath:
                    raw_list = None
                else:
                    continue
                # Note3. raw_list  {stock:{fund:{acctid: [准备format的list]}}}里的 ‘准备format的list’
                list_dicts_fmtted = self.formulate_raw_data(acctidbymxz, accttype, patchpath, sheet_type, raw_list)
                dict_shtype2listFmtted[sheet_type] += list_dicts_fmtted

            # time_cycle2 = time.time()
            # print(acctidbymxz, time_cycle2 - time_cycle1)
        for sheet_type in dict_shtype2listFmtted:
            list_dicts_fmtted = dict_shtype2listFmtted[sheet_type]
            target_collection = dict_fmt_col[sheet_type]
            if list_dicts_fmtted and self.is_trading_time:
                # target_collection.delete_many({'DataDate': self.str_day, 'UpdateTime': self.record_fmt_time})
                # 默认record_fmt_time是新时间
                target_collection.insert_many(list_dicts_fmtted)

            if list_dicts_fmtted and not self.is_trading_time:
                target_collection.delete_many({'DataDate': self.str_day})   # post 只上传一次
                target_collection.insert_many(list_dicts_fmtted)
        # print('1', time.time() - tim2, '2', tim2-tim1)
        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {
            'RawFinished': False, 'FmtFinished': True, 'FmtUpdateTime': self.record_fmt_time}})
        return

    def run(self):
        fmt_threading = threading.Thread(target=self.update_fmtdata)
        fmt_threading.start()


class Position:
    def __init__(self):
        # w.start()
        self.dt_day, self.str_day, self.is_trading_day, self.is_trading_time = ini_time_records()
        self.record_fmt_time = None
        self.record_position_time = None
        self.id2source = ID2Source(client_local_main['basic_info'], 'data/security_id.xlsx')

        self.col_acctinfo = client_local_main['basic_info']['acctinfo']
        self.db_trddata = client_local_main['trade_data']
        self.db_posttrddata = client_local_main['post_trade_data']
        self.dict_future2multiplier = {'IC': 200, 'IH': 300, 'IF': 300}
        self.gl_var_last = client_local_main['global_var']['last']

        self.warn_list = []
        # REDIS_HOST = '47.103.187.110'
        # REDIS_PORT = 6379
        # REDIS_PASS = 'Ms123456'
        # self.rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)

        self.event = threading.Event()
        self.lock = threading.Lock()
        return
    #
    # def get_order_last_from_wind(self, list_secid_query):
    #     # we do query only for securities in our account, secid should be type of wind
    #     # w.wsq("600000.SH", "rt_last,rt_latest", func=DemoWSQCallback)
    #     if list_secid_query:
    #         docs = []
    #         dict_wcode2last = {}
    #         last_from_wind = w.wsq(list_secid_query, "rt_last")   # 实时快照现价
    #         if last_from_wind.ErrorCode == 0:
    #             dict_wcode2last = dict(zip(last_from_wind.Codes, last_from_wind.Data[0]))
    #             for key in dict_wcode2last:
    #                 dt = last_from_wind.Times[0]
    #                 doc = {'TransactTime': dt.strftime("%H%M%S"), 'DataDate': dt.strftime("%Y%m%d"),
    #                        'LastPx': dict_wcode2last[key], 'WindCode': key}
    #                 docs.append(doc)
    #         elif last_from_wind.ErrorCode == -40520010:
    #             pass
    #         else:
    #             raise Exception(last_from_wind.Data[0][0])  # Error Msg here
    #         if docs:
    #             self.db_trddata['wind_last'].insert_many(docs)
    #         return dict_wcode2last
    #     else:
    #         return {}
    #
    # def get_order_last_from_redis(self, list_windcode):
    #     list_rediskey = []
    #     length = len(list_windcode)
    #     for i in range(length):
    #         list_rediskey.append('market_'+list_windcode[i])
    #     list_byte_rds = self.rds.mget(list_rediskey)
    #     dict_patch = {'511990.SH': 100, '000016': 1.17, }
    #     dict_windcode2last = {}
    #     for i in range(length):
    #         if list_byte_rds[i] is None:
    #             dict_windcode2last.update({list_windcode[i]: None})
    #             print(list_windcode[i])
    #             #
    #         else:
    #             doc = orjson.loads(list_byte_rds[i])
    #             dict_windcode2last.update({list_windcode[i]: doc['LastPx'] / 10000})   # 放大了10000倍
    #
    #     return dict_windcode2last

    @run_process
    def update_position(self):
        self.record_fmt_time = col_global_var.find_one({'DataDate': self.str_day})['FmtUpdateTime']
        # print(yesterday)
        list_dicts_position = []  # 取名改改...
        set_windcode_to_search = set()  # 防止重复
        dict_id2info = {}
        dict_pair2allcol = {}  # 为了只遍历一遍各个表格，不然特别慢！
        dict_learn_secid2src = {}  # 有的post里面没有source，得用fmt里的“学”

        list_dicts_acctinfo = list(self.col_acctinfo.find({'DataDate': self.str_day, 'DataDownloadMark': '1'}))
        for dict_acctinfo in list_dicts_acctinfo:
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            # print('acctidbymxz', acctidbymxz)
            accttype = dict_acctinfo['AcctType']
            patchmark = dict_acctinfo['PatchMark']
            data_source = dict_acctinfo['DataSourceType']
            dict_id2info.update({acctidbymxz: [accttype, patchmark, data_source]})

        # Note4. pair作用： 相同的acctid, 证券对应唯一的position，但是holding，合约，order可能不止一个
        # 存成字典dict_pair2allcol： {pair: {holding:[...], order:[...], secloan: []},之后遍历每一个key
        # Note4. 有时候计算错误（实际持仓和计算的持仓不一样）的原因是市场错了 idsrc分错导致应该当成一个票子的没合并
        # Note4. 上面的dict_id2info作用在于把acctinfo拍扁成dict，pair -> acctid -> 账户信息
        for col_name in ['trade_fmtdata_order', 'trade_fmtdata_holding', 'trade_fmtdata_secloan']:
            list_to_add = list(self.db_trddata[col_name].find(
                {'DataDate': self.str_day, 'UpdateTime': self.record_fmt_time}))
            # {'$gte':}，不用万一readfmt两遍（position还没结束），太多太超前会超级加倍
            for _ in list_to_add:
                sid = _['SecurityID']
                idsrc = _['SecurityIDSource']
                pair = (_['AcctIDByMXZ'], sid, idsrc, _['SecurityType'])
                if sid in dict_learn_secid2src:
                    if dict_learn_secid2src[sid] != idsrc:
                        dict_learn_secid2src[sid] = None   # 感觉得改改
                else:
                    dict_learn_secid2src.update({sid: idsrc})

                # set_pair_secid = set_pair_secid | {pair}  # 并集
                all_doc = _.copy()
                if pair in dict_pair2allcol:
                    if col_name in dict_pair2allcol[pair]:
                        dict_pair2allcol[pair][col_name].append(all_doc)
                    else:
                        dict_pair2allcol[pair].update({col_name: [all_doc]})
                else:
                    dict_pair2allcol.update({pair: {col_name: [all_doc]}})
        # post_col_name = ['fmtdata_holding', 'fmtdata_secloan']
        for col_name in ['post_trade_fmtdata_holding', 'post_trade_fmtdata_secloan']:
            list_to_add = list(self.db_posttrddata[col_name].find({'DataDate': self.str_day}))    # postdata同一天9：00算
            for _ in list_to_add:
                if not ('SecurityType' in _):  # 老版post里无IDSource...
                    if not ('SecurityIDSource' in _):
                        sid = _['SecurityID']
                        if sid in dict_learn_secid2src:
                            _['SecurityIDSource'] = dict_learn_secid2src[sid]
                        else:
                            _['SecurityIDSource'] = self.id2source.find_exchange(sid)  # 因为可能要回答问题所以尽量不做
                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[_['SecurityIDSource']]
                    _['SecurityType'] = get_sectype_from_code(_['SecurityID'] + windcode_suffix)
                pair = (_['AcctIDByMXZ'], _['SecurityID'], _['SecurityIDSource'], _['SecurityType'])
                # set_pair_secid = set_pair_secid | {pair}  # 并集
                all_doc = _.copy()
                if pair in dict_pair2allcol:
                    if col_name in dict_pair2allcol[pair]:
                        dict_pair2allcol[pair][col_name].append(all_doc)
                    else:
                        dict_pair2allcol[pair].update({col_name: [all_doc]})
                else:
                    dict_pair2allcol.update({pair: {col_name: [all_doc]}})
        for col_name in ['trade_future_api_holding']:
            list_to_add = list(self.db_trddata[col_name].find({'DataDate': self.str_day}))
            for _ in list_to_add:
                pair = (_['AcctIDByMXZ'], _['instrument_id'], _['exchange'])
                all_doc = _.copy()
                if pair in dict_pair2allcol:
                    if col_name in dict_pair2allcol[pair]:
                        dict_pair2allcol[pair][col_name].append(all_doc)
                    else:
                        dict_pair2allcol[pair].update({col_name: [all_doc]})
                else:
                    dict_pair2allcol.update({pair: {col_name: [all_doc]}})

        for pair in dict_pair2allcol:  # or pair in dict_pair2allcol.keys()
            acctidbymxz = pair[0]
            secid = pair[1]
            # if acctidbymxz == '3033_m_yh_5930' and secid == '510500':
            #     print(end='')
            secidsrc = pair[2]
            sectype = None

            try:
                accttype, patchmark, data_source = dict_id2info[acctidbymxz]
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
                list_dicts_secloan = dict_pair2allcol[pair]['trade_fmtdata_secloan']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_secloan = []
            try:
                list_dicts_post_secloan = dict_pair2allcol[pair]['post_trade_fmtdata_secloan']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_post_secloan = []
            try:
                list_dicts_order = dict_pair2allcol[pair]['trade_fmtdata_order']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_order = []
            try:
                list_dicts_holding_future = dict_pair2allcol[pair]['trade_future_api_holding']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_holding_future = []

            if accttype in ['c', 'm', 'o'] and self.is_trading_time:
                if len(pair) == 4:
                    sectype = pair[3]

                windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                windcode = secid + windcode_suffix

                longqty = 0    # longqty可能准
                longqty_ref = 0
                shortqty = 0
                shortqty_ref = 0   # 实在没有postdata就用它
                dict_holding_id = 'no reference'
                dict_post_holding_id = 'no reference'

                if len(list_dicts_post_holding) == 1:
                    longqty = float(list_dicts_post_holding[0]['LongQty'])
                    dict_post_holding_id = list_dicts_post_holding[0]['_id']
                elif len(list_dicts_post_holding) == 0:
                    pass
                else:
                    tmax = time.strptime('0:0:0', '%H:%M:%S')
                    post_holding_id = list_dicts_post_holding[0]['_id']
                    for d in list_dicts_post_holding:
                        t = time.strptime(d['UpdateTime'], '%H%M%S')
                        if tmax < t:
                            longqty = int(d['LongQty'])
                            tmax = t
                            post_holding_id = d['_id']
                    print('The postholding has too many information', post_holding_id)

                if len(list_dicts_holding) == 1:
                    longqty_ref = float(list_dicts_holding[0]['LongQty'])
                    dict_holding_id = list_dicts_holding[0]['_id']
                elif len(list_dicts_holding) == 0:
                    pass
                else:
                    tmax = time.strptime('0:0:0', '%H:%M:%S')
                    for d in list_dicts_holding:
                        t = time.strptime(d['UpdateTime'], '%H%M%S')
                        if tmax < t:
                            longqty_ref = float(d['LongQty'])
                            dict_holding_id = d['_id']
                            tmax = t

                if len(list_dicts_post_secloan) > 0:
                    for d in list_dicts_post_secloan:
                        shortqty += float(d['ShortQty'])  # 可能多个合约

                if len(list_dicts_secloan) > 0:
                    for d in list_dicts_secloan:
                        shortqty_ref += float(d['ShortQty'])  # 可能多个合约

                for dict_order in list_dicts_order:
                    # if self.str_day == dict_order['TradeDate']:
                    side = dict_order['Side']
                    cumqty = int(dict_order['CumQty'])
                    avgpx = dict_order['AvgPx']
                    if 'yh' in dict_order['AcctIDByMXZ']:
                        valid_cum = True
                    elif data_source == 'huat_matic_tsi' and avgpx == 0:
                        # 华泰有0元成交0元委托（不算撤单，成交量不为0的奇怪情况）
                        valid_cum = False
                    else:
                        leavesqty = int(dict_order['LeavesQty'])
                        orderqty = int(dict_order['OrdQty'])
                        valid_cum = (cumqty + leavesqty == orderqty)
                    if valid_cum:  # 交易最终态的cumqty才可以用
                        if side == 'buy':
                            longqty += cumqty
                        elif side == 'sell':
                            longqty -= cumqty
                        elif side == 'sell short':
                            shortqty += cumqty
                        elif side == 'XQHQ':
                            longqty -= cumqty
                            shortqty -= cumqty
                        elif side == 'MQHQ':  # 导致资金变动而不是券的变动
                            shortqty -= cumqty
                        else:  # 判断撤单
                            continue

                # if longqty < 0:  # 有的券商没有sell short说法, long就是net..
                #     warnings.warn("LongQty is Negative: short: %f, long: %f because "
                #                   "postdata is not clean, id %s" % (shortqty, longqty, dict_secloan_id))
                             # longqty = 0
                    # todo long小于0各种情况讨论。。c用户； m时有short， 无short

                # check
                if patchmark == '1':    # 照抄
                    longqty = longqty_ref
                    shortqty = shortqty_ref
                elif accttype == 'c' and (data_source in broker_c_without_postdata):  # Note1. 可以去掉，只作为验算
                    longqty = longqty_ref
                elif data_source in broker_m_without_postdata and accttype == 'm':   # 数据升级等问题导致只能抄
                    longqty = longqty_ref
                    shortqty = shortqty_ref
                else:
                    # todo 如果有问题界面里只显示一次
                    if abs(longqty - longqty_ref) > 0.01:   # hait, huat, gtja OK
                        err = "\n Please check fmtdata_holding: %s and the one in posttrade %s and order: %s \n" \
                              "The alogrithm to calculate longqty of account: %s is somehow wrong! \n" \
                              "longqty: %d; shortqty: %d; longqty_ref: %d"\
                              % (dict_holding_id, dict_post_holding_id, secid, acctidbymxz, longqty, shortqty, longqty_ref)

                        if acctidbymxz not in self.warn_list:
                            print(self.record_fmt_time, ':', err)
                            self.warn_list.append(acctidbymxz)
                            logger_expo.error(err)

                        longqty = longqty_ref
                        shortqty = shortqty_ref

                # 只监控有票子的
                if longqty != 0 or shortqty != 0 or longqty_ref != 0:
                    set_windcode_to_search = set_windcode_to_search | {windcode}
                    dict_position = {
                        'DataDate': self.str_day,
                        'UpdateTime': None,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                         #  'Symbol': symbol,  # 有的券商没有，可加可不加
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        # 'LongQty_ref': longqty_ref,
                        'ShortQty': shortqty,
                        'NetQty': longqty - shortqty,
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
                try:  # SHFE - 'ssXXXX'格式等先不管，它们对应CTA策略
                    windcode = dict_future2spot_windcode[secid_first_part]
                except:
                    continue
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
                        'SecurityType': 'Index Future',
                        'Symbol': None,
                        'SecurityIDSource': secidsrc,
                        'LongQty': future_longqty,
                        'ShortQty': future_shortqty,
                        'NetQty': future_longqty - future_shortqty,
                        'LongAmt': None,
                        'ShortAmt': None,
                        'NetAmt': None,
                        'WindCode': windcode
                    }
                    list_dicts_position.append(dict_position)

        # 统一一次询问现价，节约时间，市价更加精确
        self.record_position_time = datetime.datetime.today().strftime("%H%M%S")
        # self.record_wind_query_time = (datetime.datetime.today() - datetime.timedelta(hours=1, seconds=10)).strftime("%H%M%S")

        list_windcode_to_search = list(set_windcode_to_search)
        self.gl_var_last.update_one({'Key': 'SecidQuery'}, {'$set': {'Value': list_windcode_to_search}})
        print('Getting last price from wind...')
        time.sleep(2)  # wait wind_last.py
        dict_windcode2last = self.gl_var_last.find_one({'Key': 'Wcode2Last'})['Value']
        if dict_windcode2last is None:
            dict_windcode2last = {}
            iter_last = self.db_trddata['wind_last'].find({'WindCode': {'$in': list_windcode_to_search}})
            for _d in iter_last:
                dict_windcode2last.update({_d['WindCode']: _d['LastPx']})
        elif set(dict_windcode2last) != set_windcode_to_search:
            list_windcode_to_add = list(set_windcode_to_search - set(dict_windcode2last))
            iter_last = self.db_trddata['wind_last'].find({'WindCode': {'$in': list_windcode_to_add}})
            for _d in iter_last:
                dict_windcode2last.update({_d['WindCode']: _d['LastPx']})

        # self.gl_var_last.update_one({'Key': 'Wcode2Last'}, {'$set': {'Value': None}})
        for dict_position in list_dicts_position:
            windcode = dict_position['WindCode']
            if dict_position['SecurityType'] == 'Index Future':
                secid_first_part = dict_position['SecurityID'][:-4]
                point = self.dict_future2multiplier[secid_first_part]
                dict_position['LongAmt'] = dict_position['LongQty'] * dict_windcode2last[windcode] * point
                dict_position['ShortAmt'] = dict_position['ShortQty'] * dict_windcode2last[windcode] * point
            else:
                dict_position['LongAmt'] = dict_position['LongQty']*dict_windcode2last[windcode]
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

        col_global_var.update_one({'DataDate': self.str_day}, {'$set': {
            'FmtFinished': False, 'PosFinished': True, 'PositionUpdateTime': self.record_position_time}})
        # print("Update Position finished")
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
        self.record_fmt_time = None

        self.db_trddata = client_local_main['trade_data']
        self.col_acctinfo = client_local_main['basic_info']['acctinfo']

        self.event = threading.Event()
        self.lock = threading.Lock()

    @run_process
    def exposure_analysis(self):
        self.record_position_time = col_global_var.find_one({'DataDate': self.str_day})['PositionUpdateTime']
        list_dicts_acctinfo = list(self.col_acctinfo.find({'DataDate': self.str_day, 'DataDownloadMark': '1'}))
        list_dicts_position = list(self.db_trddata['trade_position'].find({'DataDate': self.str_day, 'UpdateTime': self.record_position_time}))
        dict_acctid2list_position = {}
        # Note5. 只调用mongodb.find()一次
        # 原本：for 遍历acctid， 每次查找collection.find(acctidbymxz, datadate, updatetime),
        # 改进： 只查找一次collection.find( datadate, updatetime)， 然后存成词典： {acctid: [position]}
        # 也方便汇总成一个账户的exposure
        for _ in list_dicts_position:
            acctidbymxz = _['AcctIDByMXZ']
            if acctidbymxz in dict_acctid2list_position:
                dict_acctid2list_position[acctidbymxz].append(_)
            else:
                dict_acctid2list_position[acctidbymxz] = [_]
        list_dict_acct_exposure = []
        dict_prdcode2exposure = {}
        for dict_acctinfo in list_dicts_acctinfo:
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            prdcode = dict_acctinfo['PrdCode']
            mdm = dict_acctinfo['MonitorDisplayMark']
            if dict_acctinfo['MonitorExposureAnalysisMark'] != '0':
                acct_exposure_dict = {'AcctIDByMXZ': acctidbymxz, 'PrdCode': prdcode, 'MonitorDisplayMark': mdm,
                                      'UpdateTime': self.record_position_time, 'DataDate': self.str_day,
                                      'LongQty': 0, 'ShortQty': 0, 'NetQty': 0,
                                      'LongAmt': 0, 'ShortAmt': 0, 'NetAmt': 0}
                if acctidbymxz in dict_acctid2list_position:
                    for dict_position in dict_acctid2list_position[acctidbymxz]:
                        if dict_position['SecurityType'] == 'IrrelevantItem' or dict_position['SecurityType'] == 'CE':
                            continue
                        else:
                            for key in ['LongQty', 'ShortQty', 'NetQty', 'LongAmt', 'ShortAmt', 'NetAmt']:
                                acct_exposure_dict[key] += dict_position[key]

                if not (prdcode in dict_prdcode2exposure):
                    prdcode_exposure_dict = acct_exposure_dict.copy()
                    del prdcode_exposure_dict['AcctIDByMXZ']
                    dict_prdcode2exposure[prdcode] = prdcode_exposure_dict
                    if accttype != 'f':
                        dict_prdcode2exposure[prdcode]['StkLongAmt'] = acct_exposure_dict['LongAmt']
                        dict_prdcode2exposure[prdcode]['StkShortAmt'] = acct_exposure_dict['ShortAmt']
                        dict_prdcode2exposure[prdcode]['StkNetAmt'] = acct_exposure_dict['NetAmt']
                    else:
                        dict_prdcode2exposure[prdcode]['StkLongAmt'] = 0
                        dict_prdcode2exposure[prdcode]['StkShortAmt'] = 0
                        dict_prdcode2exposure[prdcode]['StkNetAmt'] = 0
                elif dict_prdcode2exposure[prdcode]['LongQty'] is None:
                    pass
                else:
                    # 4舍5入保留两位小数， todo 在flask展示里而不是在数据库里保留2位
                    for key in ['LongQty', 'ShortQty', 'NetQty', 'LongAmt', 'ShortAmt', 'NetAmt']:
                        dict_prdcode2exposure[prdcode][key] += acct_exposure_dict[key]
                        acct_exposure_dict[key] = round(acct_exposure_dict[key], 2)
                    if accttype != 'f':
                        dict_prdcode2exposure[prdcode]['StkLongAmt'] += acct_exposure_dict['LongAmt']
                        dict_prdcode2exposure[prdcode]['StkShortAmt'] += acct_exposure_dict['ShortAmt']
                        dict_prdcode2exposure[prdcode]['StkNetAmt'] += acct_exposure_dict['NetAmt']
                list_dict_acct_exposure.append(acct_exposure_dict)

        for prdcode in dict_prdcode2exposure:
            for key in ['LongQty', 'ShortQty', 'NetQty', 'LongAmt', 'ShortAmt', 'NetAmt']:
                if dict_prdcode2exposure[prdcode][key]:
                    dict_prdcode2exposure[prdcode][key] = round(dict_prdcode2exposure[prdcode][key], 2)

        list_dict_prdcode_exposure = list(dict_prdcode2exposure.values())

        # print(pd.DataFrame(list_dict_acct_exposure))
        # logger_expo.info(pd.DataFrame(list_dict_prdcode_exposure))
        if list_dict_acct_exposure:
            self.db_trddata['trade_exposure_by_acctid'].delete_many({'DataDate': self.str_day, 'UpdateTime': self.record_position_time})
            self.db_trddata['trade_exposure_by_acctid'].insert_many(list_dict_acct_exposure)
            self.db_trddata['trade_exposure_by_prdcode'].insert_many(list_dict_prdcode_exposure)

        return

    def run(self):
        exposure_monitoring_thread = threading.Thread(target=self.exposure_analysis)
        exposure_monitoring_thread.start()


if __name__ == '__main__':
    read_raw = ReadRaw()
    fmt_data = FmtData()

    read_raw.run()
    fmt_data.run()
    if read_raw.is_trading_time:
        position = Position()
        exposure = Exposure()
        position.run()
        exposure.run()
    elif not update_postdata_manually:
        # 如果在 run_threading里的话每个都会跑3遍(run_pending把所有schedule里的都跑一边）
        while True:
            print(datetime.datetime.today().strftime("%d  %H:%M:%S"))
            schedule.run_pending()
            time.sleep(schedule_interval)  # 7200 睡2小时
