"""
todo list
1. basic_info 上传到数据库里
2. 利用db_trading_data 里的已有函数生成raw, 更新？- 边用边改
3. 万得学习wss, 实盘数据，时间序列：获取close以及实时交易数据
   获取close， 行情数据所有股票的last （wst获得，30s一次），先放到raw（一个thread）
   再从raw里，根据每个持仓的股票信息（用代码）搜索
4. post-trade-data每天早上9:00获取清算数据？量大？
"""
import pandas as pd
import pymongo
from WindPy import *
from trader_v1 import Trader
import codecs
import threading
from openpyxl import load_workbook
from xlrd import open_workbook
import datetime
import time
import functools

# global functions and objects


def run_every_30s(func):
    @functools.wraps(func)
    # 人的操作成本比较高，生产环节得删..
    def wrapper(self, *args, **kwargs):
        while self.running:
            if self.event.is_set():
                self.lock.acquire()
                func(self, *args, **kwargs)
                self.lock.release()
                print('Function: ', func.__name__, 'finished')
                time.sleep(10)
                # print('I am awaken!')
    return wrapper


class ExposMonit:
    def __init__(self):
        """
        Incorporated inputs: (可以全局化...)
        1.MongoClient('host:port username@admin:pwd')
        2.path of basic_info
        3.database names and collection names : trddata, basicinfo
        4.date
        """
        w.start()
        # todo 时间最好有今天以及昨天两个，因为各个文档更新时间不同(如果今天找不到，找数据库/地址里前一天的）
        self.dt_day = datetime.datetime.today() - datetime.timedelta(days=1)
        self.str_day = self.dt_day.strftime('%Y%m%d')

        self.client_mongo = pymongo.MongoClient(port=27017, host='localhost',
                                                username='admin', password='123456')
        self.db_trddata = self.client_mongo['trddata']
        self.db_basicinfo = self.client_mongo['basicinfo']
        self.col_acctinfo = self.db_basicinfo['acctinfo']

        self.path_basic_info = 'data/basic_info.xlsx'
        self.dict_future2multiplier = {'IC': 200, 'IH': 300, 'IF': 300}
        # self.upload_basic_info()
        # self.dirpath_data_from_trdclient

        self.event = threading.Event()
        self.lock = threading.Lock()
        self.running = True

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
            self.db_basicinfo[sheet_name].update(list_records)
        return

    def read_rawdata_from_trdclient(self, fpath, str_c_h_secliability_mark, data_source_type, accttype,
                                    acctidbybroker):
        """
        从客户端下载数据，并进行初步清洗。为字符串格式。
        tdx倒出的txt文件有“五粮液错误”，使用xls格式的可解决

        已更新券商处理格式：
            华泰: hexin, txt, cash, margin, capital, holding
            国君: 富易, csv
            海通: ehtc, xlsx, cash, capital, holding
            申宏: alphabee, txt
            建投: alphabee, txt
            中信: tdx, txt, vip, cash, capital, holding,
            民生: tdx, txt
            华福: tdx, txt

        :param acctidbybroker: 用于pb类文件对账户编号的过滤。
        :param fpath:
        :param accttype: c: cash, m: margin, f: future
        :param str_c_h_secliability_mark: ['capital', 'holding', 'secliability']
        :param data_source_type:

        :return: list: 由dict rec组成的list
        """
        # todo : 每个raw加上 AcctIDByMXZ， DataDate, UpdateTime
        # todo : 注释改进
        list_ret = []
        if str_c_h_secliability_mark == 'capital':
            dict_rec_capital = {}
            if data_source_type in ['huat_hx', 'hait_hx', 'zhes_hx', 'tf_hx', 'db_hx', 'wk_hx'] and accttype == 'c':
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()[0:6]
                    for dataline in list_datalines:
                        list_data = dataline.strip().split(b'\t')
                        for data in list_data:
                            list_recdata = data.strip().decode('gbk').split('：')
                            dict_rec_capital[list_recdata[0].strip()] = list_recdata[1].strip()

            elif data_source_type in ['yh_hx'] and accttype in ['c']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[5].decode('gbk').split()
                    list_values = list_datalines[6].decode('gbk').split()
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['yh_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_capital = df_read.to_dict('records')[0]

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
                            dict_rec_capital[list_recdata[0].strip()] = \
                                (lambda x: x if x.strip() in ['人民币'] else list_recdata[1].strip())(list_recdata[1])

            elif data_source_type in ['gtja_fy'] and accttype in ['c', 'm']:
                wb = open_workbook(fpath, encoding_override='gbk')
                ws = wb.sheet_by_index(0)
                list_keys = ws.row_values(5)
                list_values = ws.row_values(6)
                dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['hait_ehtc'] and accttype == 'c':
                df_read = pd.read_excel(fpath, skiprows=1, nrows=1)
                dict_rec_capital = df_read.to_dict('records')[0]

            elif data_source_type in ['hait_datagrp']:
                df_read = pd.read_excel(fpath, nrows=2)
                dict_rec_capital = df_read.to_dict('records')[0]

            elif data_source_type in ['xc_tdx', 'zx_tdx', 'ms_tdx'] and accttype in ['c', 'm']:
                # todo 存在五 粮 液错误
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    dataline = list_datalines[0][8:]
                    list_recdata = dataline.strip().decode('gbk').split()
                    for recdata in list_recdata:
                        list_recdata = recdata.split(':')
                        dict_rec_capital.update({list_recdata[0]: list_recdata[1]})

            elif data_source_type in ['wk_tdx', 'zhaos_tdx', 'huat_tdx', 'hf_tdx', 'gx_tdx'] and accttype in ['c',
                                                                                                              'm']:
                # 已改为xls版本，避免'五粮液错误'
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    list_values = list_datalines[1].strip().decode('gbk').replace('=', '').replace('"', '').split(
                        '\t')
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['zxjt_alphabee', 'swhy_alphabee'] and accttype in ['c', 'm']:
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split()
                    list_values = list_datalines[1].decode('gbk').split()
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['swhy_alphabee_dbf2csv', 'ax_custom']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].decode('gbk').split(',')
                    list_values = list_datalines[1].decode('gbk').split(',')
                    dict_rec_capital.update(dict(zip(list_keys, list_values)))

            elif data_source_type in ['patch']:
                pass

            elif data_source_type in ['zx_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital_wealthcats = dict(zip(list_keys, list_values))
                            if dict_capital_wealthcats['账户'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital_wealthcats)

            elif data_source_type in ['db_wealthcats']:
                fpath = fpath.replace('YYYY-MM-DD', self.dt_day.strftime('%Y-%m-%d'))
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital_wealthcats = dict(zip(list_keys, list_values))
                            if dict_capital_wealthcats['账户'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital_wealthcats)

            elif data_source_type in ['ax_jzpb']:
                # todo 账户编号不稳定，求源
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital_wealthcats = dict(zip(list_keys, list_values))
                            if dict_capital_wealthcats['账户编号'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital_wealthcats)

            elif data_source_type in ['zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb']:   # 有改动
                # todo 更改路径中的日期？没看到日期YYYYMMDD,校验新加的
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['资金账号'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital)
            elif data_source_type in ['huat_matic_tsi']:    # 有改动
                # todo : raw变format所需要的名称
                fpath = fpath.replace('<YYYYMMDD>', self.str_day)
                with codecs.open(fpath, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['fund_account'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital)    # 有改动
            elif data_source_type in ['gs_htpb']:    # 有改动
                # todo : raw变format所需要的名称
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()

                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['资金账户'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital)
            elif data_source_type in ['gtja_pluto']:     # 有改动
                # todo : raw变format所需要的名称
                fpath = fpath.replace('YYYYMMDD', self.str_day)
                with codecs.open(fpath, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()

                    list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_capital = dict(zip(list_keys, list_values))
                            if dict_capital['单元序号'] == acctidbybroker:
                                dict_rec_capital.update(dict_capital)
            else:
                raise ValueError('Field data_source_type not exist in basic info!')
            list_ret.append(dict_rec_capital)

        elif str_c_h_secliability_mark == 'holding':
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

            elif data_source_type in ['zxjt_xtpb','zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb']:   # 有改动
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
                # todo : raw变format所需要的名称
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
                # todo : raw变format所需要的名称
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
                # todo : raw变format所需要的名称
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

        elif str_c_h_secliability_mark == 'secliability':
            if data_source_type in ['zhaos_tdx'] and accttype in ['m']:
                with open(fpath, 'rb') as f:
                    list_datalines = f.readlines()
                    start_index_secliability = None
                    for index, dataline in enumerate(list_datalines):
                        str_dataline = dataline.decode('gbk')
                        if '证券代码' in str_dataline:
                            start_index_secliability = index
                    list_keys = [x.decode('gbk') for x in list_datalines[start_index_secliability].strip().split()]
                    i_list_keys_length = len(list_keys)
                    for dataline in list_datalines[start_index_secliability + 1:]:
                        list_data = dataline.strip().split()
                        if len(list_data) == i_list_keys_length:
                            list_values = [x.decode('gbk') for x in list_data]
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            # todo 自定义： 根据证券代码推测交易市场
                            secid = dict_rec_holding['证券代码']
                            if secid[0] in ['0', '1', '3']:
                                dict_rec_holding['交易市场'] = '深A'
                            else:
                                dict_rec_holding['交易市场'] = '沪A'
                            list_ret.append(dict_rec_holding)
        else:
            raise ValueError('Wrong str_c_h_secliability_mark input!')
        return list_ret

    @run_every_30s
    def update_rawdata(self):
        """
        1. 出于数据处理留痕及增强robust考虑，将原始数据按照原格式上传到mongoDB中备份
        2. 定义DataFilePath = ['fpath_capital_data'(source), 'fpath_holding_data'(source), 'fpath_trdrec_data(source)',]
        3. acctinfo数据库中DataFilePath存在文件路径即触发文件数据的上传。
        4. 添加：融券未平仓合约数据的上传
        """
        col_manually_rawdata_capital = self.db_trddata['manually_rawdata_capital']
        col_manually_rawdata_holding = self.db_trddata['manually_rawdata_holding']
        col_manually_rawdata_secliability = self.db_trddata['manually_rawdata_secliability']
        for _ in self.col_acctinfo.find({'DataDate': self.str_day, 'RptMark': 1}, {'_id': 0}):
            datafilepath = _['DataFilePath']
            # print(datafilepath)
            if datafilepath:
                # todo 算法上可以再稍微改进‘加速’一下，比如同一个fpath的不同 acctid一起读（一次遍历多个账户，一个文件至多读一次）
                list_fpath_data = _['DataFilePath'][1:-1].split(',')
                acctidbymxz = _['AcctIDByMXZ']
                acctidbybroker = _['AcctIDByBroker']
                downloaddatafilter = _['DownloadDataFilter']
                data_source_type = _['DataSourceType']
                accttype = _['AcctType']

                for ch in ['capital', 'holding', 'secliability']:
                    if ch == 'capital':
                        fpath_relative = list_fpath_data[0]
                        col_manually_rawdata = col_manually_rawdata_capital
                    elif ch == 'holding':
                        fpath_relative = list_fpath_data[1]
                        col_manually_rawdata = col_manually_rawdata_holding
                    elif ch == 'secliability':
                        if len(list_fpath_data) > 2:
                            fpath_relative = list_fpath_data[2]
                            if fpath_relative:
                                col_manually_rawdata = col_manually_rawdata_secliability
                            else:
                                continue
                        else:
                            continue
                    else:
                        raise ValueError('Value input not exist in capital and holding.')

                    col_manually_rawdata.delete_many({'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                    # fpath_absolute = os.path.join(self.dirpath_data_from_trdclient, fpath_relative)
                    try:
                        if downloaddatafilter:      #  gtjy_pluto只有交易单元，没有账户名
                            acctidbybroker = downloaddatafilter
                        list_dicts_rec = self.read_rawdata_from_trdclient(
                            fpath_relative, ch, data_source_type, accttype, acctidbybroker
                        )
                        # there are some paths that I do not have access
                        for _ in list_dicts_rec:
                            _['DataDate'] = datetime.datetime.today().strftime('%Y%m%d')
                            _['UpdateTime'] = self.dt_day.strftime('%H:%M:%S')
                            _['AcctIDByMXZ'] = acctidbymxz
                            _['AcctType'] = accttype
                            _['DataFilePath'] = data_source_type
                        if list_dicts_rec:
                            col_manually_rawdata.insert_many(list_dicts_rec)
                    except FileNotFoundError:
                        print(f'No type {data_source_type} of file or directory: {fpath_relative}')
        print('Update raw data finished.')

    @run_every_30s
    def update_trddata_f(self):
        cursor_find = list(self.col_acctinfo.find({'DataDate': self.str_day, 'AcctType': 'f', 'RptMark': 1}))
        for _ in cursor_find:
            list_future_data_capital = []
            list_future_data_holding = []
            list_future_data_trdrec = []
            prdcode = _['PrdCode']
            acctidbymxz = _['AcctIDByMXZ']
            acctidbyowj = _['AcctIDByOuWangJiang4FTrd']
            trader = Trader(acctidbyowj)
            dict_res_capital = trader.query_capital()
            if dict_res_capital:
                dict_capital_to_be_update = dict_res_capital
                dict_capital_to_be_update['DataDate'] = self.str_day
                dict_capital_to_be_update['AcctIDByMXZ'] = acctidbymxz
                dict_capital_to_be_update['AcctIDByOWJ'] = acctidbyowj
                dict_capital_to_be_update['PrdCode'] = prdcode
                list_future_data_capital.append(dict_capital_to_be_update)
                self.db_trddata['future_api_capital'].delete_many(
                    {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz}
                )
                if list_future_data_capital:
                    self.db_trddata['future_api_capital'].insert_many(list_future_data_capital)

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

                self.db_trddata['future_api_holding'].delete_many({'DataDate': self.str_day,
                                                                   'AcctIDByMXZ': acctidbymxz})
                if list_future_data_holding:
                    self.db_trddata['future_api_holding'].insert_many(list_future_data_holding)

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
                self.db_trddata['future_api_trdrec'].delete_many(
                    {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz}
                )
                if list_future_data_trdrec:
                    self.db_trddata['future_api_trdrec'].insert_many(list_future_data_trdrec)
            # print(f'{acctidbymxz} update finished!')

    def update_close_from_wind(self):
        list_astock_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a001010100000000").Data[1]
        # list_bond_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a100000000000000").Data[1]
        list_futures_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010000000000").Data[1]

        list_etf_codes = ['000300.SH', '000016.SH', '000905.SH']  # index ETF that we use
        list_bond_codes = []
        list_repo_codes = []
        list_mmf_codes = []   # 也是固定一下价值
        list_codes = list_astock_codes + list_bond_codes + list_futures_codes + list_etf_codes \
            + list_repo_codes + list_mmf_codes
        err, close_data_from_wind = w.wss(list_codes, "sec_name, close", f"tradeDate={self.str_day};priceAdj=U;cycle=D", usedf=True)
        if err == 0:
            close_data_from_wind.rename(columns={'CLOSE': 'Close', 'SEC_NAME': 'Symbol'}, inplace=True)
            close_records = []
            for index, row in close_data_from_wind.iterrows():
                doc = dict(row)
                doc.update({'Wind_code': index, 'DataDate': self.str_day})
                close_records.append(doc)
            self.db_trddata['wind_close'].delete_many({'DataDate': self.str_day})
            self.db_trddata['wind_close'].insert_many(close_records)
        else:
            print(err, close_data_from_wind)
        return

    def update_order_last_from_wind(self, list_secid_query):
        # we do query only for securities in our account, secid should be type of wind
        # 根据我们已经有的成交搜索last price，或者看最近一分钟成交价？ --- 在list里
        start_time = (datetime.datetime.today()-datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        end_time = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        docs = []
        for secid in list_secid_query:
            err, last_from_wind = w.wst(secid, "last", start_time, end_time, usedf=True)
            # 经常莫名其妙报错...service connection failed，数据也可能错...
            if err == 0:
                last_from_wind.rename(columns={}, inplace=True)
                doc = dict(last_from_wind[-1])    # 需要 time, last. sec_name???
                # todo 网好了测试一下 wst...
                doc.update({'wind_code': secid})
                docs.append(doc)
            elif err != -40520004:  # service connection error
                # or pass; or verify the data is not in wind system
                print(err, last_from_wind)
        self.db_trddata['wind_last'].insert_many(docs)
        return

    @staticmethod
    def get_sectype_from_code(windcode):
        # todo simplify the cases!
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

    def update_fmtdata(self):

        # todo list: 1. 改close相关公式 2.简化函数（不必要的fields），去掉patch 3.添加新加入的 list_fields，改格式
        #  4.last price 记录  5.postdata + last算出 short（之前的算法改动）
        dict_wcode2close = self.db_trddata['wind_close'].find({'DataDate': self.str_day})

        # set_index: 将WindCode列设做index, to_dict之后是:{col:{index:value}}形式数据

        list_dicts_acctinfo = list(
            self.col_acctinfo.find({'DataDate': self.str_day, 'RptMark': 1}, {'_id': 0}))  # {'_id': 0}隐藏
        for dict_acctinfo in list_dicts_acctinfo:
            prdcode = dict_acctinfo['PrdCode']
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            if accttype in ['c', 'm', 'o']:
                patchmark = dict_acctinfo['PatchMark']
                # 1.整理holding
                # 1.1 rawdata(无融券合约账户)
                list_dicts_holding = list(self.db_trddata['manually_rawdata_holding'].find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                ))
                list_fields_secid = ['代码', '证券代码']
                list_fields_symbol = ['证券名称']
                list_fields_shareholder_acctid = ['股东帐户', '股东账号', '股东代码']
                list_fields_exchange = ['市场', '交易市场', '交易板块', '板块', '交易所', '交易所名称']
                # 有优先级别的列表
                list_fields_longqty = [
                    '股票余额', '拥股数量', '证券余额', '证券数量', '库存数量', '持仓数量', '参考持股', '持股数量', '当前持仓',
                    '当前余额', '实际数量', '实时余额'
                ]
                list_dicts_holding_fmtted = []
                for dict_holding in list_dicts_holding:  # 不必 list_dicts_holding.keys()
                    secid = None
                    secidsrc = None
                    symbol = None
                    longqty = 0
                    shortqty = 0
                    for field_secid in list_fields_secid:
                        if field_secid in dict_holding:
                            secid = str(dict_holding[field_secid])

                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_holding:
                            shareholder_acctid = str(dict_holding[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_holding:
                            exchange = dict_holding[field_exchange]
                            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                                      '00': 'SZSE', '10': 'SSE',
                                                      '0': 'SZSE', '1': 'SSE',
                                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                                      'SH': 'SSE', 'SZ': 'SZSE'
                                                      }
                            secidsrc = dict_exchange2secidsrc[exchange]
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_holding:
                            symbol = str(dict_holding[field_symbol])

                    for field_longqty in list_fields_longqty:
                        if field_longqty in dict_holding:
                            longqty = float(dict_holding[field_longqty])

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = self.get_sectype_from_code(windcode)
                    if sectype == 'IrrelevantItem':
                        continue
                    if windcode in dict_wcode2close:
                        close = dict_wcode2close[windcode]
                    else:
                        print(f'{windcode} not found in dict_wcode2close.')
                        close = 0
                    if close is None:
                        print(f'{windcode} not found in dict_wcode2close.')
                        close = 0
                    longamt = close * longqty
                    shortamt = 0
                    netamt = longamt - shortamt

                    if accttype in ['c', 'f', 'o']:
                        cash_from_ss_in_holding_fmtted = None
                    elif accttype in ['m']:
                        cash_from_ss_in_holding_fmtted = 0
                    else:
                        raise ValueError('Unknown accttype.')

                    dict_holding_fmtted = {
                        'DataDate': self.str_day,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'LongAmt': longamt,
                        'ShortAmt': shortamt,
                        'NetAmt': netamt,
                        'CashFromShortSelling': cash_from_ss_in_holding_fmtted,
                        'OTCContractUnitMarketValue': None,
                        'LiabilityType': None,
                        'Liability': 0,
                        'LiabilityQty': None,  # 融券数量
                        'LiabilityAmt': None,  # 融资数量
                        'InterestRate': None,
                        'DatedDate': None,
                        'UnderlyingSecurityID': None,
                        'UnderlyingSecurityIDSource': None,
                        'UnderlyingSecurityType': None,
                        'UnderlyingSymbol': None,
                        'UnderlyingQty': None,
                        'UnderlyingAmt': 0,
                        'UnderlyingClose': None,
                        'UnderlyingStartValue_vec': None,
                        'Note': None
                    }
                    list_dicts_holding_fmtted.append(dict_holding_fmtted)

                # 处理融券合约账户
                list_dicts_secliability = list(self.db_trddata['manually_rawdata_secliability'].find(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                ))
                shortqty_from_ss = 0  # 注： 为余额，是未偿还额
                shortqty_from_equity_compensation = 0  # 注： 是余额
                cash_from_ss_in_dict_secliability = 0
                list_fields_shortqty_from_ss = ['剩余数量']
                list_fields_shortqty_from_equity_compensation = ['权益补偿数量']  # 权益补偿数量，来自于股票分红，zhaos_tdx中该值为余额
                list_fields_ss_avgprice = ['卖均价']
                list_fields_cash_from_short_selling = ['融券卖出成本']  # CashFromShortSelling

                for dict_secliability in list_dicts_secliability:
                    secid = None
                    secidsrc = None
                    symbol = None
                    longqty = 0
                    for field_secid in list_fields_secid:
                        if field_secid in dict_secliability:
                            secid = str(dict_secliability[field_secid])

                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_secliability:
                            shareholder_acctid = dict_secliability[field_shareholder_acctid]
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_secliability:
                            exchange = dict_secliability[field_exchange]
                            dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE',
                                                      '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                                                      '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                                                      '00': 'SZSE', '10': 'SSE',
                                                      '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                                                      '上海A股': 'SSE', '深圳A股': 'SZSE',
                                                      }
                            secidsrc = dict_exchange2secidsrc[exchange]
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_secliability:
                            symbol = str(dict_secliability[field_symbol])

                    for field_shortqty_from_ss in list_fields_shortqty_from_ss:
                        if field_shortqty_from_ss in dict_secliability:
                            shortqty_from_ss = float(dict_secliability[field_shortqty_from_ss])

                    for field_shortqty_from_equity_compensation in list_fields_shortqty_from_equity_compensation:
                        if field_shortqty_from_equity_compensation in dict_secliability:
                            shortqty_from_equity_compensation = float(
                                dict_secliability[field_shortqty_from_equity_compensation])
                    shortqty = shortqty_from_ss + shortqty_from_equity_compensation

                    for field_ss_avgprice in list_fields_ss_avgprice:
                        if field_ss_avgprice in dict_secliability:
                            ss_avgprice = float(dict_secliability[field_ss_avgprice])
                            cash_from_ss_in_dict_secliability = shortqty_from_ss * ss_avgprice

                    for field_cash_from_short_selling in list_fields_cash_from_short_selling:
                        if field_cash_from_short_selling in dict_secliability:
                            cash_from_ss_in_dict_secliability = float(dict_secliability[field_cash_from_short_selling])

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = self.get_sectype_from_code(windcode)
                    if sectype == 'IrrelevantItem':
                        continue
                    close = dict_wcode2close[windcode]
                    longamt = close * longqty
                    shortamt = close * shortqty
                    netamt = longamt - shortamt
                    dict_secliability_fmtted = {
                        'DataDate': self.str_day,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'LongAmt': longamt,
                        'ShortAmt': shortamt,
                        'NetAmt': netamt,
                        'CashFromShortSelling': cash_from_ss_in_dict_secliability,
                        'OTCContractUnitMarketValue': None,
                        'LiabilityType': (lambda x: 'Securities Liability' if x > 0 else None)(shortamt),
                        'Liability': shortamt,
                        'LiabilityQty': (lambda x: x if x else None)(shortqty),  # 融券数量
                        'LiabilityAmt': None,  # 融资数量
                        'InterestRate': None,
                        'DatedDate': None,
                        'UnderlyingSecurityID': None,
                        'UnderlyingSecurityIDSource': None,
                        'UnderlyingSecurityType': None,
                        'UnderlyingSymbol': None,
                        'UnderlyingQty': None,
                        'UnderlyingAmt': 0,
                        'UnderlyingClose': None,
                        'UnderlyingStartValue_vec': None,
                        'Note': None
                    }
                    list_dicts_holding_fmtted.append(dict_secliability_fmtted)

                # 1.2 patchdata
                # patchdata 逻辑： 是增量补充，而不是余额覆盖
                #   1. 检验是否有patchmark，有则进入patch算法，无则跳过。
                #   2. 格式化patch数据
                #   3. 将 patch data添加至holding中
                #   3. 检查capital中各字段是否为NoneType，是则使用由holding_patchdata中推算出来的值，否则使用当前值（capital）。
                #   4. 将rawdata 与 patchdata 相加，得到patched data。

                list_dicts_holding_patchdata_fmtted = []

                underlying_net_exposure = 0
                cash_from_ss_in_patch_data = 0
                if patchmark:
                    list_dicts_holding_patchdata = list(self.db_trddata['manually_patchdata_holding'].find(
                        {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}
                    ))
                    # todo 需改进自定义标的持仓价格和持仓金额的情况（eg.场外非标, 下层基金）
                    for dict_holding_patchdata in list_dicts_holding_patchdata:
                        if 'CashFromShortSelling' in dict_holding_patchdata:
                            cash_from_ss_in_patch_data += dict_holding_patchdata['CashFromShortSelling']
                        else:
                            cash_from_ss_in_patch_data += 0
                        underlying_sectype = dict_holding_patchdata['UnderlyingSecurityType']
                        if underlying_sectype in ['CS', 'ETF', 'INDEX']:
                            underlying_net_exposure_delta = dict_holding_patchdata['UnderlyingAmt']
                            underlying_net_exposure += underlying_net_exposure_delta
                        if underlying_sectype in ['Index Future']:
                            underlying_secid = dict_holding_patchdata['UnderlyingSecurityID']
                            underlying_index_future = underlying_secid[:2]
                            dict_index_future2index_spot_wcode = {'IC': '000905.SH', 'IF': '000300.SH',
                                                                  'IH': '000016.SH'}
                            close = dict_wcode2close[dict_index_future2index_spot_wcode[underlying_index_future]]
                            underlyingqty = dict_holding_patchdata['UnderlyingQty']
                            dict_index_future2multiplier = {'IC': 200, 'IF': 300, 'IH': 300}
                            multiplier = dict_index_future2multiplier[underlying_index_future]
                            underlying_net_exposure_delta = underlyingqty * close * multiplier
                            underlying_net_exposure += underlying_net_exposure_delta
                        list_dicts_holding_patchdata_fmtted.append(dict_holding_patchdata)
                list_dicts_holding_fmtted_patched = list_dicts_holding_fmtted + list_dicts_holding_patchdata_fmtted
                for dict_holding_fmtted_patched in list_dicts_holding_fmtted_patched:
                    prdcode = dict_holding_fmtted_patched['AcctIDByMXZ'].split('_')[0]
                    dict_holding_fmtted_patched['PrdCode'] = prdcode
                self.db_trddata['formatted_holding'].delete_many(
                    {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                if list_dicts_holding_fmtted_patched:
                    self.db_trddata['formatted_holding'].insert_many(list_dicts_holding_fmtted_patched)

                underlying_exposure_long = 0
                underlying_exposure_short = 0
                if underlying_net_exposure >= 0:
                    underlying_exposure_long = underlying_net_exposure
                else:
                    underlying_exposure_short = - underlying_net_exposure

                # 2.整理b/s: 出于稳健性考量，股票市值由持仓数据计算得出: holding data + raw data + patch data
                # 用现金代替可用，重点计算
                # Cash, CashEquivalent, CompositeLongAmt, Asset, ShortAmt, Liability, ApproximateNetAsset
                # patch data采用余额覆盖模式。（holding data采用增量添加模式）

                # 2.1 holding data
                stock_longamt = 0
                etf_longamt = 0
                ce_longamt = 0
                swap_longamt = 0
                stock_shortamt = 0
                etf_shortamt = 0
                flt_capital_debt = 0

                if accttype in ['c', 'f', 'o']:
                    cash_from_ss_by_acctidbymxz = None
                elif accttype in ['m']:
                    cash_from_ss_by_acctidbymxz = 0
                    for dict_holding_fmtted_patched in list_dicts_holding_fmtted_patched:
                        cash_from_ss_by_acctidbymxz += dict_holding_fmtted_patched['CashFromShortSelling']
                else:
                    raise ValueError('Unknown accttype.')

                df_holding_fmtted_patched = pd.DataFrame(list_dicts_holding_fmtted_patched)
                if df_holding_fmtted_patched.empty:
                    df_holding_fmtted_patched = pd.DataFrame(
                        columns=['DataDate', 'AcctIDByMXZ', 'CashFromSS', 'SecurityID', 'SecurityType',
                                 'Symbol', 'SecurityIDSource',
                                 'LongQty', 'ShortQty', 'LongAmt', 'ShortAmt', 'NetAmt', 'CashFromShortSelling',
                                 'OTCContractUnitMarketValue', 'LiabilityType', 'Liability', 'LiabilityQty',
                                 'LiabilityAmt',
                                 'InterestRate', 'DatedDate', 'UnderlyingSecurityID', 'UnderlyingSecurityIDSource',
                                 'UnderlyingSecurityType', 'UnderlyingSymbol', 'UnderlyingQty', 'UnderlyingAmt',
                                 'UnderlyingClose', 'UnderlyingStartValue_vec', 'Note'])
                else:
                    df_holding_amt_sum_by_sectype = df_holding_fmtted_patched.groupby(by='SecurityType').sum()
                    dict_amt2dict_sectype2amt = df_holding_amt_sum_by_sectype.to_dict()
                    if 'LongAmt' in dict_amt2dict_sectype2amt:
                        dict_sectype2longamt = dict_amt2dict_sectype2amt['LongAmt']
                        if 'CS' in dict_sectype2longamt:
                            stock_longamt = dict_sectype2longamt['CS']
                        if 'ETF' in dict_sectype2longamt:
                            etf_longamt = dict_sectype2longamt['ETF']
                        if 'CE' in dict_sectype2longamt:
                            ce_longamt = dict_sectype2longamt['CE']
                        if 'SWAP' in dict_sectype2longamt:
                            swap_longamt = dict_sectype2longamt['SWAP']
                    if 'ShortAmt' in dict_amt2dict_sectype2amt:
                        dict_sectype2shortamt = dict_amt2dict_sectype2amt['ShortAmt']
                        if 'CS' in dict_sectype2shortamt:
                            stock_shortamt = dict_sectype2shortamt['CS']
                        if 'ETF' in dict_sectype2shortamt:
                            etf_shortamt = dict_sectype2shortamt['ETF']

                # 2.2 求cash
                dict_capital = self.db_trddata['manually_rawdata_capital'].find_one(
                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                )
                if dict_capital is None:
                    dict_capital = {}
                list_fields_af = ['可用', '可用数', '现金资产', '可用金额', '资金可用金', '可用余额', 'T+1指令可用金额']
                list_fields_ttasset = ['总资产', '资产', '总 资 产', '单元总资产', '账户总资产', '担保资产']
                flt_ttasset = None
                flt_cash = None

                # 分两种情况： 1. cash acct: 至少要有cash 2. margin acct: 至少要有ttasset
                if accttype in ['c']:
                    for field_af in list_fields_af:
                        if field_af in dict_capital:
                            flt_cash = float(dict_capital[field_af])
                        else:
                            if patchmark:
                                dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                                    {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                                ))
                                if dict_patchdata_capital:
                                    if 'Cash' in dict_patchdata_capital:
                                        flt_cash = dict_patchdata_capital['Cash']
                                    if 'CapitalDebt' in dict_patchdata_capital:
                                        flt_capital_debt = dict_patchdata_capital['CapitalDebt']

                elif accttype == 'm':
                    for field_ttasset in list_fields_ttasset:
                        if field_ttasset in dict_capital:
                            flt_ttasset = float(dict_capital[field_ttasset])
                    if patchmark:
                        dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                            {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                        ))
                        if dict_patchdata_capital:
                            if 'TotalAsset' in dict_patchdata_capital:
                                if dict_patchdata_capital['TotalAsset']:
                                    flt_ttasset = dict_patchdata_capital['TotalAsset']
                            if 'CapitalDebt' in dict_patchdata_capital:
                                flt_capital_debt = dict_patchdata_capital['CapitalDebt']

                    flt_cash = flt_ttasset - stock_longamt - etf_longamt - ce_longamt

                elif accttype == 'o':
                    dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                        {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                    ))
                    if dict_patchdata_capital:
                        if 'Cash' in dict_patchdata_capital:
                            flt_cash = dict_patchdata_capital['Cash']
                        if 'CapitalDebt' in dict_patchdata_capital:
                            flt_capital_debt = dict_patchdata_capital['CapitalDebt']
                else:
                    raise ValueError('Unknown accttype')

                # 2.3 cash equivalent: ce_longamt
                flt_ce = ce_longamt

                # 2.4 etf
                flt_etf_long_amt = etf_longamt

                # 2.4 CompositeLongAmt
                flt_composite_long_amt = stock_longamt

                # 2.5 SwapAmt
                # 更新：大于0的进交易性金融资产，为资产端项目；小于0的进交易性金融负债，为负债端项目
                flt_swap_amt2asset = 0
                if swap_longamt > 0:
                    flt_swap_amt2asset = swap_longamt
                flt_swap_amt2liability = 0
                if swap_longamt < 0:
                    flt_swap_amt2liability = abs(swap_longamt)

                # 2.5 Asset
                flt_ttasset = flt_cash + flt_ce + flt_etf_long_amt + flt_composite_long_amt + flt_swap_amt2asset

                # 2.6 etf_shortamt
                flt_etf_short_amt = etf_shortamt

                # 2.7 stock_shortamt
                flt_composite_short_amt = stock_shortamt

                # 2.8 liability
                # liability = 融券负债（利息+本金）+ 融资负债（利息+本金）+ 场外合约形成的负债（交易性金融负债）
                if flt_capital_debt is None:
                    flt_capital_debt = 0
                flt_liability = (
                        float(df_holding_fmtted_patched['Liability'].sum()) + flt_capital_debt + flt_swap_amt2liability
                )

                # 2.9 net_asset
                flt_approximate_na = flt_ttasset - flt_liability

                # 2.10 读取patchdata对rawdata进行修正: 余额覆盖模式
                if patchmark:
                    dict_patchdata_capital = (self.db_trddata['manually_patchdata_capital'].find_one(
                        {'AcctIDByMXZ': acctidbymxz, 'DataDate': self.str_day}, {'_id': 0}
                    ))
                    if dict_patchdata_capital:
                        if 'Cash' in dict_patchdata_capital:
                            if dict_patchdata_capital['Cash']:
                                flt_cash = float(dict_patchdata_capital['Cash'])
                        if 'CashEquivalent' in dict_patchdata_capital:
                            if dict_patchdata_capital['CashEquivalent']:
                                flt_ce = float(dict_patchdata_capital['CashEquivalent'])
                        if 'ETFLongAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['ETFLongAmt']:
                                flt_etf_long_amt = float(dict_patchdata_capital['ETFLongAmt'])
                        if 'CompositeLongAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['CompositeLongAmt']:
                                flt_composite_long_amt = float(dict_patchdata_capital['CompositeLongAmt'])
                        if 'TotalAsset' in dict_patchdata_capital:
                            if dict_patchdata_capital['TotalAsset']:
                                flt_ttasset = float(dict_patchdata_capital['TotalAsset'])
                        if 'ETFShortAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['ETFShortAmt']:
                                flt_etf_short_amt = float(dict_patchdata_capital['ETFShortAmt'])
                        if 'CompositeShortAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['CompositeShortAmt']:
                                flt_composite_short_amt = float(dict_patchdata_capital['CompositeShortAmt'])
                        if 'Liability' in dict_patchdata_capital:
                            if dict_patchdata_capital['Liability']:
                                flt_liability = float(dict_patchdata_capital['Liability'])
                        if 'ApproximateNetAmt' in dict_patchdata_capital:
                            if dict_patchdata_capital['ApproximateNetAmt']:
                                flt_approximate_na = float(dict_patchdata_capital['ApproximateNetAmt'])
                dict_bs = {
                    'DataDate': self.str_day,
                    'PrdCode': prdcode,
                    'AcctIDByMXZ': acctidbymxz,
                    'AcctType': accttype,
                    'Cash': flt_cash,
                    'CashEquivalent': flt_ce,
                    'CashFromShortSelling': cash_from_ss_by_acctidbymxz,
                    'ETFLongAmt': flt_etf_long_amt,
                    'CompositeLongAmt': flt_composite_long_amt,
                    'AssetFromSwap': flt_swap_amt2asset,
                    'TotalAsset': flt_ttasset,
                    'CapitalDebt': flt_capital_debt,
                    'ETFShortAmt': flt_etf_short_amt,
                    'CompositeShortAmt': flt_composite_short_amt,
                    'LiabilityFromSwap': flt_swap_amt2liability,
                    'Liability': flt_liability,
                    'ApproximateNetAsset': flt_approximate_na,
                }
                self.db_trddata['b/s_by_acctidbymxz'].delete_many({'DataDate': self.str_day,
                                                                   'AcctIDByMXZ': acctidbymxz})
                list_dict_bs = [dict_bs]
                if dict_bs:
                    self.db_trddata['b/s_by_acctidbymxz'].insert_many(list_dict_bs)

                exposure_long_amt = flt_etf_long_amt + flt_composite_long_amt + underlying_exposure_long
                exposure_short_amt = flt_etf_short_amt + flt_composite_short_amt + underlying_exposure_short
                exposure_net_amt = exposure_long_amt - exposure_short_amt

            elif accttype in ['f']:
                # 按acctidbymxz exposure数据
                list_dicts_holding_future = list(
                    self.db_trddata['future_api_holding'].find({'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                )
                list_dicts_holding_future_exposure_draft = []
                for dict_holding_future in list_dicts_holding_future:
                    secid = dict_holding_future['instrument_id']
                    secid_first_part = secid[:-4]
                    dict_future2spot_windcode = {'IC': '000905.SH', 'IH': '000016.SH', 'IF': '000300.SH'}
                    windcode = dict_future2spot_windcode[secid_first_part]
                    close = dict_wcode2close[windcode]  # spot close
                    qty = dict_holding_future['position']
                    direction = dict_holding_future['direction']
                    future_long_qty = 0
                    future_short_qty = 0
                    future_long_amt = 0
                    future_short_amt = 0

                    if direction == 'buy':
                        future_long_qty = qty
                        future_long_amt = close * future_long_qty * self.dict_future2multiplier[secid_first_part]
                    elif direction == 'sell':
                        future_short_qty = qty
                        future_short_amt = close * future_short_qty * self.dict_future2multiplier[secid_first_part]
                    else:
                        raise ValueError('Unknown direction in future respond.')
                    future_net_qty = future_long_qty - future_short_qty
                    future_net_amt = future_long_amt - future_short_amt
                    dict_holding_future_exposure_draft = {
                        'SecurityID': secid,
                        'direction': direction,
                        'position': qty,
                        'LongQty': future_long_qty,
                        'ShortQty': future_short_qty,
                        'NetQty': future_net_qty,
                        'LongAmt': future_long_amt,
                        'ShortAmt': future_short_amt,
                        'NetAmt': future_net_amt,
                    }
                    list_dicts_holding_future_exposure_draft.append(dict_holding_future_exposure_draft)
                if list_dicts_holding_future_exposure_draft:
                    """一个账户的全部品种风险暴露（对IC提供的还是IH提供的未作区分）"""
                    df_holding_future_exposure_draft = pd.DataFrame(list_dicts_holding_future_exposure_draft)
                    exposure_long_amt = float(df_holding_future_exposure_draft['LongAmt'].sum())
                    exposure_short_amt = float(df_holding_future_exposure_draft['ShortAmt'].sum())
                    exposure_net_amt = exposure_long_amt - exposure_short_amt
                else:
                    exposure_long_amt = 0
                    exposure_short_amt = 0
                    exposure_net_amt = 0
                dict_capital_future = (
                    self.db_trddata['future_api_capital'].find_one(
                        {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz}
                    )
                )
                flt_approximate_na = dict_capital_future['DYNAMICBALANCE']
                dict_future_bs = {
                    'DataDate': self.str_day,
                    'AcctIDByMXZ': acctidbymxz,
                    'AcctType': 'f',
                    'ApproximateNetAsset': flt_approximate_na,
                    'Cash': flt_approximate_na,
                    'CashEquivalent': 0,
                    'CompositeLongAmt': 0,
                    'CompositeShortAmt': 0,
                    'ETFLongAmt': 0,
                    'ETFShortAmt': 0,
                    'Liability': 0,
                    'PrdCode': prdcode,
                    'SwapLongAmt': 0,
                    'TotalAsset': flt_approximate_na,
                }
                self.db_trddata['b/s_by_acctidbymxz'].delete_many(
                    {'DataDate': self.str_day, 'AcctIDByMXZ': acctidbymxz})
                if dict_future_bs:
                    self.db_trddata['b/s_by_acctidbymxz'].insert_one(dict_future_bs)

            else:
                raise ValueError('Unknown account type in basic account info.')

            dict_exposure_analysis = {
                'DataDate': self.str_day,
                'AcctIDByMXZ': acctidbymxz,
                'PrdCode': prdcode,
                'LongExposure': exposure_long_amt,
                'ShortExposure': exposure_short_amt,
                'NetExposure': exposure_net_amt,
                'ApproximateNetAsset': flt_approximate_na,
            }
            self.db_trddata['exposure_analysis_by_acctidbymxz'].delete_many({'DataDate': self.str_day,
                                                                             'AcctIDByMXZ': acctidbymxz})
            if dict_exposure_analysis:
                self.db_trddata['exposure_analysis_by_acctidbymxz'].insert_one(dict_exposure_analysis)
        print('Update capital and holding formatted by internal style finished.')
        return

    def run(self):
        update_raw_thread = threading.Thread(target=self.update_rawdata, args=())
        update_future_thread = threading.Thread(target=self.update_trddata_f)
        # 分先后顺序
        update_raw_thread.start()
        update_future_thread.start()

        while True:
            command = input("输入命令 pause/run/stop 控制进程, 按Enter再次换出：")
            if command == "run":
                print('Calling functions...')
                self.resume()

            elif command == "pause":
                self.pause()

            elif command == "stop":
                self.running = False
                break
        print('Program stopped!')

    def pause(self):
        self.event.clear()

    def resume(self):
        self.event.set()


if __name__ == '__main__':
    # run这个文件时才会调用，import 它时则不会调用
    # 使用的test方式！

    # for col in test.db_trddata.list_collection_names():
    #     test.db_trddata.drop_collection(col)
    test = ExposMonit()
    test.run()
    # test.upload_basic_info()
    # print('basic info uploaded!')
    # test.update_rawdata()
    # test.update_trddata_f()
