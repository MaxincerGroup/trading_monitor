# -*- coding:utf-8 -*-
# cash: NetAsset,
# margin: 优先：NetAsset, (没有再自己算：总资产-总负债)
# future: 动态权益（净资产），估值表上列资产时算了整个合约的市值，而不是保证金（加杠杆），期货当日负债为0； 保证金=所有现金；保证金占用=加杠杆的
# otc:
# 总资产会有歧义：可能是动态权益，也可能总市值，按理说应该放在负债里（超出保证金部分）
#
# 如果做全：
# 需要资金调度：可取资金（可提出的现金，T+1清算，交易的钱还没算），可用资金（可用于交易），（不仅仅净资产）

import pymongo
import threading
import pandas as pd
import datetime
import time
import functools
import logging

logger_na = logging.getLogger()
fh = logging.FileHandler('data/log/net_asset.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - line:%(lineno)d - %(levelname)s: %(message)s'))
logger_na.addHandler(fh)

client_local_main = pymongo.MongoClient(port=27017, host='localhost',
                                        username='admin', password='Ms123456')
col_global_var = client_local_main['global_var']['exposure_monitoring']


def run_process(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        while True:
            self.lock.acquire()
            func(self, *args, **kwargs)
            self.lock.release()
            print(func.__name__, 'has finished')
            time.sleep(60)
    return wrapper


class NetAsset:
    def __init__(self):
        self.str_day = datetime.datetime.today().strftime('%Y%m%d')
        self.record_position_time = None  # '155808'
        self.record_fmt_time = None     # '155538'

        self.db_trddata = client_local_main['trade_data']
        self.col_acctinfo = client_local_main['basic_info']['acctinfo']
        self.event = threading.Event()
        self.lock = threading.Lock()

    @run_process
    def balance_sheet(self):
        self.record_fmt_time = col_global_var.find_one({'DataDate': self.str_day})['FmtUpdateTime']
        print(self.record_fmt_time)
        self.record_position_time = col_global_var.find_one({'DataDate': self.str_day})['PositionUpdateTime']
        list_dicts_acctinfo = list(self.col_acctinfo.find({'DataDate': self.str_day, 'DataDownloadMark': '1'}))
        list_dicts_fund = list(
            self.db_trddata['trade_fmtdata_fund'].find({'DataDate': self.str_day, 'UpdateTime': self.record_fmt_time}))
        list_dicts_position = list(
            self.db_trddata['trade_position'].find({'DataDate': self.str_day, 'UpdateTime': self.record_position_time}))
        dict_acctid2list_position = {}
        dict_acctid2fund = {}
        for _ in list_dicts_position:
            acctidbymxz = _['AcctIDByMXZ']
            if acctidbymxz in dict_acctid2list_position:
                dict_acctid2list_position[acctidbymxz].append(_)
            else:
                dict_acctid2list_position[acctidbymxz] = [_]

        for _ in list_dicts_fund:
            acctidbymxz = _['AcctIDByMXZ']
            dict_acctid2fund[acctidbymxz] = _   # fund只有一个

        list_dicts_acct_balance_sheet = []
        dict_prdcode2bs = {}
        for dict_acctinfo in list_dicts_acctinfo:
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            prdcode = dict_acctinfo['PrdCode']
            mdm = dict_acctinfo['MonitorDisplayMark']
            if acctidbymxz in dict_acctid2fund:    # 期货的总资产和总负债不好算，用LongAmt等更好（直接抄exposue，这里不抄）
                ttasset = dict_acctid2fund[acctidbymxz]['TotalAsset']
                netasset = dict_acctid2fund[acctidbymxz]['NetAsset']
                avlfund = dict_acctid2fund[acctidbymxz]['AvailableFund']
                cash = dict_acctid2fund[acctidbymxz]['Cash']
                kqzj = dict_acctid2fund[acctidbymxz]['KQZJ']
            else:
                continue
            if dict_acctinfo['MonitorExposureAnalysisMark'] != '0':
                flag_na = netasset is None
                flag_cash = cash is None   # m户: avlfund可能为 None; c户 avlfund也是None
                flag_ta = ttasset is None
                assert(not(flag_na and flag_ta))  # 净资产总资产必有一个
                if flag_ta:
                    ttasset = netasset
                if flag_na:
                    netasset = ttasset
                if flag_cash:
                    if flag_ta:
                        cash = netasset
                    else:
                        cash = ttasset
                if acctidbymxz in dict_acctid2list_position:
                    for dict_position in dict_acctid2list_position[acctidbymxz]:
                        if dict_position['SecurityType'] != 'IrrelevantItem':
                            if flag_ta:
                                ttasset += dict_position['ShortAmt']  # 总资产 = 净资产+ 总负债（约为short）
                            if flag_na:
                                netasset -= dict_position['ShortAmt']
                            if flag_cash:
                                if flag_na:
                                    cash -= dict_position['NetAmt']   # 总资产 - 总市值 （约为NetAmt)
                                if flag_ta:
                                    cash -= dict_position['LongAmt']  # cash = 净资产 - （总市值 - 总负债） （约为LongAmt）

                if flag_cash and '_c_' in acctidbymxz:
                    avlfund = cash

                acct_fund_dict = {'AcctIDByMXZ': acctidbymxz, 'PrdCode': prdcode, 'MonitorDisplayMark': mdm,
                                  'UpdateTime': self.record_position_time, 'DataDate': self.str_day,
                                  'AvailableFund': avlfund, 'Cash': cash, 'KQZJ': kqzj, 'NetAsset': netasset}
                list_dicts_acct_balance_sheet.append(acct_fund_dict)

                if not (prdcode in dict_prdcode2bs):
                    prdcode_exposure_dict = acct_fund_dict.copy()
                    del prdcode_exposure_dict['AcctIDByMXZ']
                    dict_prdcode2bs[prdcode] = prdcode_exposure_dict
                else:
                    # 4舍5入保留两位小数， todo 在flask展示里而不是在数据库里保留2位
                    for key in ['Cash', 'NetAsset', 'KQZJ', 'AvailableFund']:
                        try:
                            dict_prdcode2bs[prdcode][key] += acct_fund_dict[key]
                            acct_fund_dict[key] = round(acct_fund_dict[key], 2)
                        except TypeError:   # unsupported operand type(s) for += : 'NoneType' and 'float'
                            dict_prdcode2bs[prdcode][key] = None

        for prdcode in dict_prdcode2bs:
            for key in ['Cash', 'NetAsset', 'KQZJ', 'AvailableFund']:
                if dict_prdcode2bs[prdcode][key]:
                    dict_prdcode2bs[prdcode][key] = round(dict_prdcode2bs[prdcode][key], 2)

        list_dict_prdcode_bs = list(dict_prdcode2bs.values())

        # print(pd.DataFrame(list_dict_acct_exposure))
        # logger_na.info(pd.DataFrame(list_dict_prdcode_bs))
        if list_dicts_acct_balance_sheet:
            self.db_trddata['trade_balance_sheet_by_acctid'].delete_many(
                {'DataDate': self.str_day, 'UpdateTime': self.record_position_time})
            self.db_trddata['trade_balance_sheet_by_acctid'].insert_many(list_dicts_acct_balance_sheet)
            self.db_trddata['trade_balance_sheet_by_prdcode'].insert_many(list_dict_prdcode_bs)
        return

    def run(self):

        thread_bs = threading.Thread(target=self.balance_sheet)
        thread_bs.start()


if __name__ == '__main__':
    # ini_time_records(True)
    bs = NetAsset()
    bs.run()
