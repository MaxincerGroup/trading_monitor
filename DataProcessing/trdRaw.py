# import numpy as np
import sys
import pandas as pd
import os
import time
sys.path.append(".\\Trader")
from Trader.trader_v1 import Trader


class GenTrdRaw:
    def __init__(self, info_path, server):
        self.server = server
        self.info_path = info_path

        self.fund_docs = []
        self.holding_docs = []
        self.order_docs = []

    @classmethod
    def deco_insert(cls, processing_func):  # use cls in order to be inherited
        def decorated(self, *args, **kwargs):   # decorated function can add self in the function
            processing_func(self, *args, **kwargs)
            self.server.insert('trading_rawdata_fund', self.fund_docs)
            self.server.insert('trading_rawdata_holding', self.holding_docs)
            self.server.insert('trading_rawdata_order', self.order_docs)
            return
        return decorated

    def get_from_fund(self, fields):
        return self.server.get_fileds_and_updatetime('trading_rawdata_fund', fields)

    def get_from_holding(self, fields):
        return self.server.get_fileds_and_updatetime('trading_rawdata_holding', fields)

    def get_from_order(self, fields):
        return self.server.get_fileds_and_updatetime('trading_rawdata_order', fields)


# CTP API github上有：直接和期货交易商通信=交易所通讯规则数据库链接的
# 之后加

class TraderApi(GenTrdRaw):     # 已经是2手的
    def __init__(self, info_path, server):
        super(TraderApi, self).__init__(info_path, server)
        trader_api_dict = {}

        acctid_map = {}
        df = pd.read_excel(info_path+'/basic_info.xlsx', index_col=False)
        for i, row in df.iterrows():
            if row['AcctType'] == 'f' and row['DataDownloadMark'] == 1:
                value = str(row['AcctIDByMXZ'])
                id_key = row['AcctIDByOuWangJiang4FTrd']
                acctid_map.update({id_key: value})
                trader_api_dict.update({id_key: Trader(id_key)})
        # print(broker_map)
        self.acctid_map = acctid_map
        self.trader_api_dict = trader_api_dict
        return

    @staticmethod
    def trader2dicts(trader, add_info):

        capital_dict = trader.query_capital()
        capital_dict.update(add_info)

        list_holding_dict = []
        list_order_dict = []

        holding_keys = [
            'exchange', 'instrument_id', 'direction', 'hedge', 'position', 'position_td', 'open_volume',
            'close_volume', 'unknown1', 'unknown2', 'unknown3'
        ]   # position 数量
        trdrecs_keys = ['instrument_id', 'direction', 'offset', 'volume', 'price', 'time', 'trader']
        list_list_holding = trader.query_holding()
        if list_list_holding:
            for list_holding in list_list_holding:
                tmp_doc = dict(zip(holding_keys, list_holding))
                # print(tmp_doc)
                tmp_doc.update(add_info)
                list_holding_dict.append(tmp_doc)
        list_list_order = trader.query_trdrecs()
        if list_list_order:
            for list_order in list_list_order:
                tmp_doc = dict(zip(trdrecs_keys, list_order))
                tmp_doc.update(add_info)
                list_order_dict.append(tmp_doc)
        return capital_dict, list_holding_dict, list_order_dict

    @GenTrdRaw.deco_insert
    def insert(self):
        for key in self.acctid_map.keys():  # all AcctIDByOWJ
            date = time.strftime('%Y%m%d', time.localtime())
            hour = time.strftime('%H:%M:%S', time.localtime())
            info = {'AcctIDByMXZ': self.acctid_map[key], 'DataDate': date, 'UpdateTime': hour}

            capital_dict, holding_dict, order_dict = self.trader2dicts(self.trader_api_dict[key], info)
            if capital_dict:
                self.fund_docs.append(capital_dict)
            if holding_dict:
                self.holding_docs = self.holding_docs + holding_dict
            if order_dict:
                self.order_docs = self.order_docs + order_dict
        return


class CSV(GenTrdRaw):
    def __init__(self, file_path, info_path, server):
        """
        :param server: a Server class which is pymongo server
        :param file_path:
        :param AcctIDByMXZ: = '929_c_zx_6218' for PrdCode = 929
        """
        super(CSV, self).__init__(info_path, server)
        self.path = file_path
        self.file_list = os.listdir(file_path)
        broker_map = {}
        df = pd.read_excel(info_path+'/basic_info.xlsx', index_col=False)
        for i, row in df.iterrows():
            value = dict(row[['AcctIDByMXZ', 'DataDownloadMark']])
            broker_map.update({row['AcctIDByBroker']: value})  # AcctIDByMXZ
        # print(broker_map)
        self.broker_map = broker_map
        return

    def getAcctIdBy(self, brokerid):
        """
        :param brokerid: 查表得到 brokerid对应的 AcctIDByMXZ 以及 DataDownloadMark
        如果能找到brokerid 且 Mark=1，则上传新的doc (AcctIDByMXZ != 0)
        :return: AcctIDByMXZ
        """
        if self.broker_map.__contains__(brokerid) and self.broker_map[brokerid]['DataDownloadMark'] == 1:
            AcctIDByMXZ = self.broker_map[brokerid]['AcctIDByMXZ']
        else:
            AcctIDByMXZ = 0
        return AcctIDByMXZ

    def csv2dicts(self, file_name):
        """
        :param file_name: csv file name
        :return: a list of dictionaries to upload
        """
        date = time.strftime("%Y%m%d", time.localtime())  # date = self.dateformat(file_name)
        file_name = self.path + '/' + file_name
        documents = []
        with open(file_name, 'r', encoding='utf-8-sig') as f:
            # utf-8 才可解码， sig可以去掉表头的-sig \ufeff
            i = 0
            for line in f.read().splitlines():   # splitlines()来去掉\n
                if i == 0:
                    head = line.split(',')
                    col_ind = head.index('账户')
                else:
                    line = line.split(',')
                    acct_id = self.getAcctIdBy(line[col_ind])
                    if acct_id == 0:
                        continue
                    hour = time.strftime('%H:%M:%S', time.localtime())
                    doc = {'AcctIDByMXZ': acct_id, 'DataDate': date, 'UpdateTime': hour}
                    if len(head) != len(line):
                        print(i, file_name)
                    for j in range(len(head)):
                        doc.update({head[j]: line[j]})
                    documents.append(doc)
                i += 1
        return documents

    @GenTrdRaw.deco_insert
    def insert(self, fund_name_rules, hold_name_rules, order_name_rules):
        """
        :param XXX_name_rules: the critical words in the filename to which we refer ['fund','hold','order']
        :return: None; we only upload
        """
        def process_docs(name_rules):
            docs = []
            for file in self.file_list:
                if ".csv" == file[-4:]:
                    if isinstance(name_rules, list):
                        for name_rule in name_rules:    # may be many rules
                            if name_rule in file:
                                docs = docs+self.csv2dicts(file)
                                break
                    else:
                        if name_rules in file:
                            docs = docs + self.csv2dicts(file)
            return docs

        self.fund_docs = process_docs(fund_name_rules)
        self.holding_docs = process_docs(hold_name_rules)
        self.order_docs = process_docs(order_name_rules)
        return





