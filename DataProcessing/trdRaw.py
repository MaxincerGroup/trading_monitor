# import numpy as np
import pandas as pd
import os
import time


class CSV:
    def __init__(self, file_path, info_path, server):
        """
        :param server: a Server class which is pymongo server
        :param file_path:
        :param AcctIDByMXZ: = '929_c_zx_6218' for PrdCode = 929
        """
        self.path = file_path
        self.file_list = os.listdir(file_path)
        broker_map = {}
        df = pd.read_excel(info_path+'/basic_info.xlsx', index_col=False)
        for i, row in df.iterrows():
            value = dict(row[['AcctIDByMXZ', 'DataDownloadMark']])
            broker_map.update({row['AcctIDByBroker']: value})  # AcctIDByMXZ
        # print(broker_map)
        self.broker_map = broker_map
        self.server = server
        return

    def getAcctIdBy(self, acctid):

        if self.broker_map.__contains__(acctid) and self.broker_map[acctid]['DataDownloadMark'] == 1:
            AcctIDByMXZ = self.broker_map[acctid]['AcctIDByMXZ']
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
                    doc = {'DataDate': date, 'AcctIDByMXZ': acct_id}
                    if len(head) != len(line):
                        print(i, file_name)
                    for j in range(len(head)):
                        doc.update({head[j]: line[j]})
                    documents.append(doc)
                i += 1
        return documents

    def insert(self, name_rules, collection_name):
        """
        :param name_rules: the critical words in the filename to which we refer ['fund','hold','entrust']
        :return: None; we only upload
        """
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
        self.server.insert(collection_name, docs)
        return

    def insert_fund(self, name_rules):
        self.insert(name_rules, 'trading_rawdata_fund')

    def insert_holding(self, name_rules):
        self.insert(name_rules, 'trading_rawdata_holding')

    def insert_entrust(self, name_rules):
        self.insert(name_rules, 'trading_rawdata_entrust')



