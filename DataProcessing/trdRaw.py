import pymongo
# import numpy as np
import pandas as pd
import os


class LocalServer: # can create some parent class like Server
    def __init__(self, dbname, host='localhost', port=27017):
        """
        :param dbname: database's name
        """
        self.host = host
        self.port = port
        self.client = pymongo.MongoClient(port=port, host=host)
        self.db = self.client[dbname]

    def insert(self, col_name, documents):
        """
        :param col_name: collection name; dict_lists: a list of dictionaries
        :return: None, upload dictionaries into the database
        """
        if len(documents) > 1:
            self.db[col_name].insert_many(documents)
        else:
            self.db[col_name].insert_one(documents)
        return

    def drop_all(self):
        for col in self.db.collection_names():
            self.db[col].drop()


class CSV:
    def __init__(self, file_path, AcctIDByMXZ, dateformat):
        """
        :param file_path:
        :param AcctIDByMXZ: = '929_c_zx_6218' for PrdCode = 929
        :param dateformat: a function that extracts date from filename
        """
        self.path = file_path
        self.file_list = os.listdir(file_path)
        self.dateformat = dateformat
        self.acctID = AcctIDByMXZ
        return

    def csv2dicts(self, file_name):
        """
        :param file_name: csv file name
        :return: a list of dictionaries to upload
        """
        date = self.dateformat(file_name)
        file_name = self.path + '/' + file_name
        df = pd.read_csv(file_name)
        documents = []
        info = {'DataDate': date, 'AcctIDByMXZ': self.acctID}
        for i, row in df.iterrows():
            r = dict(row)
            r.update(info)  # this return none
            documents.append(r)
        return documents

    def insert(self, server, name_rules):
        """
        :param server: a Server class which is already connected
        :param name_rules: the critical words in the filename by which we can understand ['fund','hold','entrust']
        :return: None; we only upload
        """
        fund_docs = []
        hold_docs = []
        entrust_docs = []
        fund_name, hold_name, entrust_name = name_rules[0:3]
        for file in self.file_list:
            if ".csv" in file:
                if fund_name in file:
                    fund_docs = fund_docs+self.csv2dicts(file)
                elif hold_name in file:
                    hold_docs = hold_docs + self.csv2dicts(file)
                elif entrust_name in file:
                    entrust_docs = entrust_docs + self.csv2dicts(file)

        server.insert('trading_rawdata_fund', fund_docs)
        server.insert('trading_rawdata_holding', hold_docs)
        server.insert('trading_rawdata_entrust', entrust_docs)
        return


