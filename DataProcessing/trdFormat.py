import time
import warnings


class Fund:
    def __init__(self, server, af_names, cash_names, ta_names, tmv_names, cb_names):
        self.config = {'m': {'AvailableFund': af_names,
                             'Cash': cash_names,
                             'TotalAsset': ta_names,
                             'TotalMarketValue': tmv_names,
                             'CashBalance': cb_names},
                       'c': {'Cash': cash_names, 'CashBalance': cb_names}}
        self.server = server
        return

    def reformulate(self, doc):
        date = time.strftime("%Y%m%d", time.localtime())
        update_time = time.strftime("%H:%M:%S", time.localtime())
        new_doc = {'AcctIDByMXZ': doc['AcctIDByMXZ'],
                   'DataDate': date, 'UpdateTime': update_time}
        account_type = doc['AcctIDByMXZ'].split('_')[1]
        # print(doc['AcctIDByMXZ'].split('_')[1])
        cfg = self.config[account_type]
        for key in cfg.keys():
            for name in cfg[key]:   # possible names
                if name in doc.keys():
                    value = doc[name]
                    if key == 'TotalAsset':
                        total_asset = value
                    if key == 'TotalMarketValue':
                        total_market_value = value
                    if key == 'Cash':
                        cash = value
                    new_doc.update({key: value})  # updatetime, datadate,
                    break
        try:
            if abs(total_asset - total_market_value - cash) >= 0.01:
                warnings.warn("Cash != TotalAsset - TotalMarketValue, please check the"
                              "document _id =%s in the database" % doc['_id'], Warning)
        except NameError:   # total_asset etc may not exist
            pass
        return new_doc

    def insert(self):

        new_docs = []

        for doc in self.server.find('trading_rawdata_fund'):
            new_doc = self.reformulate(doc)
            new_docs.append(new_doc)

        self.server.insert('trading_formatdata_fund', new_docs)

    def get(self, fields):
        # get field and change updatetime
        docs = []
        col_name = 'trading_formatdata_fund'
        update_time = time.strftime("%H:%M:%S", time.localtime())
        update_fields = {'UpdateTime': update_time}
        for doc in self.server.find(col_name, fields):
            doc.update(update_fields)
            docs.append(doc)
        self.server.update_fields(col_name, fields, update_fields)
        return docs     # a list of dicts, can turn directly to pd.DataFrame(docs)
