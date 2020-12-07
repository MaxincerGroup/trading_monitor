import time
import warnings
import securityID


class GenTrdFmt:
    def __init__(self, server, name):
        self.server = server
        self.search_col_name = 'trading_rawdata_' + name
        self.insert_col_name = 'trading_fmtdata_' + name
        self.config = {'m': {},  # key: list of synonyms of "key"
                       'f': {}, 'c': {}}

    def reformulate(self, doc):
        """
        In this function, we modify a document in raw data and reformulate it.
        New doc must contain attributes in self.config['m'], config['c'] or config['f']
        and we will check and add other information in the sub class.
        """
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
                    new_doc.update({key: value})  # updatetime, datadate,
                    break
        new_doc = self.check_and_add(doc, new_doc)

        return new_doc

    def check_and_add(self, doc, new_doc):
        """
        Check the account book or update (in the child class)
        """
        return new_doc

    def insert(self):
        """
        Insert reformulated documents into the collection trading_rawdata_fund
        """
        new_docs = []

        for doc in self.server.find(self.search_col_name):
            new_doc = self.reformulate(doc)
            if new_doc:
                new_docs.append(new_doc)
        self.server.insert(self.insert_col_name, new_docs)

    def get(self, fields):

        return self.server.get_fileds_and_updatetime(self.insert_col_name, fields)


class Fund(GenTrdFmt):
    def __init__(self, server, af_names, cash_names, ta_names, tmv_names, cb_names):
        """
        :param server: mongodb client that connected to the server
        :param af_names:  the "synonyms" of AvailableFund in the raw data heads
        """
        super(Fund, self).__init__(server, 'fund')
        self.config = {'m': {'AvailableFund': af_names,
                             'Cash': cash_names,
                             'TotalAsset': ta_names,
                             'TotalMarketValue': tmv_names,
                             'CashBalance': cb_names},
                       'c': {'Cash': cash_names, 'CashBalance': cb_names},
                       'f': {'Cash': cash_names, 'CashBalance': cb_names}}
        return

    def check_and_add(self, doc, new_doc):
        """
        check if cash == total_asset - total_market_value.
        """
        try:
            if abs(new_doc['TotalAsset'] - new_doc['TotalMarketValue'] - new_doc['Cash']) >= 0.01:
                warnings.warn("Cash != TotalAsset - TotalMarketValue, please check the"
                              "document _id =%s in the database"%doc['_id'], Warning)
        except KeyError:   # 'TotalAsset' etc may not exist
            pass
        return new_doc


class Holding(GenTrdFmt):
    def __init__(self, server, lq_names, symb_names, id_names, exchange_names):
        super(Holding, self).__init__(server, 'holding')
        self.config = {'m': {'LongQty': lq_names,  'Symbol': symb_names,
                             'SecurityID': id_names, 'SecurityIDSource': exchange_names},
                       'c': {'LongQty': lq_names,  'Symbol': symb_names,
                             'SecurityID': id_names, 'SecurityIDSource': exchange_names},
                       'f': {'LongQty': lq_names,  'Symbol': symb_names,
                             'SecurityID': id_names, 'SecurityIDSource': exchange_names}}

        self.idfmt = securityID.IDFmt(self.server)
        self.already_warned_securityId = False
        self.already_warned_isFuture = False

    def check_and_add(self, doc, new_doc):
        """
        只有不标准的券商不带市场代码等，标准的券商可以直接荡下来在config里
        如果没有可以通过下面的函数由SecurityID得到（证券交易所代码分配规范）
        """
        if 'f' == doc['AcctIDByMXZ'].split('_')[1] and not self.already_warned_isFuture:
            warnings.warn("holding is not for Future Accounts in China, please check "
                          "document _id=%s in the collection" % doc['_id'], Warning)
            self.already_warned_isFuture = True

        k = new_doc.keys()
        case1 = 'SecurityID' in k
        case2 = 'SecurityIDSource' in k
        if case1 and case2:
            # ok do nothing
            return new_doc
        elif case1 and not case2:
            sid = new_doc['SecurityID'][0:3]       # 用前三位来判断交易所
            sid_source = self.idfmt.find_exchange(sid)
            # print(sid_source)
            new_doc.update({"SecurityIDSource": sid_source})
            return new_doc
        else:
            if not self.already_warned_securityId:
                warnings.warn("Can't detect SecurityID of document _id=%s "
                              "in the collection"%doc['_id'], Warning)
                warnings.warn("该券商格式不合规", Warning)
            self.already_warned_securityId = True
            return new_doc


class Position(GenTrdFmt):
    def __init__(self, server, lq_names, sq_names, nq_names, la_names,
                 sa_names, na_names, id_names, symb_names, marketid_names):

        super(Position, self).__init__(server, 'position')
        self.config = {'m': {'LongQty': lq_names, 'ShortQty': sq_names, 'NetQty': nq_names,
                             'LongAmt': la_names, 'ShortAmt': sa_names, 'NetAmt': na_names,
                             'SecurityID': id_names, 'Symbol': symb_names},
                       'c': {},
                       'f': {'LongQty': lq_names, 'ShortQty': sq_names, 'NetQty': nq_names,
                             'LongAmt': la_names, 'ShortAmt': sa_names, 'NetAmt': na_names,
                             'Symbol': symb_names}}
        self.backup_idsource = marketid_names

    def check_and_add(self, doc, new_doc):
        """ Add SecurityID and IDSource """
        # not same as holding, since here is for futures
        return new_doc


class Order(GenTrdFmt):
    def __init__(self, server):
        super(Order, self).__init__(server, 'order')

    def check_and_add(self, doc, new_doc):

        return new_doc
