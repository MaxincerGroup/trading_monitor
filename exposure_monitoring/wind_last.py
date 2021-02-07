from WindPy import w
import threading
import pymongo
import datetime
import time

mongo_client = pymongo.MongoClient(host='192.168.2.2', port=27017,
                                   username='admin', password='Ms123456')


class WindLast:
    def __init__(self, forced_update=False):
        w.start()
        self.str_day = datetime.datetime.today().strftime('%Y%m%d')
        self.collection = mongo_client['trade_data']['wind_last']
        self.global_var = mongo_client['global_var']['last']
        # upsert 如果有就insert无就update
        self.global_var.update_one({'Key': 'SecidQuery'}, {'$set': {'Value': None}}, upsert=True)
        if self.global_var.find_one({'Key': 'Wcode2Last'}) is None:
            self.global_var.insert_one({'Key': 'Wcode2Last', '$set': None})
        if forced_update or self.collection.find_one({'DataDate': self.str_day}) is None:
            self.update_last_from_wind()
        return

    def update_last_from_wind(self):
        # print(w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010000000000"))
        list_astock_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a001010100000000").Data[1]
        # list_bond_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a100000000000000").Data[1]
        list_cffex_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010101000000").Data[1]
        list_shfe_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010201000000").Data[1]
        list_dce_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010301000000").Data[1]
        list_czce_codes = w.wset("sectorconstituent", f"date={self.str_day};sectorid=a599010401000000").Data[1]
        list_futures_codes = list_cffex_codes + list_shfe_codes + list_dce_codes + list_czce_codes
        # 期货sectorid CFFEX, SHFE, DCE, CZCE;能源期货  全部期货用不了了
        list_etf_codes = ['000300.SH', '000016.SH', '000905.SH', '510500.SH',  '512500.SH']  # index ETF that we use
        list_bond_codes = []
        list_repo_codes = []
        list_mmf_codes = ['511990.SH', '511830.SH', '511880.SH', '511850.SH', '511660.SH', '511810.SH',
                          '511690.SH', '159001.SZ', '159005.SZ', '159003.SZ']
        list_codes = list_astock_codes + list_bond_codes + list_futures_codes + list_etf_codes \
            + list_repo_codes + list_mmf_codes
        # err, close_data_from_wind = w.wss(list_codes, "sec_name, close", f"tradeDate={self.str_day};priceAdj=U;cycle=D", usedf=True)
        docs = []
        n = len(list_codes)/3000  # 一次最多询问4000条
        for i in range(int(n)+1):
            if (i+1)*3000 > len(list_codes):
                list_query = list_codes[i*3000:]
            else:
                list_query = list_codes[i * 3000:  (i+1)*3000]
            last_from_wind = w.wsq(list_query, "rt_last")  # 实时快照现价
            if last_from_wind.ErrorCode == 0:
                dict_wcode2last = dict(zip(last_from_wind.Codes, last_from_wind.Data[0]))
                for key in dict_wcode2last:
                    dt = last_from_wind.Times[0]
                    doc = {'TransactTime': dt.strftime("%H%M%S"), 'DataDate': dt.strftime("%Y%m%d"),
                           'LastPx': dict_wcode2last[key], 'WindCode': key}
                    docs.append(doc)
                self.collection.delete_many({'DataDate': self.str_day})
                self.collection.insert_many(docs)
            elif last_from_wind.ErrorCode == -40520010:
                pass
            else:
                raise Exception(last_from_wind.Data[0][0])

    def get_order_last_from_wind(self, ):
        # we do query only for securities in our account, secid should be type of wind
        # w.wsq("600000.SH", "rt_last,rt_latest", func=DemoWSQCallback)
        while True:
            time.sleep(1)
            list_secid_query = self.global_var.find_one({'Key': 'SecidQuery'})['Value']
            if list_secid_query:
                docs = []
                dict_wcode2last = {}
                last_from_wind = w.wsq(list_secid_query, "rt_last")   # 实时快照现价
                if last_from_wind.ErrorCode == 0:
                    dict_wcode2last = dict(zip(last_from_wind.Codes, last_from_wind.Data[0]))
                    for key in dict_wcode2last:
                        dt = last_from_wind.Times[0]
                        doc = {'TransactTime': dt.strftime("%H%M%S"), 'DataDate': dt.strftime("%Y%m%d"),
                               'LastPx': dict_wcode2last[key], 'WindCode': key}
                        docs.append(doc)
                elif last_from_wind.ErrorCode == -40520010:
                    pass
                else:
                    raise Exception(last_from_wind.Data[0][0])  # Error Msg here
                if docs:
                    # self.collection.insert_many(docs)
                    self.global_var.update_one({'Key': 'SecidQuery'}, {'$set': {'Value': None}})
                    self.global_var.update_one({'Key': 'Wcode2Last'}, {'$set': {'Value': dict_wcode2last}})

    def run(self):
        thread_last = threading.Thread(target=self.get_order_last_from_wind)
        thread_last.start()


if __name__ == '__main__':
    wl = WindLast()
    wl.run()
