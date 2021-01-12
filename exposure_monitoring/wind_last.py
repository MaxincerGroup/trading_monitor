from WindPy import w
import threading
import pymongo
import datetime
import time

mongo_client = pymongo.MongoClient(host='localhost', port=27017,
                                   username='admin', password='123456')


class WindLast:
    def __init__(self):
        w.start()
        self.str_day = datetime.datetime.today().strftime('%Y%m%d')
        self.collection = mongo_client['trade_data']['wind_last']
        self.global_var = mongo_client['global_var']['last']
        # upsert 如果有就insert无就update
        self.global_var.update_one({'Key': 'SecidQuery'}, {'$set': {'Value': None}}, upsert=True)
        self.global_var.update_one({'Key': 'Wcode2Last'}, {'$set': {'Value': None}}, upsert=True)
        return

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
                    self.collection.insert_many(docs)
                    self.global_var.update_one({'Key': 'SecidQuery'}, {'$set': {'Value': None}})

                    self.global_var.update_one({'Key': 'Wcode2Last'}, {'$set': {'Value': dict_wcode2last}})

    def run(self):
        thread_last = threading.Thread(target=self.get_order_last_from_wind)
        thread_last.start()


if __name__ == '__main__':
    wl = WindLast()
    wl.run()
