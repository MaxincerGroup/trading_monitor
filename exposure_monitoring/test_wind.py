from WindPy import *
import datetime

w.start()
stock = ['510500.SH', '511660.SH', '512500.SH', '000905.SH', '000300.SH', '000016.SH',
                                   'IC2009.CFE', 'IC2012.CFE']
# error_code, returns = w.wss(stock, "sec_name,close,fund_fundmanager","tradeDate=20201208", usedf=True)

list_secid_query = ['600000.SH', '510500.SH']
start_time = (datetime.datetime.today()-datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
end_time = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
docs = []

for secid in list_secid_query:
    last_from_wind = w.wst(secid, "last", start_time, end_time)
    # 经常莫名其妙报错...service connection failed，数据也可能错...
    if last_from_wind.ErrorCode == 0:
        date_str = last_from_wind.Times[-1].strftime("%Y-%m-%d %H:%M:%S")   # datetime.datetime
        doc = {'TransactTime': date_str, 'LastPx': last_from_wind.Data[0][-1], 'wind_code': secid}    # 需要 time, last. sec_name???
        docs.append(doc)
    else:  # service connection error
        # or pass; or verify the data is not in wind system
        print(last_from_wind)

# 不是所有期货都有分时成交价， 终端看看哪些选择
# err, res = w.wsi(["IF2012.CFE", "IF2101.CFE", "IF2103.CFE", "IF2106.CFE"], "open,high,close", "2020-12-9 09:00:00", "2020-12-9 14:48:41", usedf=True)
# "A，B，C，D"类型也可以
# 输出是 t1-A，t2-A,.. tn-A; t-B...
print(docs)
# print(res)
