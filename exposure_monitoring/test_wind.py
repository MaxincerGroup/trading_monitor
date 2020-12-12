from WindPy import *

w.start()
stock = ['510500.SH', '511660.SH', '512500.SH', '000905.SH', '000300.SH', '000016.SH',
                                   'IC2009.CFE', 'IC2012.CFE']
# error_code, returns = w.wss(stock, "sec_name,close,fund_fundmanager","tradeDate=20201208", usedf=True)

returns = w.wst("IF.CFE", "last,volume", "2019-4-2 09:00:00", "2019-4-2 14:04:45", usedf=True)

# 不是所有期货都有分时成交价， 终端看看哪些选择
# err, res = w.wsi(["IF2012.CFE", "IF2101.CFE", "IF2103.CFE", "IF2106.CFE"], "open,high,close", "2020-12-9 09:00:00", "2020-12-9 14:48:41", usedf=True)
# "A，B，C，D"类型也可以
# 输出是 t1-A，t2-A,.. tn-A; t-B...
print(returns[1])
# print(res)
