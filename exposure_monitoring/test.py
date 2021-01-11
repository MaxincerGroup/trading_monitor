from WindPy import *
import datetime
from flask import Flask
import schedule
import time
import tkinter
import tkinter.messagebox
import exposure_monitoring

flask_test = False
wind_test = False
schedule_test = False
global_test = False
upload_pseudo_postdata = True

if upload_pseudo_postdata:
    exposure_monitoring.dt_test_day = datetime.datetime.today()
    read_raw = exposure_monitoring.ReadRaw()
    read_raw.path_basic_info = 'C:/Users/86133/Desktop/假装post/basic_info临时.xlsx'
    read_raw.db_basicinfo = exposure_monitoring.client_local_test['basic_info_']
    read_raw.col_acctinfo = read_raw.db_basicinfo['acctinfo']
    read_raw.is_trading_time = False
    # read_raw.db_trddata = exposure_monitoring.client_local_test['trade_data_']
    # read_raw.db_posttrddata = exposure_monitoring.client_local_test['post_trade_data_']
    read_raw.upload_basic_info()
    read_raw.run()

    read_fmt = exposure_monitoring.FmtData()
    # read_fmt.db_trddata = exposure_monitoring.client_local_test['trade_data_']
    # read_fmt.db_posttrddata = exposure_monitoring.client_local_test['post_trade_data_']
    read_fmt.col_acctinfo = exposure_monitoring.client_local_test['basic_info_']['acctinfo']
    read_fmt.is_trading_time = False
    read_fmt.run()

# 定时运行一段代码...愚蠢方法就是：while True: if time == '...': run(); else: time.sleep(2)

if schedule_test:
    def show_msg_tip():
        tkinter.messagebox.showinfo('提示', '该休息会儿了')

    schedule.every().day.at("18:02:00").do(show_msg_tip)

    while True:
        schedule.run_pending()
        time.sleep(120)

if flask_test:
    app = Flask(__name__)

    @app.route('/')
    def index():    # 装饰器的作用是将路由映射到视图函数index
        return 'ok'

    if __name__ == '__main__':
        app.run()

if wind_test:
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
