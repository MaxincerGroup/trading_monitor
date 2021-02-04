import datetime
import schedule
import time
import tkinter
import tkinter.messagebox
import exposure_monitoring
import os
import logging

schedule_test = False
global_test = False
cmtimetest = False
upload_pseudo_postdata = False
log_test = False

if log_test:
    logger = logging.getLogger('mylogger')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('data/log/test.log')
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.error('aasaaaaaa')
    logger.info('oooooooh')
    logger.debug('kill')

if upload_pseudo_postdata:

    exposure_monitoring.is_trading_time_manual = False
    exposure_monitoring.update_postdata_manually = True
    read_raw = exposure_monitoring.ReadRaw()
    read_raw.path_basic_info = 'data/basic_info.xlsx'
    read_raw.db_basicinfo = exposure_monitoring.client_local_main['basic_info_']
    read_raw.col_acctinfo = read_raw.db_basicinfo['acctinfo']

    # read_raw.db_trddata = exposure_monitoring.client_local_test['trade_data_']
    # read_raw.db_posttrddata = exposure_monitoring.client_local_test['post_trade_data_']
    read_raw.upload_basic_info()
    read_raw.run()

    read_fmt = exposure_monitoring.FmtData()
    # read_fmt.db_trddata = exposure_monitoring.client_local_test['trade_data_']
    # read_fmt.db_posttrddata = exposure_monitoring.client_local_test['post_trade_data_']
    read_fmt.col_acctinfo = exposure_monitoring.client_local_main['basic_info_']['acctinfo']
    read_fmt.run()

    while True:
        print(datetime.datetime.today().strftime("%d  %H:%M:%S"))
        schedule.run_pending()
        # if func.__name__ == 'update_fmtdata':
        #     while col_global_var.find_one({'DataDate': self.str_day})['RawUpdateTime'] is None:
        #         time.sleep(1)
        #     print('34, ', col_global_var.find_one({'DataDate': self.str_day})['RawUpdateTime'])
        # func(self, *args, **kwargs)
        # print('Function: ', func.__name__, 'finished, go to sleep')
        time.sleep(10)

# 获得修改时间（post处理方法）
if cmtimetest:
    mtime = time.ctime(os.path.getmtime('C:/Users/86133/Desktop/假装post/basic_info临时.xlsx'))
    ctime = time.ctime(os.path.getctime('C:/Users/86133/Desktop/假装post/basic_info临时.xlsx'))
    # getatime access time
    # geta/c/mtime 输出时间戳 timestamp
    # time.strftime('%Y%d%m', stamp)
    print("Last modified : %s, last created time: %s" % (mtime, ctime))

# 定时运行一段代码...愚蠢方法就是：while True: if time == '...': run(); else: time.sleep(2)
# schedule就是自动考虑time interval的问题（sleep太久错过时间点）
if schedule_test:
    def show_msg_tip():
        tkinter.messagebox.showinfo('提示', '该休息会儿了')

    schedule.every().day.at("10:40:40").do(show_msg_tip)

    while True:
        schedule.run_pending()
        time.sleep(120)
