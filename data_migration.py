import pymongo
import datetime

client_source = pymongo.MongoClient(host='192.168.2.162', port=27017)
client_target = pymongo.MongoClient(host='localhost', port=27019)
# 为啥有时会19有时18？

query_rule = {}  # 以后更新用DataDate: today, 第一次{}
dbnames = ['post_trade_data', 'trddata']

for dbname in ['trddata']:
    db_source = client_source[dbname]
    db_target = client_target[dbname]
    # colnames = db_source.list_collection_names()
    colnames = set(db_source.list_collection_names())-set(db_target.list_collection_names())
    # colnames = ['manually_rawdata_holding', 'tgtcpsamt', 'bgt_by_acctidbymxz', 'future_api_capital', 'items_2b_adjusted',
    #             'items_budget', 'facct_holding_aggr_by_secid_first_part', 'facct_holding_aggr_by_prdcode',
    #             'col_cpslongamt_from_sse_by_acctidbymxz', 'formatted_holding', 'manually_rawdata_secliability']
    print(colnames)
    for col in colnames:
        if col in ['manually_rawdata_holding', 'formatted_holding', 'manually_rawdata_secliability']:  # 两表过大...只记录最近几天的...
            print(col, 'Start')
            day0 = datetime.datetime(2020, 11, 13, 10, 0, 0)   # 最新就这个日期...
            for i in range(16):
                day = day0 - datetime.timedelta(days=i)
                str_day = day.strftime("%Y%m%d")
                list_find = list(db_source[col].find({'DataDate': str_day}))
                print(col, str_day, 'Ok')
                if list_find:
                    db_target[col].insert_many(list_find)
        else:
            print(col, 'Start')
            list_find = list(db_source[col].find(query_rule))
            print(col, 'Ok')
            db_target[col].insert_many(list_find)
