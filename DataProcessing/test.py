import trdRaw, trdFormat, securityID
from mongoServer import Server

# 如何把config.ini放到Trader里？

info_path = 'C:/Users/86133/Desktop/研二实习'
file_path = info_path+'/zx_wealthcats'
file_path2 = info_path + '/security_id.xlsx'
# test classes
to_upload_security_info = False
to_read_csv = False
to_read_trader = False  # 每周1-5 9:00-17:00服务器才开
to_formulate_fund = False
to_formulate_holding = True

client = Server('trading')

if to_upload_security_info:
    # client.drop('stock_info')
    idfmt = securityID.IDFmt(client)
    # idfmt.upload_security_info()
    print(idfmt.id2service_map)

if to_read_csv:
    client.drop(['trading_rawdata_fund', 'trading_rawdata_holding', 'trading_rawdata_entrust'])
    csv = trdRaw.CSV(file_path, info_path, client)

    csv.insert('Fund', 'Position', ['Order', 'Transactions'])

if to_read_trader:
    client.drop(['trading_rawdata_fund', 'trading_rawdata_holding', 'trading_rawdata_entrust'])
    trd_api = trdRaw.TraderApi(info_path, client)

    trd_api.insert()

# processing raw 和 formulation 不是一起干的...架构

if to_formulate_fund:

    # input of zx_wealthcats:
    af_names = []
    cash_names = ['当前余额', '可用资金']
    ta_names = ['总资产']
    tmv_names = ['证券市值']
    cb_names = ['昨日余额']
    """
    # input from future account
    af_names = []
    cash_names = ['DYNAMICBALANCE']
    ta_names = []
    tmv_names = []
    cb_names = ['STATICBALANCE']
    """
    fundformat = trdFormat.Fund(client, af_names, cash_names, ta_names, tmv_names, cb_names)
    # trading_client.drop('trading_formatdata_fund')
    # print(fundformat.config)
    fundformat.insert()

    print(fundformat.get({'AcctIDByMXZ': '2_f_zc_0709'}))

if to_formulate_holding:
    """
    # input from future account
    lq_names = ['position']
    symb_names = []
    id_names = ['instrument_id']
    exchange_names = ['exchange']
    """
    # input from zx_wealthcats
    lq_names = ['当前余额']
    symb_names = ['名称']
    id_names = ['代码']
    exchange_names = []   # 市场代码？

    holdingformat = trdFormat.Holding(client, lq_names, symb_names, id_names, exchange_names)

    holdingformat.insert()

    print(holdingformat.get({'AcctIDByMXZ': '2_f_zc_0709'}))
