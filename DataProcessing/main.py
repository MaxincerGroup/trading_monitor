import trdRaw, trdFormat
from mongoServer import Server

info_path = 'C:/Users/86133/Desktop/研二实习'
file_path = info_path+'/zx_wealthcats'
# AcctIDByMXZ = '929_c_zx_6218'

trading_client = Server('trading')
to_read = False
if to_read:
    trading_client.drop_all()
    csv = trdRaw.CSV(file_path, info_path, trading_client)

    csv.insert_fund('Fund')
    csv.insert_holding('Position')
    csv.insert_entrust(['Order', 'Transactions'])

# 不是一起干的...架构

af_names = []
cash_names = ['当前余额', '可用资金']
ta_names = ['总资产']
tmv_names = ['证券市值']
cb_names = ['昨日余额']

fundformat = trdFormat.Fund(trading_client, af_names, cash_names, ta_names, tmv_names, cb_names)
# trading_client.drop('trading_formatdata_fund')
# fundformat.insert()

print(fundformat.get({'AcctIDByMXZ': '929_c_zx_6218'}))
