import trdRaw

file_path = 'C://Users//86133//Desktop//研二实习//zx_wealthcats'
name_rules = ['Fund', 'Position', 'Transactions']
AcctIDByMXZ = '929_c_zx_6218'


def dateformat_zx_wealthcats(file):
    ind1 = file.find('_')
    ind2 = file.find('.csv')
    return file[ind1+1:ind2]


csv = trdRaw.CSV(file_path, AcctIDByMXZ, dateformat_zx_wealthcats)
trading_client = trdRaw.LocalServer('trading')
file_name = "StockOrder_2020-11-19.csv"
csv.insert(trading_client, name_rules)



