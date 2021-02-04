# todo wind_last 文件也可以放过来
import pandas as pd
import warnings


def get_sectype_from_code(windcode):
    list_split_wcode = windcode.split('.')
    secid = list_split_wcode[0]
    exchange = list_split_wcode[1]
    if exchange in ['SH', 'SSE'] and len(secid) == 6:
        if secid in ['511990', '511830', '511880', '511850', '511660', '511810', '511690']:
            return 'CE'
        elif secid in ['204001']:
            return 'CE'
        elif secid[:3] in ['600', '601', '603', '605', '688']:
            return 'CS'
        elif secid in ['510500', '000905', '512500']:
            return 'ETF'
        else:
            return 'IrrelevantItem'

    elif exchange in ['SZ', 'SZSE'] and len(secid) == 6:
        if secid[:3] in ['000', '001', '002', '003', '004', '300', '301', '302', '303', '304', '305', '306', '307',
                         '308', '309']:
            return 'CS'
        elif secid[:3] in ['115', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129']:
            return '可转债'
        elif secid[:3] in ['131']:
            return 'CE'
        elif secid in ['159001', '159005', '159003']:
            return 'CE'
        else:
            return 'IrrelevantItem'
    elif exchange in ['CFE', 'CFFEX']:
        return 'Index Future'

    elif exchange == 'ITN':
        sectype = secid.split('_')[0]
        return sectype

    else:
        raise ValueError(f'{windcode} has unknown exchange or digit number is not 6.')


class ID2Source:
    def __init__(self, database, file_path=None):
        self.file_path = file_path
        self.database = database

        self.id2service_map = {}
        self.__documents = []
        if file_path:
            self.id_info_dict()
            self.upload_security_info()
        else:
            if "security_info" in self.database.list_collection_names():
                self.download_security_info()
            else:
                raise NameError("There's no collection in the database that contains "
                                "security information")

        self.old_search_securityids = {}  # id and response

    def id_info_dict(self):
        df = pd.read_excel(self.file_path, dtype=str)
        for i, row in df.iterrows():
            name = row['第1位'] + row['第2-3位']
            value = row[['交易所', '业务标识定义']].to_dict()
            doc = value.copy()
            if name in self.id2service_map.keys():
                value = [self.id2service_map[name], value]
                # print('security id %s should be noted because it refers '
                #       'to 2 securitys of 2 Exchanges'%name)
            doc.update({'代码前3位': name})
            self.__documents.append(doc)
            self.id2service_map.update({name: value})
        return

    def upload_security_info(self):
        self.database['security_info'].drop()
        self.database['security_info'].insert_many(self.__documents)

    def download_security_info(self):
        for doc in self.database.find('security_info'):
            self.__documents.append(doc)
            name = doc['代码前3位']
            value = doc.copy()  # 防止对__documents里doc 修改
            del value['代码前3位']
            if name in self.id2service_map.keys():
                value = [value, self.id2service_map[name]]
            self.id2service_map.update({name: value})

    def find_exchange(self, sid):
        # ['000','100','102','103','104','105','106','107','108', '111', '118', '120','121','122','123','124','125','126','127','128',
        # '129','131','140','150','151','159','160','161','162','163','164','165', '166', '167', '168', '169'
        # '201','202','203','204','205','206','207','360']
        secid = sid
        sid = sid[:3]
        is_new = not(sid in self.old_search_securityids.keys())
        # print(self.old_search_securityids)
        if is_new:
            if sid == '000':
                if secid in []:  # 一般默认A股
                    result = 0
                else:
                    result = 1    #  int(input('%s 证券是否为主板A股？1/0'%secid))  # SZSE: 主板A股, SSE 上证指数系列
                # result = 0 -> SSE; 1-> SZSE
            if sid[0:2] == '10':
                result = int(input('%s 证券是否为付息式国债或贴现式国债？1/0'%secid))
                # 'SZSE':付息式国债或贴现式国债 SSE: 100-109债券回购，质押入库等
            if sid in ['111', '118']:
                result = int(input('%s 证券是否为企业债券？ 1/0'%secid))
                # 'SZSE': （中小）企业债， SSE: 111可转换公司债； 118 科创板公司债
            if sid[0:2] == '12':
                result = int(input('%s 证券是否为可转换公司债券？1/0'%secid))
                # SZSE 可转换公司债券 SSE 120-129 公司债券，企业债券，资产支持证券等
            if sid == '131':
                result = int(input('%s 证券是否为资产支持证券？1/0'%secid))
                result = 1 - result # SSE:资产支持证券; SZSE: 债券回购等等操作
            if sid == '140':
                result = int(input('%s 证券是否为优先股？1/0'%secid))   # SZSE: 优先股; SSE: 地方政府债券
            if sid in ['150', '151', '159']:
                result = int(input('%s 证券是否为分级基金子基金或ETF？1/0'%secid))
                # SZSE: 分级基金子基金, 159=ETF; SSE: 非公开公司债，159=ABS
            if sid[0:2] == '16':
                result = int(input('%s 证券是否为开放式基金？1/0'%secid))     # SZSE: 开放式基金; SSE: 债券相关,ABS等
            if sid[0:2] == '20':
                result = int(input('%s 证券是否为B股？1/0'%secid))    # SZSE: B股; SSE: 债券回购
            if sid == '360':
                result = int(input('%s 证券是否为非公开发行优先股？1/0'%secid))
                result = 1 - result     # SSE: 非公开发行优先股; SZSE: 主板股东大会网络投票
            try:
                if result == 1:
                    security_type = 'SZSE'
                else:
                    security_type = 'SSE'
                self.old_search_securityids.update({sid: security_type})
                return security_type
            except NameError:  # we may not define result
                pass
        else:
            return self.old_search_securityids[sid]
        # other cases
        try:
            return self.id2service_map[sid]['交易所']
        except KeyError:    # if id is not among security_id excel, it should be OTC (场外交易）
            warnings.warn('We can not find this id in Exchange, return OTC')
            return 'OTC'

    def clean_search_response(self):
        self.old_search_securityids = {}
        return
