import pandas as pd
import warnings


class IDFmt:
    def __init__(self, server, file_path=None):
        self.file_path = file_path
        self.server = server

        self.id2service_map = {}
        self.__documents = []
        if file_path:
            if "security_info" in self.server.list_collection_names():
                flag = input("security_info already exists, are you sure to upload again? Y/N")
                if flag == 'Y':
                    self.server.drop("security_info")
                    self.id_info_dict()
                    self.upload_security_info()
                else:
                    self.download_security_info()
            else:
                if "security_info" in self.server.list_collection_names():
                    self.id_info_dict()
                    self.upload_security_info()
                else:
                    raise NameError("There's no collection in the database that contains"
                                    "security information")
        else:
            self.download_security_info()

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
        self.server.insert('security_info', self.__documents)

    def download_security_info(self):
        for doc in self.server.find('security_info'):
            self.__documents.append(doc)
            name = doc['代码前3位']
            value = doc.copy()  # 防止对__documents里doc 修改
            del value['代码前3位']
            if name in self.id2service_map.keys():
                value = [value, self.id2service_map[name]]
            self.id2service_map.update({name: value})

    def find_exchange(self, sid):
        # ['000','100','102','103','104','105','106','107','108','120','121','122','123','124','125','126','127','128',
        # '129','131','140','150','151','159','160','161','162','163','164','165','201','202','203','204','205','206','207','360']
        is_new = not(sid in self.old_search_securityids.keys())
        # print(self.old_search_securityids)
        if is_new:
            if sid == '000':
                result = int(input('证券是否为主板A股？1/0'))  # SZSE: 主板A股, SSE 上证指数系列
                # result = 0 -> SSE; 1-> SZSE
            if sid[0:2] == '10':
                result = int(input('证券是否为付息式国债或贴现式国债？1/0'))
                # 'SZSE':付息式国债或贴现式国债 SSE: 100-109债券回购，质押入库等
            if sid[0:2] == '12':
                result = int(input('证券是否为可转换公司债券？1/0'))
                # SZSE 可转换公司债券 SSE 120-129 公司债券，企业债券，资产支持证券等
            if sid == '131':
                result = int(input('证券是否为资产支持证券？1/0'))
                result = 1 - result # SSE:资产支持证券; SZSE: 债券回购等等操作
            if sid == '140':
                result = int(input('证券是否为优先股？1/0'))   # SZSE: 优先股; SSE: 地方政府债券
            if sid in ['150', '151', '159']:
                result = int(input('证券是否为分级基金子基金或ETF？1/0'))
                # SZSE: 分级基金子基金, 159=ETF; SSE: 非公开公司债，159=ABS
            if sid[0:2] == '16':
                result = int(input('证券是否为开放式基金？1/0'))     # SZSE: 开放式基金; SSE: 债券相关
            if sid[0:2] == '20':
                result = int(input('证券是否为B股？1/0'))    # SZSE: B股; SSE: 债券回购
            if sid == '360':
                result = int(input('证券是否为非公开发行优先股？1/0'))
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
            if sid in self.old_search_securityids:
                return self.old_search_securityids[sid]
        # other cases
        try:
            return self.id2service_map[sid]['交易所']
        except KeyError:    # if id is not among security_id excel, it should be OTC (场外交易）
            print('We can not find this id in Exchange')
            return 'OTC'

    def clean_search_response(self):
        self.old_search_securityids = {}
        return
