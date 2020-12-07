import pymongo
import time


class Server:  # can create some parent class like Server
    def __init__(self, dbname, host='localhost', port=27017,
                 username='admin', password='123456'):
        """
        :param dbname: database name;
        :param password: should better be prompt input
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.__client = pymongo.MongoClient(port=port, host=host,
                                            username=username, password=password)
        self.__db = self.__client[dbname]   # 'semi' private variable
        return

    def insert(self, col_name, documents):
        """
        :param col_name: collection name; dict_lists: a list of dictionaries
        :return: None, upload dictionaries into the database
        """
        if len(documents) > 1:
            self.__db[col_name].insert_many(documents)
        elif len(documents) == 1:
            self.__db[col_name].insert_one(documents)
        return

    def find(self, col_name, field=None):
        # 可以加一些账户的判断，判断其权限
        # field should be a dictionary
        return self.__db[col_name].find(field)

    def list_collection_names(self):
        return self.__db.list_collection_names()

    def update_fields(self, col_name, search_fields, update_fields):
        """
        :param col_name: collection name
        :param search_fields: a dict; to search the target documents
        :param update_fields: a dict; to modify specific fields in the target
        """
        self.__db[col_name].update_many(search_fields, {'$set': update_fields})
        return

    def get_fileds_and_updatetime(self, col_name, search_fields):
        """
        get field and change UpdateTime
        :param search_fields: a dict; to search the target documents
        :return: a list of found docs
        """
        docs = []
        update_time = time.strftime("%H:%M:%S", time.localtime())
        update_fields = {'UpdateTime': update_time}
        for doc in self.find(col_name, search_fields):
            doc.update(update_fields)
            docs.append(doc)
        self.update_fields(col_name, search_fields, update_fields)
        return docs

    def drop(self, col_name):
        flag = input('Are you sure to drop collection(s): %s? Y/N' % col_name)
        if flag == 'Y':
            if isinstance(col_name, list):
                for c in col_name:
                    self.__db[c].drop()
            else:
                self.__db[col_name].drop()
        return
