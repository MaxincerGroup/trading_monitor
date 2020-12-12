# coding: utf-8

import zmq
import json
import configparser
import telnetlib


class Trader:
    def __init__(self, product_id):
        """
        :param product_id: FAcctIDByOWJ
        """
        self.user_id = 'MXZ'
        self.product_id = str(product_id)
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")
        self.query_address = self.config.get("query", "query_address")
        self.query_port = self.config.get("query", "query_port")
        try:
            telnetlib.Telnet(self.query_address, port=self.query_port, timeout=10)
        except BaseException as e:
            raise Exception(f"测连{self.query_address}:{self.query_port} 连接不通，请联系管理员。")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{self.query_address}:{self.query_port}")
        recv_message = self.send_message(
            json.dumps({"PRODUCTID": self.product_id, "USERID": self.user_id, "VERB": "check"})
        )
        if recv_message:
            dict_message = json.loads(recv_message, encoding="gbk")
            if "SUCCESS" in dict_message.keys():
                if dict_message["SUCCESS"]:
                    self.product_name = dict_message["PRODUCTNAME"]
                else:
                    raise Exception(f"测连{self.product_id}产品失败：{dict_message['MSG']}！")
            else:
                raise Exception("接收测连数据格式错误！")
        else:
            raise Exception("接收测连数据为空！")

    def send_message(self, message):
        self.socket.send_string(message)
        ret_msg = self.socket.recv_string()
        return ret_msg

    def query_holding(self):
        recv_message = self.send_message(
            json.dumps({"PRODUCTID": self.product_id, "USERID": self.user_id, "VERB": "holding"})
        )
        if len(recv_message):
            return json.loads(recv_message, encoding="utf-8")
        else:
            raise Exception("未接收到产品持仓数据！")

    def query_trdrecs(self):
        recv_message = self.send_message(
            json.dumps({"PRODUCTID": self.product_id, "USERID": self.user_id, "VERB": "traded"})
        )
        if len(recv_message):
            return json.loads(recv_message, encoding="utf-8")
        else:
            raise Exception("未接收到产品成交订单数据！")

    def query_capital(self):
        recv_message = self.send_message(
            json.dumps({"PRODUCTID": self.product_id, "USERID": self.user_id, "VERB": "account"})
        )
        if len(recv_message):
            return json.loads(recv_message, encoding="utf-8")
        else:
            raise Exception("未接收到产品资金情况数据！")

