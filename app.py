"""
Function:
    1. monitor 307 margin account position
"""
from datetime import datetime
from json import dumps

from flask import Flask, render_template
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config['MONGO_URI'] = "mongodb://admin:Ms123456@192.168.2.2:27017/trade_data?authSource=admin"
mongo = PyMongo(app)

str_today = datetime.today().strftime('%Y%m%d')
query_time = '000000'


@app.route('/')
def homepage():
    return render_template('home_page.html')


@app.route('/js_get_position_data')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_position_data():
    global query_time
    res = []
    query_res = list(mongo.db.trade_position.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time}}, {'_id': 0}))
    for _ in query_res:
        update_time = _['UpdateTime']
        if int(update_time) > int(query_time):
            query_time = update_time
            res = [_]
        else:
            res.append(_)
    json_data = dumps(res)
    return json_data


@app.route('/js_get_acct_exposure_data', methods=['Get', 'Post'])  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_acct_exposure_data():
    global query_time
    res = []
    query_res = list(
        mongo.db.trade_acct_exposure.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time}}, {'_id': 0}))
    for _ in query_res:
        update_time = _['UpdateTime']
        if int(update_time) > int(query_time):
            query_time = update_time
            res = [_]
        else:
            res.append(_)
    json_data = dumps(res)
    return json_data


@app.route('/js_get_acct_exposure_data/8111')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_acct_exposure_data_by():
    global query_time
    res = []
    query_res = list(
        mongo.db.trade_acct_exposure.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                           'PrdCode': 8111}, {'_id': 0}))
    for _ in query_res:
        update_time = _['UpdateTime']
        if int(update_time) > int(query_time):
            query_time = update_time
            res = [_]
        else:
            res.append(_)
    json_data = dumps(res)
    return json_data


@app.route('/js_get_prdcode_exposure_data')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_prdcode_exposure_data():
    global query_time
    res = []
    query_res = list(
        mongo.db.trade_prdcode_exposure.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time}}, {'_id': 0}))
    for _ in query_res:
        update_time = _['UpdateTime']
        if int(update_time) > int(query_time):
            query_time = update_time
            res = [_]
        else:
            res.append(_)
    json_data = dumps(res)
    return json_data


@app.route('/position')
def display_trade_position():

    return render_template('position.html')   # 用js的模板去渲染, 模板里带变量acctid


@app.route('/acct_exposure/8111')
def display_trade_acct_exposure_by():

    return render_template('acct_exposure8111.html')


@app.route('/acct_exposure')
def display_trade_acct_exposure():

    return render_template('acct_exposure.html')


@app.route('/prdcode_exposure')
def display_trade_prdcode_exposure():

    return render_template('prdcode_exposure.html')

# render_template可以传入其他参数， 然后在html里{{}}表示变量
# html里写语句
# {% for x in list %} ....\n {% endfor %}
# {% if %} ...\n {% else %}  ...\n  {% endif %}
# 模板的继承: {% extends 父模板名称 %}
# 模板override: {% block 重载块名 %} {% endblock %}

# html使用js <script type="text/javascript" src="../static/monitor_position.js"></script>
# html中引入外部js文件并调用带参函数 https://blog.csdn.net/congju/article/details/52434830

# !!! 历史上的错误会再一次出现：浏览器没清除缓存（用的是以前缓存的文件）： ctrl+shift+R


if __name__ == '__main__':

    app.run()



