"""
Function:
    1. monitor 307 margin account position
"""
from datetime import datetime
from json import dumps

from flask import Flask, render_template
from flask_pymongo import PyMongo

app = Flask(__name__)

trade = PyMongo(app, uri="mongodb://admin:Ms123456@192.168.2.2:27017/trade_data?authSource=admin")
global_var = PyMongo(app, uri="mongodb://admin:Ms123456@192.168.2.2:27017/global_var?authSource=admin")

str_today = datetime.today().strftime('%Y%m%d')
query_time = '000000'


@app.route('/')
def homepage():
    return render_template('home_page.html')


@app.route('/js_get_position_data')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_position_data():
    global query_time
    res = []
    expos_rec = global_var.db.exposure_monitoring.find_one({'DataDate': str_today})
    if expos_rec:
        query_time = expos_rec['PositionUpdateTime']
    query_res = list(trade.db.trade_position.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time}},
                                                  {'_id': 0}))
    if not expos_rec:
        for _ in query_res:
            update_time = _['UpdateTime']
            if int(update_time) > int(query_time):
                query_time = update_time
                res = [_]
            else:
                res.append(_)
    else:
        res = query_res

    json_data = dumps(res)
    return json_data


@app.route('/js_get_account_position_detail/<acctid>')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_acct_position_detail(acctid):
    global query_time
    res = []
    expos_rec = global_var.db.exposure_monitoring.find_one({'DataDate': str_today})
    if expos_rec:
        query_time = expos_rec['PositionUpdateTime']
    query_res = list(trade.db.trade_position.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                   'AcctIDByMXZ': acctid}, {'_id': 0}))
    if not expos_rec:
        for _ in query_res:
            update_time = _['UpdateTime']
            if int(update_time) > int(query_time):
                query_time = update_time
                res = [_]
            else:
                res.append(_)
    else:
        res = query_res

    json_data = dumps(res)
    return json_data


@app.route('/js_get_acct_exposure_data')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_acct_exposure_data():
    global query_time
    dict_res = {}
    expos_rec = global_var.db.exposure_monitoring.find_one({'DataDate': str_today})
    if expos_rec:
        query_time = expos_rec['PositionUpdateTime']
    query_expo = list(
        trade.db.trade_exposure_by_acctid.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                'MonitorDisplayMark': '1'}, {'_id': 0}))
    if not expos_rec:
        for _ in query_expo:
            update_time = _['UpdateTime']
            acctid = _['AcctIDByMXZ']
            if int(update_time) > int(query_time):
                query_time = update_time
                dict_res = {acctid: _}  # 默认acctid仅对应一个acct exposure
            elif int(update_time) == int(query_time):
                dict_res.update({acctid: _})
    else:
        for _ in query_expo:
            dict_res.update({_['AcctIDByMXZ']: _})

    query_fund = list(
        trade.db.trade_balance_sheet_by_acctid.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                     'MonitorDisplayMark': '1'}, {'_id': 0}))
    for _ in query_fund:
        acctid = _['AcctIDByMXZ']
        if acctid in dict_res:
            dict_res[acctid].update(_)  # 加上fund信息，不改变acctid等
            na = dict_res[acctid]['NetAsset']
            if na:  # na is None or 0
                dict_res[acctid]['NetAmt/NetAsset'] = round(dict_res[acctid]['NetAmt'] / na, 2)
            else:
                dict_res[acctid]['NetAmt/NetAsset'] = '净值为0'

        else:
            dict_res.update({acctid: _})
            print('有账户没有持仓信息')

    res = list(dict_res.values())
    json_data = dumps(res)
    return json_data


@app.route('/js_get_prd_exposure_detail/<prdcode>')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_prd_exposure_detail(prdcode):
    global query_time
    dict_res = {}
    expos_rec = global_var.db.exposure_monitoring.find_one({'DataDate': str_today})
    if expos_rec:
        query_time = expos_rec['PositionUpdateTime']
    query_expo = list(
        trade.db.trade_exposure_by_acctid.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                'PrdCode': prdcode, 'MonitorDisplayMark': '1'}, {'_id': 0}))

    if not expos_rec:
        for _ in query_expo:
            update_time = _['UpdateTime']
            acctid = _['AcctIDByMXZ']
            if int(update_time) > int(query_time):
                query_time = update_time
                dict_res = {acctid: _}  # 默认acctid仅对应一个acct exposure
            elif int(update_time) == int(query_time):
                dict_res.update({acctid: _})
    else:
        for _ in query_expo:
            dict_res.update({_['AcctIDByMXZ']: _})

    query_fund = list(
        trade.db.trade_balance_sheet_by_acctid.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                     'PrdCode': prdcode, 'MonitorDisplayMark': '1'}, {'_id': 0}))
    for _ in query_fund:
        acctid = _['AcctIDByMXZ']
        if acctid in dict_res:
            dict_res[acctid].update(_)  # 加上fund信息，不改变acctid等
            na = dict_res[acctid]['NetAsset']
            if na:  # na is None or 0
                dict_res[acctid]['NetAmt/NetAsset'] = round(dict_res[acctid]['NetAmt'] / na, 2)
            else:
                dict_res[acctid]['NetAmt/NetAsset'] = '净资产为0'
        else:
            dict_res.update({acctid: _})
            print('有账户没有持仓信息')

    res = list(dict_res.values())
    json_data = dumps(res)
    return json_data


@app.route('/js_get_prdcode_exposure_data')  # 自定义XXX：网页打开 192.168.2.2:5000/XXXX
def get_prdcode_exposure_data():
    global query_time
    dict_res = {}
    expos_rec = global_var.db.exposure_monitoring.find_one({'DataDate': str_today})
    if expos_rec:
        query_time = expos_rec['PositionUpdateTime']
    query_expo = list(
        trade.db.trade_exposure_by_prdcode.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                'MonitorDisplayMark': '1'}, {'_id': 0}))
    if not expos_rec:
        for _ in query_expo:
            update_time = _['UpdateTime']
            prdcode = _['PrdCode']
            if int(update_time) > int(query_time):
                query_time = update_time
                dict_res = {prdcode: _}  # 默认acctid仅对应一个acct exposure
            elif int(update_time) == int(query_time):
                dict_res.update({prdcode: _})
    else:
        for _ in query_expo:
            dict_res.update({_['PrdCode']: _})

    query_fund = list(
        trade.db.trade_balance_sheet_by_prdcode.find({'DataDate': str_today, 'UpdateTime': {'$gte': query_time},
                                                     'MonitorDisplayMark': '1'}, {'_id': 0}))
    for _ in query_fund:
        prdcode = _['PrdCode']
        if prdcode in dict_res:
            dict_res[prdcode].update(_)  # 加上fund信息，不改变acctid等
            na = dict_res[prdcode]['NetAsset']
            if na:  # na is None or 0
                dict_res[prdcode]['NetAmt/NetAsset'] = round(dict_res[prdcode]['NetAmt'] / na, 2)
            else:
                dict_res[prdcode]['NetAmt/NetAsset'] = '净值为0'
        else:
            dict_res.update({prdcode: _})
            print('有产品没有持仓信息')

    res = list(dict_res.values())
    json_data = dumps(res)
    return json_data


@app.route('/position')
def display_trade_position():

    return render_template('position.html')   # 用js的模板去渲染, 模板里带变量acctid


@app.route('/account_position_detail/<acctid>')
def display_trade_acct_position_detail(acctid):

    return render_template('acct_position_detail.html', acctid=acctid)


@app.route('/acct_exposure')
def display_trade_acct_exposure():

    return render_template('acct_exposure.html')


@app.route('/prd_exposure_detail/<prdcode>')
def display_trade_prd_exposure_detail(prdcode):

    return render_template('prd_exposure_detail.html', prdcode=prdcode)


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


"""
CSS入门：

"""

"""
.js 文件：
$() 是标识符，可以是任何对象 $("#datatable") 表示jQuery对象,选择html上所有id = “datatable”类型的变量，具体定义在body里
<table class="easyui-datagrid" id="datatable" title="account position detail" style="width:100%;height:1000px"
           data-options="singleSelect:true,collapsible:true">
    </table>
$.ajax({})是请求的api，一般标准写法：
$.ajax({
    url:"http://www.microsoft.com",    //请求的url地址
    dataType:"json",   //返回格式为json
    async:true,//请求是否异步，默认为异步，这也是ajax重要特性
    data:{"id":"value"},    //参数值
    type:"POST",   //请求方式, GET, POST等
    beforeSend:function(){
        //请求前的处理
    },
    success:function(req){
        //请求成功时处理
    },
    complete:function(){
        //请求完成的处理
    },
    error:function(){
        //请求出错处理
    }
});

datagrid每行加超链接： 用formatter
{field: 'PrdCode', title: 'PrdCode', sortable: true},
{field: 'AcctIDByMXZ', title: 'AcctIDByMXZ', formatter: function(value){
    return '<a href="http://192.168.2.2:5000/account_position_detail/'+value+'">'+value+'</a>'}},

value就是每一格的值， <a>展示的值</a>， 同html
"""
# 这里用了jquery easyui 插件，网上可以下载，里面有 datagrid等用法

if __name__ == '__main__':

    app.run()



