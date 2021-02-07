const intervalTime = 60000;

$("#datatable").datagrid(
    {
        title:'exposure monitor by account ID',
        remoteSort:false,
        rownumbers:true,
        columns:[
            [
                {field: 'DataDate', title: 'DataDate', sortable: true},
                {field: 'UpdateTime', title: 'UpdateTime', sortable: true},
                {field: 'PrdCode', title: 'PrdCode', sortable: true},
                {field: 'AcctIDByMXZ', title: 'AcctIDByMXZ', formatter: function(value){
                    return '<a href="http://192.168.2.2:5000/account_position_detail/'+value+'" target="_blank">'+value+'</a>'}},
                //{field: 'AcctIDByMXZ', title: 'AcctIDByMXZ', sortable: true},
                {field: 'LongQty', title: 'LongQty', sortable: true},
                {field: 'ShortQty', title: 'ShortQty', sortable: true},
                {field: 'NetQty', title: 'NetQty', sortable: true},
                {field: 'LongAmt', title: 'LongAmt', sortable: true},
                {field: 'ShortAmt', title: 'ShortAmt', sortable: true},
                {field: 'NetAmt', title: 'NetAmt', sortable: true},
                {field: 'NetAsset', title: 'NetAsset', sortable: true},
                {field: 'NetAmt/NetAsset', title: 'NetAmt/NetAsset', sortable: true},

            ]
        ]
    }
);

fun()
setInterval(fun, intervalTime);

function fun() {
    $.ajax(
        {
            type:"GET",
            url:'/js_get_acct_exposure_data',
            dataType:'json',
            success:function (data){
                data=data.sort()
                $('#datatable').datagrid('loadData', data);
            }
        }
    )
}



