const intervalTime = 60000;

$("#datatable").datagrid(
    {
        title:'exposure monitor by product code',
        remoteSort:false,
        rownumbers:true,
        columns:[
            [
                {field: 'DataDate', title: 'DataDate', sortable: true},
                {field: 'UpdateTime', title: 'UpdateTime', sortable: true},
                {field: 'PrdCode', title: 'PrdCode', sortable: true, formatter:function(value){
                     return '<a href="http://192.168.2.2:5000/prd_exposure_detail/'+value+'" target="_blank">'+value+'</a>';
                    }},

                {field: 'StkLongAmt', title: 'StkLongAmt', sortable: true},
                {field: 'StkShortAmt', title: 'StkShortAmt', sortable: true},
                {field: 'StkNetAmt', title: 'StkNetAmt', sortable: true},
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
            url:'/js_get_prdcode_exposure_data',
            dataType:'json',
            success:function (data){
                data=data.sort()
                $('#datatable').datagrid('loadData', data);
            }
        }
    )
}



