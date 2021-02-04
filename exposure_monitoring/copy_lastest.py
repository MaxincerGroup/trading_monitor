import os
import datetime
import shutil

today = datetime.datetime.today()
source_path = '//192.168.2.100/trddata/investment_manager_products'
str_date = today.strftime('%m%d')
target_path = 'C:/Users/admin/Desktop/%s'%str_date
l = []


for d in os.listdir(source_path):
    current_src = source_path + '/' + d
    if os.path.isdir(current_src):
        current_target = target_path + '/' + d
        os.makedirs(current_target, exist_ok=True)
        flag = True
        for f in os.listdir(current_src):
            file_path = current_src + '/' + f
            make_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            if os.path.isfile(file_path) and make_time.date() == today.date():
                shutil.copy(file_path, current_target + '/' + f)
                flag = False
            elif d == 'yh_apama':
                current_target_ = current_target + '/' + f
                current_src_ = current_src + '/' + f
                os.makedirs(current_target_, exist_ok=True)
                for f_ in os.listdir(current_src_):
                    file_path_ = current_src_ + '/' + f_
                    make_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path_))  # 复制文件是否今天生成
                    if os.path.isfile(file_path_) and make_time.date() == today.date():
                        shutil.copy(file_path_, current_target_ + '/' + f_)
                        flag = False
        if flag:
            l.append(d)
print(l)
with open(target_path+'/'+'broker_wihtout_post.txt', 'w') as f:
    f.write(str(l))
# N1.输出list可以复制到 broker_c_without_postdata, broker_m_without_postdata