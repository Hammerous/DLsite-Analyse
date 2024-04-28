#'''
from scrapy import cmdline
import os
dirpath=os.path.dirname(os.path.abspath(__file__))
# 获取当前路径
os.chdir(dirpath)         # 切换到当前目录

#file_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_154\\missing_ids.json'
file_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_154.json'
#file_path = 'D:\\2024Spring\\DLsite-Analysis\\dojinDB.json'
#file_path = 'D:\\2024Spring\\DLsite-Analysis\\batches\\missing_ids.json'

if os.path.exists('all.log'):
    os.remove('all.log')
cmdline.execute("scrapy crawl dualsites_spider -a json_path={0} -s LOG_FILE=all.log".format(file_path).split())