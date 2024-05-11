#'''
from scrapy import cmdline
import os,shutil
dirpath=os.path.dirname(os.path.abspath(__file__))
# 获取当前路径
os.chdir(dirpath)         # 切换到当前目录

#file_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_84\\missing_ids.json'
file_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_102.json'
#file_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_84.json'
workdict_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_84\\workdict.json'
RJ2ID_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\batch_84\\RJ2IDs.json'
#file_path = 'D:\\2024Spring\\DLsite-Analysis\\test_batch\\84_ids.json'
#file_path = 'D:\\2024Spring\\DLsite-Analysis\\dojinDB.json'
#file_path = 'D:\\2024Spring\\DLsite-Analysis\\batches\\missing_ids.json'

if os.path.exists('all.log'):
    os.remove('all.log')
folder_path = os.path.splitext(file_path)[0]
#if os.path.exists(folder_path):
#    shutil.rmtree(folder_path)
cmdline.execute("scrapy crawl dualsites_spider -a json_path={0} -s LOG_FILE=all.log".format(file_path).split())
#cmdline.execute("scrapy crawl subRJ_Crawler -a json_path={0} -a RJ2ID_path={1} -s LOG_FILE=all.log".format(workdict_path, RJ2ID_path).split())