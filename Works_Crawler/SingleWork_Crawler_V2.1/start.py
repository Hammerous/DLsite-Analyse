#'''
from scrapy import cmdline
import os
dirpath=os.path.dirname(os.path.abspath(__file__))
# 获取当前路径
os.chdir(dirpath)         # 切换到当前目录
if os.path.exists('all.log'):
    os.remove('all.log')
cmdline.execute("scrapy crawl WorksCrawler -a csv_path=D:\\2024Spring\\DLsite-Analysis\\test_batch2\\batch_102.csv -s LOG_FILE=all.log".split())