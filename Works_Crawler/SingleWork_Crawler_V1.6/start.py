#'''
from scrapy import cmdline
import os
dirpath=os.path.dirname(os.path.abspath(__file__))
# 获取当前路径
os.chdir(dirpath)         # 切换到当前目录
if os.path.exists('all.log'):
    os.remove('all.log')
cmdline.execute("scrapy crawl WorksCrawler -a csv_path=D:\\2024Spring\\DLsite-Analysis\\main_batch\Missing.csv -s LOG_FILE=all.log".split())
#'''
'''
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process
from scrapy.utils.project import get_project_settings
from bilibili_spider.spiders.spider import SpiderSpider

pages = 2

def start(page):
    print(f"page={page}")
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    spider = SpiderSpider()
    process.crawl(spider, page=page)
    process.start()

if __name__ == "__main__":
    for i in range(1,pages+1):
        page = i
        p = Process(target=start, args=(page,))
        p.start()
'''
# 项目不同， 改为你项目的名字