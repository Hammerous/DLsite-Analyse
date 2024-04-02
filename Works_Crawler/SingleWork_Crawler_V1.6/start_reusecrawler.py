import os,time
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

folder_path = r'D:\2024Spring\DLsite-Analysis\ID_Crawler\WorkID_inPage_V1.1\CSVs\batches'

'''
i = 0
for i in range(0,len(aid_serial)):
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('spider',aid_serial[i])
    process.start()
'''

def sleep(_, duration=1):
    print(f'sleeping for: {duration}')
    time.sleep(duration)  # block here

i = 0
def crawl(runner):
    global i,file_path
    if(i<len(file_path)):
        print('\nCrawl Mission {0}/{1}'.format(i+1,len(file_path)))
        d = runner.crawl('WorksCrawler',file_path[i])
        #d.addBoth(sleep)
        d.addBoth(lambda _: crawl(runner))
        i+=1
    else:
        os._exit(0)
    return d


def loop_crawl():
    runner = CrawlerRunner(get_project_settings())
    crawl(runner)
    reactor.run()

file_path = []
if __name__ == '__main__':
    i = 0
    dirpath=os.path.dirname(os.path.abspath(__file__))
    # 获取当前路径
    os.chdir(dirpath)         # 切换到当前目录
    if os.path.exists(folder_path) is False:
        raise ValueError("A Folder Path must be provided")
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path.append(os.path.join(folder_path, filename))
    if len(file_path) == 0:
        raise ValueError("A CSV must be provided")
    loop_crawl()