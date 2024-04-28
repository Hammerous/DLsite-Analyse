from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
import os,time

#folder_path = r'D:\2024Spring\DLsite-Analysis\batches'
#folder_path = r'D:\2024Spring\DLsite-Analysis\Missing_batches'
#folder_path = r'D:\2024Spring\DLsite-Analysis\batches1'
#folder_path = r'D:\2024Spring\DLsite-Analysis\batches2'

def sleep(_, duration=1):
    print(f'sleeping for: {duration}')
    time.sleep(duration)  # block here

i = 0
def crawl_main(runner):
    global i,file_paths
    if(i < len(file_paths)):
        print('\nCrawl Mission {0}/{1}: {2}'.format(i+1,len(file_paths),file_paths[i][0]))
        d = runner.crawl('dualsites_spider',file_paths[i][0])
        #d.addBoth(sleep)
        d.addBoth(lambda _: crawl_main(runner))
        i+=1
    else:
        os._exit(0)
    return d

def crawl_sub(runner):
    global i,file_paths
    d = runner.crawl('subRJ_Crawler',file_paths[i][1:])
    d.addBoth(lambda _: crawl_sub(runner))

@defer.inlineCallbacks
def crawl(runner):
    for idx in range(len(file_paths)):
        for idx_opt in range(2):
            print('\nCrawl Mission {0}/{1}: {2}'.format(idx+1,len(file_paths),file_paths[idx][0]))
            yield runner.crawl(SpiderNames[idx_opt], file_paths[idx][idx_opt])
    reactor.stop()

def loop_crawl():
    runner = CrawlerRunner(get_project_settings())
    crawl(runner)
    reactor.run() # the script will block here until the last crawl call is finished

file_paths = []
sub_items = ['workdict.json','RJ2IDs.json']
SpiderNames = ['dualsites_spider','subRJ_Crawler']
if __name__ == '__main__':
    dirpath=os.path.dirname(os.path.abspath(__file__))
    # 获取当前路径
    os.chdir(dirpath)         # 切换到当前目录
    if os.path.exists(folder_path) is False:
        raise ValueError("A Folder Path must be provided")
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filebasename = filename.split(".")[0]
            main_file = os.path.join(folder_path, filename)
            sub_file = [os.path.join(folder_path, filebasename+ '\\' +each) for each in sub_items]
            file_paths.append([main_file,sub_file])
    if len(file_paths) == 0:
        raise ValueError("A JSON must be provided")
    loop_crawl()