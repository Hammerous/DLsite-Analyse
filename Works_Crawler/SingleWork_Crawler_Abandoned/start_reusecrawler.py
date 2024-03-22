#'''
from scrapy import cmdline
import pandas as pd
import os,time
from scrapy.crawler import CrawlerProcess,CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

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
    global i,ID_serial,folder_path
    i+=1
    if(i<len(ID_serial)):
        d = runner.crawl('WorkCrawler',ID_serial[i],folder_path)
        #d.addBoth(sleep)
        d.addBoth(lambda _: crawl(runner))
    return d


def loop_crawl():
    runner = CrawlerRunner(get_project_settings())
    crawl(runner)
    reactor.run()

ID_serial = []
folder_path = ''

if __name__ == '__main__':
    dirpath=os.path.dirname(os.path.abspath(__file__))
    # 获取当前路径
    os.chdir(dirpath)         # 切换到当前目录

    #ID = 446225
    folder_path = os.path.join(dirpath,'Json')
    csv_path = r'C:\Users\User\Desktop\DLsite-Analyse-main\SingleWork_Crawler\日間ランキング.csv'

    if os.path.exists(folder_path) is False:
        os.makedirs(folder_path)
    work_dict = {}
    if csv_path is None:
        raise ValueError("A CSV Path must be provided")
    # CSV read in
    try:
        df = pd.read_csv(csv_path)
        # check required data
        if "ID" in df.columns and "Rank" in df.columns and "Sales" in df.columns:
            for index, row in df.iterrows():
                work_dict[row["ID"]] = {"Rank": str(row["Rank"]), "Sales": str(row["Sales"])}
        else:
            missing_columns = [col for col in ["ID", "rank", "sales"] if col not in df.columns]
            print(f"Missing columns {missing_columns} in {csv_path}")
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
    
    for key,value in work_dict.items():
        ID_serial.append(key)
        #cmdline.execute('scrapy crawl WorkCrawler -a workID={0} -a folder={1} -s LOG_FILE=all.log'.format(key,folder_path).split())
    loop_crawl()