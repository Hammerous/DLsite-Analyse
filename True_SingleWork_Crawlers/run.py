from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os,time,random,multiprocessing
import pandas as pd

def run(key,folder_path):
    global threads_num
    threads_num+=1
    process = CrawlerProcess(settings=get_project_settings())
    process.crawl("WorkCrawler",key,folder_path)
    process.start()
    process.join()
    threads_num-=1

threads_num = 0
threads_max = 4

if __name__== '__main__':
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
    

    '''
    ID_serial = []
    for key,value in work_dict.items():
        ID_serial.append(key)
    while(True):
        if(len(ID_serial)):
            if(threads_num < threads_max-1):
                process = multiprocessing.Process(target=run,args=(ID_serial[-1],folder_path))
                #time.sleep(10)
                process.start()
                del ID_serial[-1]
            else:
                process.join()
                time.sleep(random.random()+1)
        else:
            break
    '''
