#pipeline of v1.0

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import time,json,codecs,os

class SingleworkCrawlerPipeline:
    Time_TMP = int(0)

    def __init__(self):
        SingleworkCrawlerPipeline.Time_TMP = time.time()    
        local_time = time.localtime(SingleworkCrawlerPipeline.Time_TMP)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        #timeStr_name = time.strftime("%Y%m%d_%H%M%S", local_time)
        print("Program Start Time: "+ timeStr)

    def open_spider(self, spider):
        self.json_file = codecs.open(spider.crawl_folder_path +'.json', 'w+', encoding='UTF-8')
        # 在爬虫开始时，首先写入一个 '[' 符号，构造一个 json 数组
        # 为使得 Json 文件具有更高的易读性，我们辅助输出了 '\n'（换行符）
        self.json_file.write('[\n')

    #处理每一个item用的函数
    def process_item(self, item, spider):
        item_json = json.dumps(dict(item), ensure_ascii=False)
        self.json_file.write('\t' + item_json + ',\n')
        spider.pbar_download.update(1)
        return item
    
    def close_spider(self, spider):
        spider.pbar_request.refresh()
        spider.pbar_download.refresh()
        spider.pbar_request.close()
        spider.pbar_download.close()
        # 在结束后，需要对 process_item 最后一次执行输出的 “逗号” 去除
        # 当前文件指针处于文件尾，我们需要首先使用 SEEK 方法，定位文件尾前的两个字符（一个','(逗号), 一个'\n'(换行符)）的位置
        self.json_file.seek(-2, os.SEEK_END)
        # 使用 truncate() 方法，将后面的数据清空
        self.json_file.truncate()
        # 重新输出'\n'，并输入']'，与 open_spider(self, spider) 时输出的 '['，构成一个完整的数组格式
        self.json_file.write('\n]')

        print('Checking data completency...')
        # 读取JSON文件中的所有ID
        self.json_file.seek(0)  # 移动到文件开头
        try:
            data = json.load(self.json_file)
            json_ids = {int(item['ID']) for item in data}
        except json.JSONDecodeError:
            json_ids = set()
        # 关闭文件
        self.json_file.close()
        # 检查work_dict中的ID是否在json_ids中
        missing_ids = set(spider.work_dict.keys()) - json_ids
        # 如果有缺失的ID，写入一个新的txt文件
        if missing_ids:
            print("Missing: {0}".format(missing_ids))
            with open(spider.crawl_folder_path + '_missing_ids.txt', 'w') as f:
                for missing_id in missing_ids:
                    f.write(str(missing_id) + '\n')
        else:
            print("All IDs Checked!")
        
        time_now = time.time()
        local_time =  time.localtime(time_now)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        print("Pipeline End Time: "+ timeStr)
        Running_interval = int(time_now - SingleworkCrawlerPipeline.Time_TMP)
        m, s = divmod(Running_interval, 60)
        h, m = divmod(m, 60)
        print('Total Running Time: %d:%02d:%02d'% (h, m, s))
