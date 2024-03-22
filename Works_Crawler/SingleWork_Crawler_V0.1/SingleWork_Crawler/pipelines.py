# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import time,json,codecs,os


class SingleworkCrawlerPipeline:
    Time_TMP = int(0)

    def __init__(self, *args, **kwargs):
        SingleworkCrawlerPipeline.Time_TMP = time.time()    
        local_time = time.localtime(SingleworkCrawlerPipeline.Time_TMP)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        #timeStr_name = time.strftime("%Y%m%d_%H%M%S", local_time)
        print("Program Start Time: "+ timeStr)

    def open_spider(self, spider):
        self.json_file = codecs.open(os.path.join(spider.workID+'.json'), 'w+', encoding='UTF-8')
        self.json_file.write('[\n')

    #处理每一个item用的函数
    def process_item(self, item, spider):
        item_json = json.dumps(dict(item), ensure_ascii=False)
        self.json_file.write('\t' + item_json + ',\n')
        return item
    
    def close_spider(self, spider):
        # 当前文件指针处于文件尾，我们需要首先使用 SEEK 方法，定位文件尾前的两个字符（一个','(逗号), 一个'\n'(换行符)）的位置
        self.json_file.seek(-2, os.SEEK_END)
        # 使用 truncate() 方法，将后面的数据清空
        self.json_file.truncate()
        self.json_file.write('\n]')
        # 关闭文件
        self.json_file.close()
        
        time_now = time.time()
        local_time =  time.localtime(time_now)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        print("Pipeline End Time: "+ timeStr)
        Running_interval = int(time_now - SingleworkCrawlerPipeline.Time_TMP)
        m, s = divmod(Running_interval, 60)
        h, m = divmod(m, 60)
        print('Total Running Time: %d:%02d:%02d'% (h, m, s))
