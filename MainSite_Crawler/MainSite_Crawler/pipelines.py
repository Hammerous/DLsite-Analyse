# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os,codecs,json


class MainsiteCrawlerPipeline:

    def open_spider(self, spider):
        if os.path.exists(spider.crawl_folder_path) is False:
            os.makedirs(spider.crawl_folder_path)

    #处理每一个item用的函数
    def process_item(self, item, spider):
        for field in item.fields:
            item.setdefault(field, "")
        file_path = os.path.join(spider.crawl_folder_path,item['ID']+'.json')
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='UTF-8') as f:
                previous = json.load(f)
            write_in = [previous[0],dict(item)]
        else:
            write_in = [dict(item)]
        self.json_file = codecs.open(file_path, 'w+', encoding='UTF-8')
        #self.json_file.write('[\n')
        item_json = json.dumps(write_in, ensure_ascii=False)
        self.json_file.write(item_json)
        #self.json_file.write('\t' + item_json + ',\n')
        # 在结束后，需要对 process_item 最后一次执行输出的 “逗号” 去除
        # 当前文件指针处于文件尾，我们需要首先使用 SEEK 方法，定位文件尾前的两个字符（一个','(逗号), 一个'\n'(换行符)）的位置
        #self.json_file.seek(-2, os.SEEK_END)
        # 使用 truncate() 方法，将后面的数据清空
        #self.json_file.truncate()
        #self.json_file.write('\n]')
        # 关闭文件
        self.json_file.close()
        return item
    
    def close_spider(self, spider):
        spider.pbar_request.refresh()
        spider.pbar_download.refresh()
        spider.pbar_request.close()
        spider.pbar_download.close()

        print('Checking data integrity...')
        work_dict = spider.work_dict
        missing_ids = {}
        for key, value in work_dict.items():
            # 检查json文件是否存在
            json_file_path = os.path.join(spider.crawl_folder_path, f"{key}.json")
            if not os.path.exists(json_file_path):
                missing_ids[key] = work_dict[key]
                continue
            # 读取json文件内容
            try:
                with open(json_file_path, "r", encoding="utf-8") as json_file:
                    json_data = json.load(json_file)
            except json.JSONDecodeError:
                print(f"File {json_file_path} decoding ERROR\n")
                continue
            # 检查json文件中的值是否与字典中的值一致
            if len(json_data) != len(value):
                missing_ids[key] = work_dict[key]
        if(len(missing_ids)):
            print(f"{len(missing_ids)} IDs Missing")
            missing_json_file = codecs.open(os.path.join(spider.crawl_folder_path, 'missing_ids.json'), 'w+', encoding='UTF-8')
            missing_json_file.write('[\n')
            item_json = json.dumps(missing_ids, ensure_ascii=False)
            missing_json_file.write('\t' + item_json + ',\n')
            # 在结束后，需要对 process_item 最后一次执行输出的 “逗号” 去除
            # 当前文件指针处于文件尾，我们需要首先使用 SEEK 方法，定位文件尾前的两个字符（一个','(逗号), 一个'\n'(换行符)）的位置
            missing_json_file.seek(-2, os.SEEK_END)
            # 使用 truncate() 方法，将后面的数据清空
            missing_json_file.truncate()
            missing_json_file.write('\n]')
            # 关闭文件
            missing_json_file.close()
        else:
            print("All IDs Checked!")