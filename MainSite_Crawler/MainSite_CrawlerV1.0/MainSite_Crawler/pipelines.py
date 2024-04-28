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
        self.json_file.close()
        return item
    
    def close_spider(self, spider):
        spider.pbar_request.refresh()
        spider.pbar_download.refresh()
        spider.pbar_request.close()
        spider.pbar_download.close()

        print('Checking data integrity...')
        work_dict = spider.work_dict
        missing_ids = []
        for key, value in work_dict.items():
            # if JSON file exists
            json_file_path = os.path.join(spider.crawl_folder_path, f"{key}.json")
            if not os.path.exists(json_file_path):
                missing_ids.append({"ID":key,"release_dtl":work_dict[key]})
                continue
            # reading JSON
            try:
                with open(json_file_path, "r", encoding="utf-8") as json_file:
                    json_data = json.load(json_file)
            except json.JSONDecodeError:
                print(f"File {json_file_path} decoding ERROR\n")
                continue
            # checking site details
            if len(json_data) != len(value):
                missing_ids.append({"ID":key,"release_dtl":work_dict[key]})
        if(len(missing_ids)):
            print(f"{len(missing_ids)} IDs Missing")
            missing_json_file = codecs.open(os.path.join(spider.crawl_folder_path, 'missing_ids.json'), 'w+', encoding='UTF-8')
            item_json = json.dumps(missing_ids, ensure_ascii=False)
            missing_json_file.write(item_json)
            # 关闭文件
            missing_json_file.close()
        else:
            print("All IDs Checked!")