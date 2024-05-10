# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os,codecs,json,copy

class MainsiteCrawlerPipeline:

    def open_spider(self, spider):
        if os.path.exists(spider.crawl_folder_path) is False:
            os.makedirs(spider.crawl_folder_path)

    #处理每一个item用的函数
    def process_item(self, item, spider):
        spider.pbar_download.update(1)
        for field in item.fields:
            item.setdefault(field, None)
        file_path = os.path.join(spider.crawl_folder_path,item['ID']+'.json')
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='UTF-8') as f:
                write_in = json.load(f)
            write_in.append(dict(item))
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
        work_dict = spider.work_dict
        
        workdict_path = os.path.join(spider.crawl_folder_path, 'workdict.json')
        if not(os.path.exists(workdict_path)):
            ### execute only when dualsites spider runs
            all_ids = []
            for key, value in work_dict.items():
                all_ids.append({"ID":key,"release_dtl":value})
            json_file = codecs.open(workdict_path, 'w+', encoding='UTF-8')
            #self.json_file.write('[\n')
            item_json = json.dumps(all_ids, ensure_ascii=False)
            json_file.write(item_json)
            json_file.close()

            RJ2IDs_path = os.path.join(spider.crawl_folder_path, 'RJ2IDs.json')
            json_file = codecs.open(RJ2IDs_path, 'w+', encoding='UTF-8')
            item_json = json.dumps(spider.RJ2IDs, ensure_ascii=False)
            json_file.write(item_json)
            json_file.close()

        print('Checking data integrity...')
        missing_ids = []
        for key, value in work_dict.items():
            # if JSON file exists
            json_file_path = os.path.join(spider.crawl_folder_path, f"{key}.json")
            if not os.path.exists(json_file_path):
                missing_ids.append({"ID" : key,"release_dtl" : value})
                continue
            # reading JSON
            try:
                with open(json_file_path, "r", encoding="utf-8") as json_file:
                    json_data = json.load(json_file)
            except json.JSONDecodeError:
                print(f"File {json_file_path} decoding ERROR\n")
                continue
            # checking site details
            release_dtl = copy.deepcopy(value)
            for each in json_data:
                if each['Site'][0] == 'D':
                    for sub_key, sub_value in value.items():
                        if sub_key[0] == 'd':
                            release_dtl.pop(sub_key)
                            break
                else:
                    release_dtl.pop(each['Site'])
            if(release_dtl):
                missing_ids.append({"ID" : key, "release_dtl" : release_dtl})
        ### when scanning, all RJ and DM ids information under the same dojin ID will be saved to the Missing file if any defection found

        if(len(missing_ids)):
            print(f"{len(missing_ids)} IDs Missing")
            missing_file_path = os.path.join(spider.crawl_folder_path, 'missing_ids.json')
            missing_json_file = codecs.open(missing_file_path, 'w+', encoding='UTF-8')
            item_json = json.dumps(missing_ids, ensure_ascii=False)
            missing_json_file.write(item_json)
            # 关闭文件
            missing_json_file.close()
        else:
            print("All IDs Checked!")