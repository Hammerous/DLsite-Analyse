import os,shutil,json,codecs

def merge_json_files(directory_path, output_json_file):  
    total_records = 0
    # 遍历目录下的所有文件
    json_file = codecs.open(output_json_file, 'w+', encoding='UTF-8')
    json_file.write('[\n')
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            json_file_path = os.path.join(directory_path, filename)
            # 读取JSON文件中的内容并添加到列表中
            with open(json_file_path, 'r', encoding='UTF-8') as sub_json_file:
                data = json.load(sub_json_file)
                for record in data:
                    item_json = json.dumps(dict(record), ensure_ascii=False)
                    json_file.write('\t' + item_json + ',\n')
                    total_records+=1
    
    # 将整合后的数据写入输出JSON文件
    # 在结束后，需要对 process_item 最后一次执行输出的 “逗号” 去除
    # 当前文件指针处于文件尾，我们需要首先使用 SEEK 方法，定位文件尾前的两个字符（一个','(逗号), 一个'\n'(换行符)）的位置
    json_file.seek(-2, os.SEEK_END)
    # 使用 truncate() 方法，将后面的数据清空
    json_file.truncate()
    # 重新输出'\n'，并输入']'，与 open_spider(self, spider) 时输出的 '['，构成一个完整的数组格式
    json_file.write('\n]')
    json_file.close()
    print("ALL JSONs Merged")

import ijson
def count_records(json_path):
    with open(json_path, 'r', encoding='UTF-8') as f:
      count = sum(1 for item in ijson.items(f, 'item'))
      print(count)
    return count

def url_extract(json_path, output_json_file):
    total_records = 0
    output = codecs.open(output_json_file, 'w+', encoding='UTF-8')
    output.write('[\n')
    with open(json_path,"r+", encoding='utf-8') as f:
        for record in ijson.items(f, 'item'):
            ID = record['ID']
            release_dtl = record['release_dtl']
            urls = {}
            for key,value in release_dtl.items():
                if(key[:2] == 'd_'):
                    urls['FANZA'] = value['url']
                elif(key[:2] == 'RJ'):
                    urls['DLsite'] = value['url']
                else:
                    urls[key] = value['url']
            url_info = {ID:urls}
            item_json = json.dumps(url_info, ensure_ascii=False)
            output.write('\t' + item_json + ',\n')
            total_records+=1

    output.seek(-2, os.SEEK_END)
    # 使用 truncate() 方法，将后面的数据清空
    output.truncate()
    # 重新输出'\n'，并输入']'，与 open_spider(self, spider) 时输出的 '['，构成一个完整的数组格式
    output.write('\n]')
    output.close()
    return total_records

def unfold_json_files(directory_path):  
    total_records = 0
    # 遍历目录下的所有文件
    for foldername in os.listdir(directory_path):
        folder_path = os.path.join(directory_path, foldername)
        if os.path.isdir(folder_path):
            for filename in os.listdir(folder_path):
                if filename == 'missing_ids.json':
                    continue
                if filename.endswith(".json"):
                    json_file_path = os.path.join(folder_path, filename)
                    shutil.move(json_file_path,directory_path)
                    total_records+=1
    print("ALL JSON Folders Unfolded\n{0} Files in total".format(total_records))

os.chdir(os.path.dirname(os.path.abspath(__file__)))
#merge_json_files(r'D:\2024Spring\DLsite-Analysis\ALL Batches', 'dojinDB.json')
#print("{0} records loaded".format(count_records('test_batch\\test_batch.json')))
#unfold_json_files(r'D:\2024Spring\DLsite-Analysis\batches')
#json_path = 'test_batch\\test_batch.json'
#output_json_file = 'test_batch\\test_url.json'
json_path = 'dojinDB.json'
output_json_file = 'dojin_urls.json'
print(count_records('data_20240428.json'))
print(count_records('data_20240404.json'))
#print("{0} records extracted".format(url_extract(json_path, output_json_file)))