import os,csv,json,codecs,ijson

def merge_txt_to_csv(directory_path, csv_file_path):
    with open(csv_file_path, 'w', newline='') as csvfile:
        fieldnames = ['ID']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # 遍历目录下的所有文件
        for filename in os.listdir(directory_path):
            if filename.endswith(".txt"):
                txt_file_path = os.path.join(directory_path, filename)
                # 读取txt文件中的内容并写入CSV文件
                with open(txt_file_path, 'r') as txtfile:
                    for line in txtfile:
                        writer.writerow({'ID': line.strip()})
    print("ALL TXTs Merged")

def merge_json2json(directory_path, json_file_path):
    missing_json_file = codecs.open(json_file_path, 'w+', encoding='UTF-8')
    filename = 'missing_ids.json'
    missing_data = []
    for foldername in os.listdir(directory_path):
        folder_path = os.path.join(directory_path, foldername)
        if os.path.isdir(folder_path):
            missing_file_path = os.path.join(folder_path, filename)
            if os.path.exists(missing_file_path):
                with open(missing_file_path, 'r') as missing_file:
                    missing_json_data = json.load(missing_file)
                    for each in missing_json_data:
                        missing_data.append(each)
    item_json = json.dumps(missing_data, ensure_ascii=False)
    missing_json_file.write(item_json)
    print("ALL Missing JSONs Merged, {0} records loaded".format(len(missing_data)))

def missingIDS_check(directory_path, json_file_path):
    work_dict = {}
    print('Loading {0} ...'.format(json_file_path))
    with open(json_file_path,"r+", encoding='utf-8') as f:
            for record in ijson.items(f, 'item'):
                ID = record['ID']
                release_dtl = record['release_dtl']
                urls = {}
                for key,value in release_dtl.items():
                    urls[key] = {'url':value['url']}
                work_dict[ID] = urls
    print('{0} records loaded'.format(len(work_dict.items())))
    print('Checking data integrity...')
    missing_ids = []
    for key, value in work_dict.items():
        # if JSON file exists
        json_file_path = os.path.join(directory_path, f"{key}.json")
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
        missing_json_file = codecs.open(os.path.join(directory_path, 'missing_ids.json'), 'w+', encoding='UTF-8')
        item_json = json.dumps(missing_ids, ensure_ascii=False)
        missing_json_file.write(item_json)
        # 关闭文件
        missing_json_file.close()
    else:
        print("All IDs Checked!")

os.chdir(os.path.dirname(os.path.abspath(__file__)))
#merge_txt_to_csv(r'main_batch', r'main_batch\Missing.csv')
#missingIDS_check('batches', 'dojinDB.json')
merge_json2json(r'batches2', r'batches2\Missing.json')