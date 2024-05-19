import ijson,json,codecs,os

def check_Folder2JSON(directory_path):
    Folder2JSON_dict = {}
    missing_folder = set()
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            foldername = filename.split(".")[0]
            folder_dir = os.path.join(directory_path,foldername)
            filepath = os.path.join(directory_path,filename)
            if os.path.exists(folder_dir):
                Folder2JSON_dict[filename] = (filepath,folder_dir)
            else:
                missing_folder.add(filename)
    if(len(missing_folder)):
        print("JSON Batch Folder Missing: {0}".format(missing_folder))
        return False
    else:
        print("ALL JSON's Batch Folders Loaded!")
        return Folder2JSON_dict


def ID_set(childFolder_path):
    ID_set = set()
    for filename in os.listdir(childFolder_path):
        if filename.endswith(".json"):
            ID = filename.split(".")[0]
            if(ID.isdigit()):
                ID_set.add(ID)
    return ID_set

from decimal import Decimal
def decimal_to_str(obj):
    if isinstance(obj, Decimal):
        return str(obj)

def batches_screening(directory_path, finalJs_path):
    Folder2JSON_dict = check_Folder2JSON(directory_path)
    if(Folder2JSON_dict):
        MainFile = codecs.open(finalJs_path, 'w+', encoding='UTF-8')
        MainFile.write('[\n')
        for batch, paths in Folder2JSON_dict.items():
            childFolder_path = paths[1]
            id_set = ID_set(childFolder_path)
            print('Loading {0} ...'.format(batch))
            with open(paths[0],"r+", encoding='UTF-8') as f:
                for record in ijson.items(f, 'item'):
                    ID = record['ID']
                    if ID in id_set:
                        appendINFO_path = os.path.join(childFolder_path,"{0}.json".format(ID))
                        with open(appendINFO_path, 'r',encoding='UTF-8') as sub_f:
                            sub_data = json.load(sub_f)
                            record['SiteInfo'] = sub_data
                    item_json = json.dumps(record, ensure_ascii=False, default=decimal_to_str)
                    MainFile.write('\t' + item_json + ',\n')
        MainFile.seek(-2, os.SEEK_END)
        MainFile.truncate()
        MainFile.write('\n]')
        MainFile.close()
        print("ALL json files Merged!")

if __name__=='__main__':
    #directory_path = 'test_batch'
    #finalJs_path = 'test.json'
    directory_path = 'main_batch'
    finalJs_path = 'data_20240518.json'
    batches_screening(directory_path, finalJs_path)