import pymysql,ijson,datetime,re,json,codecs,os,decimal
from tqdm.autonotebook import tqdm

def dumps(item: dict) -> str:
    return json.dumps(item, default=default_type_error_handler, ensure_ascii=False)

def default_type_error_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError

def set_global():
    cursor.execute('set GLOBAL innodb_flush_log_at_trx_commit = 2;')
    cursor.execute('set GLOBAL innodb_log_buffer_size = 8*1024*1024;')
    cursor.execute('set global innodb_buffer_pool_size = 1024*1024*1024;')
    cursor.execute('SET GLOBAL innodb_thread_concurrency = 64;')
    cursor.execute('set global innodb_flush_neighbors = 0;')
    cursor.execute('set global innodb_io_capacity = 10000;')
    cursor.execute('set global bulk_insert_buffer_size = 128*1024*1024;')
    cursor.execute('set global sync_binlog = 0;')
    cursor.execute('set global innodb_file_per_table = on;')
    cursor.execute('set GLOBAL innodb_autoextend_increment = 256*1024*1024;')

def json_writein(filepath, json_data):
    json_file = codecs.open(filepath, 'w+', encoding='UTF-8')
    item_json = dumps(json_data)
    json_file.write(item_json)
    json_file.close()

import difflib
def similar_diff_qk_ratio(str1, str2):
     return difflib.SequenceMatcher(None, str1, str2).quick_ratio()

def date2jd(date_string):
    if(date_string):
        date_obj = datetime.datetime.strptime(date_string, "%Y-%m-%d")
        julian_day = date_obj.toordinal() + 1721424.5
        return int(julian_day + 1)
    return None

def jd2date(julian_day):
    ordinal_day = julian_day - 1721424.5
    date = datetime.datetime.fromordinal(int(ordinal_day))
    return date.strftime("%Y-%m-%d")

def has_duplicate_start(lst):
    seen = set()
    for item in lst:
        if item[0] in seen:
            return True
        seen.add(item[0])
    return False

def create_relationChart(chart1,chart2,type1,type2):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS {0}2{1} (
        {0}_id {2} NOT NULL,
        {1}_id {3} NOT NULL,
        FOREIGN KEY ({0}_id) REFERENCES {0}(id),
        FOREIGN KEY ({1}_id) REFERENCES {1}(id),
        UNIQUE ({0}_id, {1}_id)
    )
    """.format(chart1,chart2,type1,type2))

def simple_element(parent, ele_name, name_length):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS {0} (
        id INT PRIMARY KEY,
        name VARCHAR({1}) BINARY,
        UNIQUE (name) 
    ) 
    """.format(ele_name, name_length))
    create_relationChart(parent, ele_name, 'INT', 'INT')

def create_tables():
    cursor.execute("""
    CREATE TABLE Work_main (
                   id INT AUTO_INCREMENT PRIMARY KEY,
                   title VARCHAR(255) BINARY NOT NULL,
                   UNIQUE (title)
    )
    """)
    cursor.execute("""
    CREATE TABLE doujinWork (
                id INT PRIMARY KEY,
                main_id INT,
                DM_id VARCHAR(20) DEFAULT NULL,
                RJ_id VARCHAR(20) DEFAULT NULL,
                UNIQUE (id)
    )
    """)
    cursor.execute("""
    CREATE TABLE Tag (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(128) BINARY,
        UNIQUE (name)
    )
    """)
    cursor.execute("""
    CREATE TABLE doujinWork2Tag (
        doujinWork_id INT NOT NULL,
        genre_count INT DEFAULT NULL,
        Tag_id INT NOT NULL,
        FOREIGN KEY (doujinWork_id) REFERENCES doujinWork(id),
        FOREIGN KEY (Tag_id) REFERENCES Tag(id),
        UNIQUE (doujinWork_id, Tag_id)
    )
    """)
    simple_element('doujinWork','Genre', 64)
    simple_element('doujinWork','Series', 64)
    simple_element('doujinWork','Circle', 64)
    simple_element('doujinWork','CV', 64)
    simple_element('doujinWork','Creator', 64)
    simple_element('doujinWork','Music', 64)
    simple_element('doujinWork','Senario', 64)
    #simple_element('doujinWork','WorkType', 64)
    create_relationChart('Circle', 'Series', 'INT', 'INT')
    #FOREIGN KEY (main_id) REFERENCES Work_main(id)
    cursor.execute("""
    CREATE TABLE RJWork (
                site_accessibility BOOLEAN NOT NULL,
                id VARCHAR(20) NOT NULL PRIMARY KEY,
                title VARCHAR(255) BINARY NOT NULL,
                work_id INT DEFAULT NULL,
                doujin_id INT DEFAULT NULL,
                released_date INT DEFAULT NULL,
                ref_price INT UNSIGNED DEFAULT NULL,
                `Sales` INT DEFAULT NULL ,
                `Favorites` INT DEFAULT NULL,
                `Reviews` INT DEFAULT NULL,
                `CmtReviews` INT DEFAULT NULL,
                `Rating` Float(3,2) DEFAULT NULL,
                `Rate_5` INT DEFAULT NULL,
                `Rate_4` INT DEFAULT NULL,
                `Rate_3` INT DEFAULT NULL,
                `Rate_2` INT DEFAULT NULL,
                `Rate_1` INT DEFAULT NULL,
                Other_info VARCHAR(64) NULL,
                Age_Restrict VARCHAR(10) NULL,
                language VARCHAR(20) NOT NULL,
                UNIQUE (id)
    )
    """)
    cursor.execute("""
    CREATE TABLE DMWork (
                site_accessibility BOOLEAN NOT NULL,
                id VARCHAR(20) NOT NULL PRIMARY KEY,
                title VARCHAR(255) BINARY NOT NULL,
                work_id INT DEFAULT NULL,
                doujin_id INT DEFAULT NULL,
                released_date INT DEFAULT NULL,
                ref_price INT UNSIGNED DEFAULT NULL,
                `Worktype` VARCHAR(64) NULL,
                `Sales` INT DEFAULT NULL,
                `Favorites` INT DEFAULT NULL,
                `Reviews` INT DEFAULT NULL,
                `CmtReviews` INT DEFAULT NULL,
                `Rating` Float(3,2) DEFAULT NULL,
                `Rate_5` INT DEFAULT NULL,
                `CmtRate_5` INT DEFAULT NULL,
                `Rate_4` INT DEFAULT NULL,
                `CmtRate_4` INT DEFAULT NULL,
                `Rate_3` INT DEFAULT NULL,
                `CmtRate_3` INT DEFAULT NULL,
                `Rate_2` INT DEFAULT NULL,
                `CmtRate_2` INT DEFAULT NULL,
                `Rate_1` INT DEFAULT NULL,
                `CmtRate_1` INT DEFAULT NULL,
                UNIQUE (id)
    )
    """)
    ### The unit of JPY is an integer
    cursor.execute("""
    CREATE TABLE SalesRecord (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                time_resolution BOOLEAN NOT NULL,
                jd_time INT UNSIGNED NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                doujin_id INT NOT NULL,
                code_id VARCHAR(20) NOT NULL,
                price INT DEFAULT NULL,
                amount INT DEFAULT NULL,
                delta_amount INT DEFAULT NULL,
                delta_revenue INT DEFAULT NULL
    )
    """)
    cursor.execute("""
    CREATE TABLE doujinPrice (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                doujin_id INT NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                jd_time INT UNSIGNED NOT NULL,
                code_id VARCHAR(20) NOT NULL,
                price INT UNSIGNED NULL
    )
    """)
    cursor.execute("""
    CREATE TABLE RJWorkofFame (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                `RJ_id` VARCHAR(20) NOT NULL,
                `category` VARCHAR(32) NOT NULL,
                `type` VARCHAR(32) NOT NULL,
                `rank` INT UNSIGNED NOT NULL,
                `time_str` VARCHAR(12) DEFAULT NULL,
                FOREIGN KEY (RJ_id) REFERENCES RJWork(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE DMWorkofFame (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                `DM_id` VARCHAR(20) NOT NULL,
                `type` VARCHAR(10) NOT NULL,
                `rank` INT UNSIGNED NOT NULL,
                FOREIGN KEY (DM_id) REFERENCES DMWork(id)
    )
    """)
    db.commit()

def create_indexs():
    print('Creating index of Sales Record sequences ...')
    cursor.execute(f"ALTER TABLE SalesRecord ADD INDEX sales_idx (doujin_id, code_id, jd_time);")
    cursor.execute(f"ALTER TABLE doujinPrice ADD INDEX price_idx (doujin_id, code_id, jd_time);")
    
    print('Creating index of relation Charts ...')
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Tag'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Genre'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Series'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Circle'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','CV'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Creator'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Music'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','Senario'))
    #cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','WorkType'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Circle','Series'))
    cursor.execute("ALTER TABLE doujinPrice ADD INDEX work2pricetime (doujin_id, jd_time);")

    print('Creating index of Entities ...')
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Work_main'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id, main_id);".format('doujinWork'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Series'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Circle'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('CV'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Creator'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Music'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Senario'))

def insert_dictID(parent_dict, relation_lst, exist_item, ID):
    if(len(exist_item)):
        max_id = len(parent_dict)
        insert_item = exist_item.difference(parent_dict)                                                # [Performance Waring: this step takes a lot of computation]
        exist_item = exist_item.difference(insert_item)
        for i, each in enumerate(insert_item, 1):
            this_id = max_id + i
            parent_dict[each] = this_id
            relation_lst.append((ID, this_id))
        relation_lst.extend([(ID, parent_dict[each]) for each in exist_item])

def insert_Tags(parent_Tags, Tag_relation_lst, Tags_dict, ID):
    if(len(Tags_dict)):
        max_id = len(parent_Tags)
        exist_Tags = set(Tags_dict.keys())
        insert_Tags = exist_Tags.difference(parent_Tags)                                                # [Performance Waring: this step takes a lot of computation]
        exist_Tags = exist_Tags.difference(insert_Tags)
        for i, each in enumerate(insert_Tags, 1):
            this_id = max_id + i
            parent_Tags[each] = this_id
            Tag_relation_lst.append((ID, Tags_dict[each], this_id))
        Tag_relation_lst.extend([(ID, Tags_dict[each], parent_Tags[each]) for each in exist_Tags])
     
def iter_count(json_path):
    from itertools import (takewhile, repeat)
    buffer = 1024 * 1024 * 4
    with open(json_path, 'r', encoding='UTF-8') as f:
        buf_gen = takewhile(lambda x: x, (f.read(buffer) for _ in repeat(None)))
        return sum(buf.count('\t') for buf in buf_gen)

def insert_tmpdict(tmpdict, parent_entity):
    for key, value in tmpdict.items():
        print("Writing Entities related to doujinWork... --item: '{0} items to {1}'".format(key, parent_entity))
        cursor.executemany("INSERT DELAYED INTO {0} (id, name) VALUES (%s, %s);".format(key), [(value, key[:64]) for key, value in value[0].items()])
        cursor.executemany("INSERT DELAYED INTO {1}2{0} ({1}_id, {0}_id) VALUES (%s, %s);".format(key, parent_entity), set(value[1]))
        db.commit()

daily_type = {'beforelast','lastmonth','thismonth','30days'}

import numpy as np
def process_array(data):
    #data =  np.ascontiguousarray(data)
    data_idx = np.argsort(data[:,0])
    data = data[data_idx]
    duplicate_indices = np.where(data[:,0][1:] == data[:,0][:-1])[0] + 1
    if duplicate_indices.size:
        remove_lst = []
        for group in np.split(duplicate_indices, np.where(np.diff(duplicate_indices) != 1)[0]+1):
            this_group = np.insert(group,0,group[0]-1)
            amount_arr = data[this_group,2]
            if not np.all(amount_arr == amount_arr[0]):
                this_group = np.delete(this_group, np.argmin(np.abs(amount_arr - (data[this_group[0]-1,2] + data[this_group[-1]+1,2])/2)))
            else:
                this_group = group
            remove_lst.extend(this_group)
        data = np.delete(data, remove_lst, axis = 0)
        data_idx = np.delete(data_idx, remove_lst)
    delta_amount = np.insert(np.diff(data[:, 2]),0,data[0,2])
    return np.column_stack((delta_amount, delta_amount * data[:, 1])), data_idx

import fractions
def record_extract(doujin_id, info, is_daily):
    time_dict = {}
    for code_id, detail in info.items():
        if detail:
            detail = np.array([(is_daily, each[2], doujin_id, code_id, int(each[3]), int(each[0]), float(each[1]))for each in detail])
            arr = np.array(detail[:,6],dtype=float)
            time_arr = np.array(detail[:,4],dtype=int)
            not_integer_bool = (arr - arr.astype(int)).astype(bool)
            not_integer_idx = np.where(not_integer_bool)[0]
            if arr.size > 1:
                if not_integer_idx.size:
                    if not_integer_idx[0] == 0:
                        slope = (arr[0] - arr[1])/(time_arr[0] - time_arr[1])
                        fraction = fractions.Fraction.from_float(slope).limit_denominator()
                        possible_range = np.arange(fraction.denominator)*slope + arr[0]
                        delta_arr = np.abs(possible_range - np.rint(possible_range))
                        min_idx = np.argmin(delta_arr)
                        if(delta_arr[min_idx] > 1e-6):
                            raise ValueError("Record at {0} incorrect: {1}".format(code_id, detail))
                        date = time_arr[0] + min_idx
                        detail = np.vstack((detail,np.array((is_daily,jd2date(date),doujin_id, code_id,\
                                                    date, detail[-1][5], possible_range[min_idx]))))
                                                    # time_resolution, time_str, doujin_id, code_id, jd_time, price, amount
                    if not_integer_idx[-1] == arr.size - 1:
                        slope = (arr[-1] - arr[-2])/(time_arr[-2] - time_arr[-1])
                        fraction = fractions.Fraction.from_float(slope).limit_denominator()
                        if(fraction.denominator < 3650):
                            possible_range = np.arange(fraction.denominator) * slope + arr[-1]
                            delta_arr = np.abs(possible_range - np.rint(possible_range))
                            min_idx = np.argmin(delta_arr)
                            if(delta_arr[min_idx] > 1e-6):
                                raise ValueError("Record at {0} incorrect: {1}".format(code_id, detail))
                            date = time_arr[-1] - min_idx
                            detail = np.vstack((detail,np.array((is_daily,jd2date(date),doujin_id, code_id,\
                                                        date, detail[-1][5], possible_range[min_idx]))))
                detail = np.delete(detail, not_integer_idx, axis = 0)
            detail[:,6] = np.array(detail[:,6],dtype = float).astype(int)
        time_dict[code_id] = detail.tolist()
    return time_dict

def insert_sales_history(ref_code_dict, historyData, doujin_id, time_lst):
    # time_resolution, time_str, doujin_id, code_id, jd_time, price, amount, (delta_amount, delta_revenue)
    valid_time_dict = {}
    expelled_records = {}
    for type, record in historyData.items():
        if record:
            is_daily = int(type in daily_type)
            for sitetype, data in record.items():
                ref_code = ref_code_dict[sitetype]
                time_dict = record_extract(doujin_id, data, is_daily)
                valid_time_dict[ref_code] = valid_time_dict.get(ref_code, []) + time_dict.pop(ref_code, [])
                expelled_records.update({k: expelled_records.get(k, []) + v for k, v in time_dict.items() if v})
    result_lst = []
    for records in valid_time_dict.values():
        if records:
            this_records = np.array(records)
            arr = np.array(this_records[:,4:], dtype=int)
            result, idx_array = process_array(arr)
            this_records = np.column_stack((this_records[idx_array], result)).tolist()
            result_lst.extend(this_records)
    time_lst.extend(result_lst)
    return expelled_records
    
def insert_price_history(priceData, doujin_id, Child_dict, price_lst):
    # jd_time, time_str, doujin_id, code_id, price
    priceinfo_lst = [(date2jd(time), time, doujin_id, Child_dict['1' if fanza != 'None' else '2'], fanza if fanza != 'None' else dlsite)
                     for time, dlsite, fanza in zip(priceData['time'], priceData['dlsite'], priceData['fanza'])
                     if fanza != 'None' or dlsite != 'None']
    price_lst.extend(priceinfo_lst)

def RJSiteInfo_extract(RJ_Record, valid_id, this_doujinID, Tags_dict, Common_setDict, RJ_lstDict, RJWorkofFame):
    if(RJ_Record["Reviews_info"]):
        Tags_dict.update(RJ_Record["Reviews_info"])
    if(RJ_Record["Series"]):
        Common_setDict['Series'].extend([s.strip() for s in RJ_Record["Series"]])
    for key in RJ_lstDict.keys():
        if(RJ_Record[key]):
            RJ_lstDict[key].extend(RJ_Record[key])
    if(RJ_Record["Ranking_info"]):
        RJWorkofFame.extend((valid_id, rank_recored['category'], rank_recored['term'], rank_recored['rank'], rank_recored['rank_date'])\
                                for rank_recored in RJ_Record["Ranking_info"])
    if(len(RJ_Record['Rating_info'])):
        Rating_info = (RJ_Record['Rating_info']['5'],RJ_Record['Rating_info']['4'],RJ_Record['Rating_info']['3'],RJ_Record['Rating_info']['2'],RJ_Record['Rating_info']['1'])
    else:
        Rating_info = (None,) * 5
    released_date = None
    if(RJ_Record['Released_date']):
        released_date = date2jd(RJ_Record['Released_date'][0].split()[0].replace("年","-").replace("月","-").replace("日",""))
    return (True, valid_id, RJ_Record['Title'], this_doujinID, released_date, RJ_Record['Price'],\
            RJ_Record['language'], RJ_Record['Sales'] if RJ_Record['Sales'] else None, RJ_Record['Age_Restrict'],\
            RJ_Record['Other_info'], RJ_Record['Favorites'], RJ_Record['Reviews'], RJ_Record['CmtReviews'], RJ_Record['Rating']) + Rating_info

def DMSiteInfo_extract(DM_Record, valid_id, this_doujinID, Tags_dict, Common_setDict, DMWorkofFame):
    if(bool(re.match(r'^d_.*zero$', valid_id))):
        Tags_dict['FANZA無料'] = None 
    if(DM_Record["Sub_Genre"]):
        Common_setDict['Genre'].append(DM_Record["Sub_Genre"].strip())
    if(DM_Record["Series"] != '----'):
        Common_setDict['Series'].append(DM_Record["Series"].strip())
    if(DM_Record["Ranking_info"]):
        DMWorkofFame.extend((valid_id, rank_type, rank) for rank_type, rank in DM_Record["Ranking_info"].items())
    #for key in DM_lstDict.keys():
    #    DM_lstDict[key].append(DM_Record[key])
    if(DM_Record['Rating_info']):
        Rating_info = (DM_Record['Rating_info']['5'][0],DM_Record['Rating_info']['5'][1],DM_Record['Rating_info']['4'][0],DM_Record['Rating_info']['4'][1],\
                        DM_Record['Rating_info']['3'][0],DM_Record['Rating_info']['3'][1],DM_Record['Rating_info']['2'][0],DM_Record['Rating_info']['2'][1],\
                        DM_Record['Rating_info']['1'][0],DM_Record['Rating_info']['1'][1])
    else:
        Rating_info = (None,) * 10
    released_date = None
    if[DM_Record['Released_date']]:
        released_date = date2jd(DM_Record['Released_date'].split()[0].replace("/","-"))
    return (True, valid_id, DM_Record['Title'], this_doujinID, released_date, DM_Record['Price'], DM_Record['WorkType'], DM_Record['Sales'],\
                DM_Record['Favorites'], DM_Record['Reviews'], DM_Record['CmtReviews'], DM_Record['Rating']) + Rating_info

def insert_RJ(sql_paraments):
      cursor.executemany("""
            INSERT DELAYED INTO RJWork (site_accessibility, id, title, doujin_id, released_date, ref_price, language, Sales, Age_Restrict, Other_info, Favorites, Reviews, CmtReviews, Rating,
                        Rate_5,Rate_4,Rate_3,Rate_2,Rate_1) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, sql_paraments
      )

def insert_DM(sql_paraments):
      cursor.executemany("""
            INSERT DELAYED INTO DMWork (site_accessibility, id, title, doujin_id, released_date, ref_price, Worktype, Sales, Favorites, Reviews, CmtReviews, Rating,
                        Rate_5, CmtRate_5, Rate_4, CmtRate_4, Rate_3, CmtRate_3, Rate_2, CmtRate_2, Rate_1, CmtRate_1) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, sql_paraments)

def unlisted_ID_process(record,valid_id,this_title,this_doujinID,False_DM_dict,False_RJ_dict):
    release_dtl = record['release_dtl'][valid_id]
    released_date = release_dtl['date']
    if[released_date]:
            released_date = date2jd(released_date)
    if(valid_id[0] == 'd'):
        False_DM_dict[valid_id] = (False, valid_id, this_title, this_doujinID, released_date, release_dtl['work price'], None, release_dtl['sales']) + (None,) * 14
    elif valid_id not in RJ2doujin_dict:
        False_RJ_dict[valid_id] = (False, valid_id, this_title, this_doujinID, released_date, release_dtl['work price'], '日本語(DEFAULT)', release_dtl['sales']) + (None,) * 11
    else:
        raise ValueError("Duplicate RJ Records created: {0} at {1}".format(v_lst, this_doujinID))

file_path = r"D:\2024Spring\DLsite-Analysis\data_20240518.json"
db_name = 'doujinDB'
#file_path = r"D:\2024Spring\DLsite-Analysis\test.json"
#db_name = 'testDB'
if __name__=='__main__':
    print(f"Loading .json file: {file_path} ...")
    items_numbers = iter_count(file_path)
    print(f"{items_numbers} JSON Records Found!")
    db = pymysql.connect(host='localhost', user='root', password='Pathfinder', charset='utf8')
    cursor = db.cursor()

    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    db.commit()

    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
    result = cursor.fetchone()
    if not result:
            cursor.execute(f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET utf8 COLLATE utf8_bin")
            print(f"DATABASE {db_name} created")
    else:
            print(f"DATABASE {db_name} existes")

    # 选择数据库
    cursor.execute("USE {0}".format(db_name))
    #set_global()
    create_tables()
    pbar_record = tqdm(total=items_numbers, desc=f"Writing into DATABASE: {db_name}", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} | [{elapsed}]',\
                       leave=True, position = 0, mininterval = 1)

    critical_records = []

    Work_main_dict = {}
    reverse_main_dict = {}
    max_workID = 0

    RJ2doujin_dict = {}
    RJWorkofFame = []
    False_RJ_dict = {}
    DM2doujin_dict = {}
    DMWorkofFame = []
    False_DM_dict = {}

    doujin2WorkID_dict = {}
    doujin2Tags = ({},[])
    doujin_tmpdict = {'Circle':({},[]),
                        'Genre':({},[]),
                        'Series':({},[])}
    RJ_tmpdict = {'CV':({},[]),
                'Creator':({},[]),
                'Music':({},[]),
                'Senario':({},[])}
    DM_tmpdict = {'WorkType':({},[])}
    deposited_ids_dict = {}
    deposited_records = {}

    list_bufferSize = 500000
    Series2CircleID_lst = []
    list_dict = {'doujinWork':[], 'time_record':[], 'price':[]}
    list_command = {'doujinWork':"INSERT DELAYED INTO doujinWork (id, main_id, DM_id, RJ_id) VALUES (%s, %s, %s, %s);", 
                    'time_record':"""
                    INSERT DELAYED INTO SalesRecord (time_resolution, time_str, doujin_id, code_id, jd_time, price, amount, delta_amount, delta_revenue) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, 
                    'price':"INSERT DELAYED INTO doujinPrice (jd_time, time_str, doujin_id, code_id, price) VALUES (%s, %s, %s, %s, %s);"
                    }

    with open(file_path, 'r', encoding='UTF-8') as f:
        for record in ijson.items(f, 'item'):

            RJ_lstDict = {'CV':[], 'Creator':[], 'Music':[], 'Senario':[]}
            #DM_lstDict = {'WorkType':[]}
            Tags_dict = {}
            Common_setDict = {'Circle': [],'Genre':[], 'Series':[]}
            
            RJ_para = []
            DM_para = []
            RJWork_sql = []
            Child_dict = {}
            Child_RJexist = False
            Work_main_id = False
            is_JPver = True
            is_new_doujinID = True

            this_doujinID = int(record['ID'])
            c_lst = record['controversial']
            v_lst = []

            if 'title' in record:                           #regular record insert
                this_title = record['title']
                if this_title in reverse_main_dict:                                                         # [Performance Waring: this step takes a lot of computation]
                    Work_main_id = reverse_main_dict[this_title]
                else:
                    if(record["main_genre"]):
                        Common_setDict['Genre'].append(record["main_genre"])
                    if(record["circle"]):
                        Common_setDict['Circle'].append(record["circle"])
                    Tags_dict.update({key:None for key in set(record['main_tags'])})
            else:
                this_title = Work_main_dict[doujin2WorkID_dict[this_doujinID]]
                Work_main_id = reverse_main_dict[this_title]
                is_new_doujinID = False
            
            SiteInfo = {}
            if 'SiteInfo' in record:
                for each in record['SiteInfo']:
                    if each['Status'] == 'verified':
                        v_lst.append(each['Site'])
                    SiteInfo[each['Site']] = each
                if c_lst:
                    if record['verified'] or len(c_lst) > 1:
                        c_lst_dict = {"R": [], "d": []}
                        for key in c_lst_dict.keys():
                            c_lst_dict[key] = [each for each in c_lst if each[0] == key]
                        for lst in c_lst_dict.values():
                            if(lst):
                                if(len(lst)>1):
                                    simularity_lst = [similar_diff_qk_ratio(SiteInfo[each]['Title'], this_title) for each in lst]
                                    max_idx = max(range(len(simularity_lst)), key=simularity_lst.__getitem__)
                                else:
                                    max_idx = 0
                                v_lst.append(lst[max_idx])
                                record['verified'].append(lst[max_idx])
                                del lst[max_idx]
                            if(lst):
                                deposited_ids_dict.update({each : SiteInfo[each] for each in lst})
                    else:
                        v_lst = c_lst
                        record['verified'] = c_lst
                if(has_duplicate_start(record['verified'])):
                    #raise ValueError("Verified IDs belongs to the same site: {0}".format(record['verified']))
                    critical_records.append(record)
                    continue
                Child_dict = {("1" if valid_id[0] == 'd' else "2"): valid_id for valid_id in record['verified']}
                insert_RJs = {valid_id for valid_id in v_lst if valid_id[0] != 'd'}.difference(RJ2doujin_dict)                                          # [Performance Waring: this step takes a lot of computation]
                for valid_id in v_lst:
                    if valid_id in SiteInfo:
                        each = SiteInfo[valid_id]
                        if(each["Tags"]):
                            Tags_dict.update({item.strip():None for item in each["Tags"]})
                        if(each["Genre"]):
                            Common_setDict['Genre'].append(each["Genre"])
                        if(each["Circle"]):
                            Common_setDict['Circle'].append(each["Circle"])
                        if(valid_id[0] == 'd'):
                            if valid_id in RJ2doujin_dict:
                                raise ValueError("Duplicate Entry of DMWork: {0} in doujinID: {1}".format(valid_id, this_doujinID))
                            if valid_id in False_DM_dict:
                                del False_DM_dict[valid_id]
                            DM_para.append(DMSiteInfo_extract(each, valid_id, this_doujinID, Tags_dict, Common_setDict, DMWorkofFame))
                        else:
                            if(valid_id in insert_RJs):
                                RJWork_sql.append(valid_id)
                                if valid_id in False_RJ_dict:
                                    del False_RJ_dict[valid_id]
                                RJ_para.append(RJSiteInfo_extract(each, valid_id, this_doujinID, Tags_dict, Common_setDict, RJ_lstDict, RJWorkofFame))
                            elif(valid_id == Child_dict["2"]):
                                cursor.execute("UPDATE RJWork SET doujin_id = %s WHERE id = %s",(this_doujinID, valid_id))
                                target_doujinID = RJ2doujin_dict[valid_id]
                                if not Work_main_id:
                                    Work_main_id = doujin2WorkID_dict[target_doujinID]
                                else:
                                    doujin2WorkID_dict[target_doujinID] = Work_main_id
                                is_JPver = each['language'] == '日本語(DEFAULT)' or each['language'] == '日本語'
                    else:
                        unlisted_ID_process(record,valid_id,this_title,this_doujinID,False_DM_dict,False_RJ_dict)
            elif is_new_doujinID:                       # using information from dojinDB as compensation
                c_lst.extend(record['verified'])
                v_lst = set(c_lst)
                for valid_id in v_lst:
                    unlisted_ID_process(record,valid_id,this_title,this_doujinID,False_DM_dict,False_RJ_dict)
                Child_dict = {("1" if valid_id[0] == 'd' else "2"): valid_id for valid_id in v_lst}
            else:
                Work_main_id = doujin2WorkID_dict[this_doujinID]        ### duplicate doujinID process, no 'price_data' and 'historyData' in dictionary
            
            if(Work_main_id):
                if(is_JPver):
                    previous_title = Work_main_dict[Work_main_id]
                    reverse_main_dict[this_title] = reverse_main_dict.pop(previous_title)
                    Work_main_dict[Work_main_id] = this_title
            else:
                max_workID += 1
                reverse_main_dict[this_title] = max_workID
                Work_main_dict[max_workID] = this_title
                Work_main_id = max_workID

            list_dict['doujinWork'].append((this_doujinID, Work_main_id, Child_dict["1"] if "1" in Child_dict else None,\
                                            Child_dict["2"] if "2" in Child_dict else None))
            doujin2WorkID_dict[this_doujinID] = Work_main_id
            if(len(DM_para)> 1):
                raise ValueError("Multiple DMWork Record in doujinID:{0} -- {1}".format(this_doujinID, DM_para))
            if(record['price_data']):
                insert_price_history(record['price_data'], this_doujinID, Child_dict, list_dict['price'])
            if(record['historyData']):
                expelled_records = insert_sales_history(Child_dict, record['historyData'], this_doujinID, list_dict["time_record"])
                if expelled_records:
                    deposited_records.update(expelled_records)
            if(DM_para):
                DM2doujin_dict[Child_dict['1']] = this_doujinID
                insert_DM(DM_para)
            if(RJ_para):
                insert_RJ(RJ_para)
                RJ2doujin_dict.update({each:this_doujinID for each in RJWork_sql})
            for key, value in doujin_tmpdict.items():
                if key in Common_setDict:
                    insert_dictID(value[0], value[1], set(Common_setDict[key]), this_doujinID)             # [Performance Waring: this step takes a lot of computation]
            insert_Tags(doujin2Tags[0], doujin2Tags[1], Tags_dict, this_doujinID)
            for key, value in RJ_tmpdict.items():
                insert_dictID(value[0], value[1], set(RJ_lstDict[key]), this_doujinID)                       # [Performance Waring: this step takes a lot of computation]
            #for key, value in DM_tmpdict.items():
            #    insert_dictID(value[0], value[1], set(DM_lstDict[key]), this_doujinID)                       # [Performance Waring: this step takes a lot of computation]

            if(Common_setDict['Circle'] and Common_setDict['Series']):
                for circle_name in Common_setDict['Circle']:
                    Series2CircleID_lst.extend([(doujin_tmpdict['Circle'][0][circle_name],doujin_tmpdict['Series'][0][each]) for each in Common_setDict['Series']])
            for key, value in list_dict.items():
                if(len(value) > list_bufferSize):
                    cursor.executemany(list_command[key],value)
                    db.commit()
                    list_dict[key].clear()
            pbar_record.update(1)
    pbar_record.close()

    print("Leftover into DATABASE...")
    New_doujinID = 1000000
    inhalted_key = set()
    for code_key, value in deposited_ids_dict.items():
        RJ_lstDict = {'CV':[], 'Creator':[], 'Music':[], 'Senario':[]}
        #DM_lstDict = {'WorkType':[]}
        Tags_dict = {}
        Common_setDict = {'Circle': [],'Genre':[], 'Series':[]}

        RJ_para = []
        DM_para = []
        this_doujinID = None
        title = value["Title"]

        if title in reverse_main_dict:
            Work_main_id = reverse_main_dict[title]
            if(value["Tags"]):
                Tags_dict.update({item.strip():None for item in value["Tags"]})
            if(value["Genre"]):
                Common_setDict['Genre'].append(value["Genre"])
            if(value["Circle"]):
                Common_setDict['Circle'].append(value["Circle"])
            if code_key[0] == 'd' and code_key not in DM2doujin_dict:
                if code_key in False_DM_dict:
                    this_doujinID = False_DM_dict[code_key][3]
                    del False_DM_dict[code_key]
                    cursor.execute("UPDATE doujinWork SET DM_id = %s WHERE id = %s",(code_key, this_doujinID))
                if not this_doujinID:
                    New_doujinID += 1
                    this_doujinID = New_doujinID
                    doujin2WorkID_dict[New_doujinID] = Work_main_id
                    list_dict['doujinWork'].append((this_doujinID, Work_main_id, code_key, None))
                DM_para.append(DMSiteInfo_extract(value, code_key, this_doujinID, Tags_dict, Common_setDict, DMWorkofFame))
            elif code_key not in RJ2doujin_dict:
                if code_key in False_RJ_dict:
                    this_doujinID = False_RJ_dict[code_key][3]
                    del False_RJ_dict[code_key]
                    cursor.execute("UPDATE doujinWork SET RJ_id = %s WHERE id = %s",(code_key, this_doujinID))
                if not this_doujinID:
                    New_doujinID += 1
                    this_doujinID = New_doujinID
                    doujin2WorkID_dict[New_doujinID] = Work_main_id
                    list_dict['doujinWork'].append((this_doujinID, Work_main_id, None, code_key))
                RJ_para.append(RJSiteInfo_extract(value, code_key, this_doujinID, Tags_dict, Common_setDict, RJ_lstDict, RJWorkofFame))
            else:
                continue
            if(DM_para):
                DM2doujin_dict[code_key] = this_doujinID
                insert_DM(DM_para)
            if(RJ_para):
                insert_RJ(RJ_para)
                RJ2doujin_dict[code_key] = this_doujinID
            for key, value in doujin_tmpdict.items():
                if key in Common_setDict:
                    insert_dictID(value[0], value[1], set(Common_setDict[key]), this_doujinID)             # [Performance Waring: this step takes a lot of computation]
            insert_Tags(doujin2Tags[0], doujin2Tags[1], Tags_dict, this_doujinID)
            for key, value in RJ_tmpdict.items():
                insert_dictID(value[0], value[1], set(RJ_lstDict[key]), this_doujinID)                       # [Performance Waring: this step takes a lot of computation]
            #for key, value in DM_tmpdict.items():
            #    insert_dictID(value[0], value[1], set(DM_lstDict[key]), this_doujinID)                       # [Performance Waring: this step takes a lot of computation]

            if(Common_setDict['Circle'] and Common_setDict['Series']):
                for circle_name in Common_setDict['Circle']:
                    Series2CircleID_lst.extend([(doujin_tmpdict['Circle'][0][circle_name],doujin_tmpdict['Series'][0][each]) for each in Common_setDict['Series']])
            inhalted_key.add(code_key)
    for key in inhalted_key:
        del deposited_ids_dict[key]

    if False_RJ_dict:
        insert_RJ(False_RJ_dict.values())
        RJ2doujin_dict.update({code: value[3] for code, value in False_RJ_dict.items()})
    if False_DM_dict:
        insert_DM(False_DM_dict.values())
        DM2doujin_dict.update({code: value[3] for code, value in False_DM_dict.items()})

    inhalted_key = set()
    for key, records in deposited_records.items():
        if key in DM2doujin_dict or key in RJ2doujin_dict and records:
            this_records = np.array(records)
            arr = np.array(this_records[:,4:], dtype=int)
            result, idx_array = process_array(arr)
            list_dict['time_record'].extend(np.column_stack((this_records[idx_array], result)).tolist())
            inhalted_key.add(key)
    for key in inhalted_key:
        del deposited_records[key]

    for key, value in list_dict.items():
        if(value):
            cursor.executemany(list_command[key],value)
            db.commit()
            list_dict[key].clear()
    print("Memory Data into DATABASE...")
    print("Writing Work_main...")
    cursor.executemany("INSERT DELAYED INTO Work_main (id, title) VALUES (%s, %s);", [(key, value) for key, value in Work_main_dict.items()])
    db.commit()
    insert_tmpdict(RJ_tmpdict,'doujinWork')
    insert_tmpdict(DM_tmpdict,'doujinWork')
    insert_tmpdict(doujin_tmpdict,'doujinWork')
    print("Writing Entities related to doujinWork... --item: '{0} items to {1}'".format('Tag', 'doujinWork'))

    print("Updating Work IDs...")
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id, doujin_id, work_id);".format('RJWork'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id, doujin_id, work_id);".format('DMWork'))
    RJ_update_lst = [(doujin2WorkID_dict[doujinID], code_id) for code_id, doujinID in RJ2doujin_dict.items()]
    cursor.executemany('UPDATE RJWork SET work_id = (%s) WHERE id = (%s)', RJ_update_lst)
    DM_update_lst = [(doujin2WorkID_dict[doujinID], code_id) for code_id, doujinID in DM2doujin_dict.items()]
    cursor.executemany('UPDATE DMWork SET work_id = (%s) WHERE id = (%s)', DM_update_lst)

    cursor.executemany("INSERT DELAYED INTO Tag (id, name) VALUES (%s, %s);", [(value, key[:128]) for key, value in doujin2Tags[0].items()])
    cursor.executemany("INSERT DELAYED INTO doujinWork2Tag (doujinWork_id, genre_count,Tag_id) VALUES (%s,%s,%s);", set(doujin2Tags[1]))
    cursor.executemany("INSERT DELAYED INTO RJWorkofFame (RJ_id, `category`, `type`, `rank`, `time_str`) VALUES (%s, %s, %s, %s, %s);", RJWorkofFame)
    cursor.executemany("INSERT DELAYED INTO DMWorkofFame (DM_id, `type`, `rank`) VALUES (%s, %s, %s);", DMWorkofFame)
    db.commit()

    print("Altering Foreign Keys...")
    cursor.execute('ALTER TABLE doujinWork ADD CONSTRAINT doujin2Workmain FOREIGN KEY (main_id) REFERENCES Work_main(id);')
    cursor.execute('ALTER TABLE RJWork ADD CONSTRAINT RJ2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE RJWork ADD CONSTRAINT RJ2Workmain FOREIGN KEY (work_id) REFERENCES Work_main(id);')
    cursor.execute('ALTER TABLE DMWork ADD CONSTRAINT DM2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE DMWork ADD CONSTRAINT DM2Workmain FOREIGN KEY (work_id) REFERENCES Work_main(id);')
    cursor.execute('ALTER TABLE SalesRecord ADD CONSTRAINT daily2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE doujinPrice ADD CONSTRAINT Price2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    db.commit()

    print("Writing Relation Charts... --item: {0}".format('Circle to Series'))
    cursor.executemany("INSERT DELAYED INTO Circle2Series (Circle_id, Series_id) VALUES (%s, %s);", set(Series2CircleID_lst))
    db.commit()
    create_indexs()

    cursor.execute('SHOW TABLES;')
    tables = cursor.fetchall()
    for table_name in tables:
        print(f"Optimising: {table_name[0]} ...")
        cursor.execute(f'OPTIMIZE TABLE {table_name[0]};')

    # 关闭连接
    cursor.close()
    db.close()
    print("ALL Records in DATABASE!")
    filepaths = os.path.dirname(file_path)
    if deposited_ids_dict:
        json_writein(os.path.join(filepaths,'deposited_ids.json'),deposited_ids_dict)
    if deposited_records:
        json_writein(os.path.join(filepaths,'deposited_records.json'),deposited_records)
    if critical_records:
        json_writein(os.path.join(filepaths,'critical_records.json'),critical_records)
    print("ALL Missing ids and Records saved!")