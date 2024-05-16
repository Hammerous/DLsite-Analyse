import pymysql,ijson,datetime,hashlib,binascii,re
from tqdm.autonotebook import tqdm

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

def md5(str):
    m = hashlib.md5()
    m.update(str.encode("utf8"))
    #print(m.hexdigest())
    #return m.hexdigest()
    return binascii.unhexlify(m.hexdigest())

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
        id INT AUTO_INCREMENT PRIMARY KEY,
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
    simple_element('doujinWork','Tag', 128)
    simple_element('doujinWork','Genre', 64)
    simple_element('doujinWork','Series', 64)
    simple_element('doujinWork','Circle', 64)
    simple_element('doujinWork','CV', 64)
    simple_element('doujinWork','Creator', 64)
    simple_element('doujinWork','Music', 64)
    simple_element('doujinWork','Senario', 64)
    simple_element('doujinWork','WorkType', 64)
    create_relationChart('Circle', 'Series', 'INT', 'INT')
    #FOREIGN KEY (main_id) REFERENCES Work_main(id)
    create_relationChart('Work_main','doujinWork', 'INT', 'INT')
    cursor.execute("""
    CREATE TABLE RJWork (
                site_accessibility BOOLEAN NOT NULL,
                id VARCHAR(20) NOT NULL PRIMARY KEY,
                title VARCHAR(255) BINARY NOT NULL,
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
    create_relationChart('doujinWork','RJWork', 'INT', 'VARCHAR(20)')
    cursor.execute("""
    CREATE TABLE DMWork (
                site_accessibility BOOLEAN NOT NULL,
                id VARCHAR(20) NOT NULL PRIMARY KEY,
                title VARCHAR(255) BINARY NOT NULL,
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
    create_relationChart('doujinWork','DMWork', 'INT', 'VARCHAR(20)')
    ### The unit of JPY is an integer
    cursor.execute("""
    CREATE TABLE SalesRecord_daily (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                jd_time INT UNSIGNED NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                doujin_id INT NOT NULL,
                code_id VARCHAR(20) NOT NULL,
                price INT DEFAULT NULL,
                amount Float(10,2) DEFAULT NULL,
                delta_amount Float(10,2) DEFAULT NULL,
                delta_revenue Float(10,2) DEFAULT NULL
    )
    """)
    cursor.execute("""
    CREATE TABLE SalesRecord_monthly (
                id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                jd_time INT UNSIGNED NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                doujin_id INT NOT NULL,
                code_id VARCHAR(20) NOT NULL,
                price INT UNSIGNED DEFAULT NULL,
                amount Float(10,2) DEFAULT NULL,
                delta_amount Float(10,2) DEFAULT NULL,
                delta_revenue Float(10,2) DEFAULT NULL
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
    db.commit()

def create_indexs():
    print('Creating index of Sales Record sequences ...')
    cursor.execute(f"ALTER TABLE SalesRecord_daily ADD INDEX daily_sales_idx (doujin_id, code_id, jd_time);")
    cursor.execute(f"ALTER TABLE SalesRecord_monthly ADD INDEX monthly_sales_idx (doujin_id, code_id, jd_time);")
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
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('doujinWork','WorkType'))

    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Circle','Series'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Work_main','doujinWork'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('doujinWork','RJWork'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('doujinWork','DMWork'))
    cursor.execute("ALTER TABLE doujinPrice ADD INDEX work2pricetime (doujin_id, jd_time);")

    print('Creating index of Entities ...')
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('Work_main'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('doujinWork'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('RJWork'))
    cursor.execute("ALTER TABLE {0} ADD INDEX index_{0} (id);".format('DMWork'))
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
     
def iter_count(json_path):
    from itertools import (takewhile, repeat)
    buffer = 1024 * 1024 * 4
    with open(json_path, 'r', encoding='UTF-8') as f:
        buf_gen = takewhile(lambda x: x, (f.read(buffer) for _ in repeat(None)))
        return sum(buf.count('\t') for buf in buf_gen)

def insert_RJ(sql_paraments):
      cursor.executemany("""
            INSERT DELAYED INTO RJWork (site_accessibility, id, title, doujin_id, released_date, ref_price, language, Sales, Age_Restrict, Other_info, Favorites, Reviews, CmtReviews, Rating,
                        Rate_5,Rate_4,Rate_3,Rate_2,Rate_1) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, sql_paraments
      )

def insert_DM(sql_paraments):
      cursor.executemany("""
            INSERT DELAYED INTO DMWork (site_accessibility, id, title, doujin_id, released_date, ref_price, Sales, Favorites, Reviews, CmtReviews, Rating,
                        Rate_5, CmtRate_5, Rate_4, CmtRate_4, Rate_3, CmtRate_3, Rate_2, CmtRate_2, Rate_1, CmtRate_1) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, sql_paraments)

def insert_tmpdict(tmpdict, parent_entity):
    for key, value in tmpdict.items():
        print("Writing Entities related to doujinWork... --item: '{0} items to {1}'".format(key, parent_entity))
        cursor.executemany("INSERT DELAYED INTO {0} (id, name) VALUES (%s, %s);".format(key), [(value, key[:64]) for key, value in value[0].items()])
        cursor.executemany("INSERT DELAYED INTO {1}2{0} ({1}_id, {0}_id) VALUES (%s, %s);".format(key, parent_entity), set(value[1]))
        db.commit()

daily_type = {'beforelast','lastmonth','thismonth','30days'}

def if_integer(string):
    reg_exp = "[-+]?\d+$"
    return re.match(reg_exp, string) is not None

def record_extract(ref_code, info, dict, doujin_id):
    # detail format: "price","amount","time_ymd","jd_date"
    code_dtl = list(info.keys())
    if(code_dtl):
        if(len(code_dtl) == 1):
            code_dtl = code_dtl[0]
        else:
            return False
    else:
        code_dtl = ref_code
    for each in info[code_dtl]:
        date = int(each[3])
        if date not in dict or if_integer(each[1]):
            dict[date] = (each[2],doujin_id,code_dtl,int(float(each[0])),int(float(each[1])))
        ### data from dojinDB sometimes has amount data in float type, this constrains it to conserve only integer data
        ### because float data is commonly incorrect for the pricing data, especially when the unit of JPY is an integer
    return True

def insert_sales_history(ref_code_dict, historyData, doujin_id, daily_lst, monthly_lst):
    daily_dict = {}
    monthly_dict = {}
    for type, record in historyData.items():
        if(record):
            target_dict = daily_dict if type in daily_type else monthly_dict
            for sitetype, data in record.items():
                ref_code = ref_code_dict[sitetype]
                status = record_extract(ref_code, data, target_dict, doujin_id)
                if not status:
                    print("Record Codes in {0} illigal: {1}".format(doujin_id, data))
                    raise ValueError
    daily_lst.extend([value + (key,) for key, value in daily_dict.items()])
    monthly_lst.extend([value + (key,) for key, value in monthly_dict.items()])
    
def insert_price_history(priceData, doujin_id, Child_dict, price_lst):
    # jd_time, time_str, doujin_id, code_id, price
    priceinfo_lst = [(date2jd(time), time, doujin_id, Child_dict['1' if fanza != 'None' else '2'], fanza if fanza != 'None' else dlsite)
                     for time, dlsite, fanza in zip(priceData['time'], priceData['dlsite'], priceData['fanza'])
                     if fanza != 'None' or dlsite != 'None']
    price_lst.extend(priceinfo_lst)

#file_path = r"D:\2024Spring\DLsite-Analysis\data_20240511.json"
file_path = r"D:\2024Spring\DLsite-Analysis\test.json"
if __name__=='__main__':
    print(f"Loading .json file: {file_path} ...")
    items_numbers = iter_count(file_path)
    print(f"{items_numbers} JSON Records Found!")
    #db = pymysql.connect(host='localhost', user='root', password='Pathfinder', charset='utf8')
    db = pymysql.connect(host='106.15.203.185',user='oldyear',password='gui99lover', charset='utf8')
    cursor = db.cursor()
    db_name = 'testDB'
    #db_name = 'doujinDB'

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
    create_tables()
    pbar_record = tqdm(total=items_numbers, desc=f"Writing into DATABASE: {db_name}", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} | [{elapsed}]',\
                       leave=True, position = 0, mininterval = 1)

    Work_main_dict = {}
    reverse_main_dict = {}
    max_workID = 0
    RJ2doujin_dict = {}
    False_RJ_dict = {}
    DM2doujin_dict = {}
    False_DM_dict = {}
    doujin2WorkID_dict = {}
    doujin_tmpdict = {'Tag':({},[]),
                        'Circle':({},[]),
                        'Genre':({},[]),
                        'Series':({},[])}
    RJ_tmpdict = {'CV':({},[]),
                'Creator':({},[]),
                'Music':({},[]),
                'Senario':({},[])}
    DM_tmpdict = {'WorkType':({},[])}
    deposited_ids_dict = {}
    deposited_record = {}

    list_bufferSize = 100000
    Series2CircleID_lst = []
    list_dict = {'doujinWork':[], 'daily_record':[], 'monthly_record':[], 'price':[]}
    list_command = {'doujinWork':"INSERT DELAYED INTO doujinWork (id, main_id, DM_id, RJ_id) VALUES (%s, %s, %s, %s);", 
                    'daily_record':"""
                       INSERT DELAYED INTO SalesRecord_daily (time_str, doujin_id, code_id, price, amount, jd_time) 
                       VALUES (%s, %s, %s, %s, %s, %s);
                       """, 
                    'monthly_record':"""
                       INSERT DELAYED  INTO SalesRecord_monthly (time_str, doujin_id, code_id, price, amount, jd_time) 
                       VALUES (%s, %s, %s, %s, %s, %s);
                       """, 
                    'price':"INSERT DELAYED INTO doujinPrice (jd_time, time_str, doujin_id, code_id, price) VALUES (%s, %s, %s, %s, %s);"
                    }

    with open(file_path, 'r', encoding='UTF-8') as f:
        for record in ijson.items(f, 'item'):

            RJ_lstDict = {'CV':[], 'Creator':[], 'Music':[], 'Senario':[]}
            DM_lstDict = {'WorkType':[]}
            Common_setDict = {'Tag': set(), 'Circle': [],'Genre':[], 'Series':[]}
            
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
                    Common_setDict['Tag'] = set(record['main_tags'])
            else:
                this_title = Work_main_dict[doujin2WorkID_dict[this_doujinID]]
                Work_main_id = reverse_main_dict[this_title]
                is_new_doujinID = False

            SiteInfo = {}
            if 'SiteInfo' in record:
                for each in record['SiteInfo']:
                    if each['Status'] == 'verified':
                        v_lst.append(each['Site'])
                    SiteInfo[each.pop('Site')] = each
                
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
                    print("Verified IDs belongs to the same site: {0}".format(record['verified']))
                    raise ValueError
                Child_dict = {("1" if valid_id[0] == 'd' else "2"): valid_id for valid_id in record['verified']}
                RJ_set = {valid_id for valid_id in v_lst if valid_id[0] != 'd'}
                insert_RJs = RJ_set.difference(RJ2doujin_dict)                                          # [Performance Waring: this step takes a lot of computation]
                for valid_id in v_lst:
                    each = SiteInfo[valid_id]
                    if(each["Tags"]):
                        Common_setDict['Tag'] = {item.strip() for item in each["Tags"]}.union(Common_setDict['Tag'])
                    if(each["Genre"]):
                        Common_setDict['Genre'].append(each["Genre"])
                    if(each["Circle"]):
                        Common_setDict['Circle'].append(each["Circle"])
                    if(valid_id[0] == 'd'):
                        if valid_id in RJ2doujin_dict:
                            print("Duplicate Entry of DMWork: {0} in doujinID: {1}".format(valid_id, this_doujinID))
                            raise ValueError
                        if valid_id in False_DM_dict:
                            del False_DM_dict[valid_id]
                        if(bool(re.match(r'^d_.*zero$', valid_id))):
                            Common_setDict['Tag'].add('FANZA無料') 
                        if(each["Sub_Genre"]):
                            Common_setDict['Genre'].append(each["Sub_Genre"].strip())
                        if(each["Series"] != '----'):
                            Common_setDict['Series'].append(each["Series"].strip())
                        for key in DM_lstDict.keys():
                            DM_lstDict[key].append(each[key])
                        if(each['Rating_info']):
                            Rating_info = (each['Rating_info']['5'][0],each['Rating_info']['5'][1],each['Rating_info']['4'][0],each['Rating_info']['4'][1],\
                                            each['Rating_info']['3'][0],each['Rating_info']['3'][1],each['Rating_info']['2'][0],each['Rating_info']['2'][1],\
                                            each['Rating_info']['1'][0],each['Rating_info']['1'][1])
                        else:
                            Rating_info = (None,) * 10
                        released_date = None
                        if[each['Released_date']]:
                            released_date = date2jd(each['Released_date'].split()[0].replace("/","-"))
                        DM_para.append((True, valid_id, each['Title'], this_doujinID, released_date, each['Price'], each['Sales'],\
                                    each['Favorites'], each['Reviews'], each['CmtReviews'], each['Rating']) + Rating_info)
                    else:
                        if(valid_id in insert_RJs):
                            if valid_id in False_RJ_dict:
                                del False_RJ_dict[valid_id]
                            if(each["Series"]):
                                Common_setDict['Series'].extend([s.strip() for s in each["Series"]])
                            for key in RJ_lstDict.keys():
                                if(each[key]):
                                    RJ_lstDict[key].extend(each[key])
                            if(len(each['Rating_info'])):
                                Rating_info = (each['Rating_info']['5'],each['Rating_info']['4'],each['Rating_info']['3'],each['Rating_info']['2'],each['Rating_info']['1'])
                            else:
                                Rating_info = (None,) * 5
                            RJWork_sql.append(valid_id)
                            released_date = None
                            if[each['Released_date']]:
                                released_date = date2jd(each['Released_date'][0].split()[0].replace("年","-").replace("月","-").replace("日",""))
                            RJ_para.append((True, valid_id, each['Title'], this_doujinID, released_date, each['Price'],\
                                            each['language'], each['Sales'] if each['Sales'] else None, each['Age_Restrict'],\
                                            each['Other_info'], each['Favorites'], each['Reviews'], each['CmtReviews'], each['Rating']) + Rating_info)
                        elif(valid_id == Child_dict["2"]):
                            cursor.execute("UPDATE LOW_PRIORITY RJWork SET doujin_id = %s WHERE id = %s",(this_doujinID, valid_id))
                            target_doujinID = RJ2doujin_dict[valid_id]
                            if not Work_main_id:
                                Work_main_id = doujin2WorkID_dict[target_doujinID]
                            else:
                                doujin2WorkID_dict[target_doujinID] = Work_main_id
                            is_JPver = each['language'] == '日本語(DEFAULT)' or each['language'] == '日本語'
            elif is_new_doujinID:                       # using information from dojinDB as compensation
                Child_dict = {("1" if valid_id[0] == 'd' else "2"): valid_id for valid_id in record['verified']}
                v_lst = record['verified']
                v_lst.extend(c_lst)
                for valid_id in v_lst:
                    release_dtl = record['release_dtl'][valid_id]
                    released_date = release_dtl['date']
                    if[released_date]:
                            released_date = date2jd(released_date)
                    if(valid_id[0] == 'd'):
                        False_DM_dict[valid_id] = (False, valid_id, this_title, this_doujinID, released_date, release_dtl['work price'], release_dtl['sales']) + (None,) * 14
                    elif valid_id not in RJ2doujin_dict:
                        False_RJ_dict[valid_id] = (False, valid_id, this_title, this_doujinID, released_date, release_dtl['work price'], '日本語(DEFAULT)', release_dtl['sales']) + (None,) * 11
                    else:
                         print("Duplicate RJ Records created: {0} at {1}".format(v_lst, this_doujinID))
                         raise ValueError
            else:
                Work_main_id = doujin2WorkID_dict[this_doujinID]
            
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
                print("Multiple DMWork Record in doujinID:{0} -- {1}".format(this_doujinID, DM_para))
                raise ValueError
            if(DM_para):
                 DM2doujin_dict[Child_dict['1']] = this_doujinID
                 insert_DM(DM_para)
            if(record['price_data']):
                insert_price_history(record['price_data'], this_doujinID, Child_dict, list_dict['price'])
            if(record['historyData']):
                insert_sales_history(Child_dict, record['historyData'], this_doujinID, list_dict["daily_record"], list_dict["monthly_record"])
            if(RJ_para):
                insert_RJ(RJ_para)
                for each in RJWork_sql:
                    RJ2doujin_dict[each] = this_doujinID
            for key, value in doujin_tmpdict.items():
                if key in Common_setDict:
                    insert_dictID(value[0], value[1], set(Common_setDict[key]), this_doujinID)             # [Performance Waring: this step takes a lot of computation]
            #insert_dictID(CircleID_dict, Circle2WorkID_lst, Circle_set, Work_main_id)
            for key, value in RJ_tmpdict.items():
                insert_dictID(value[0], value[1], set(RJ_lstDict[key]), this_doujinID)                       # [Performance Waring: this step takes a lot of computation]
            for key, value in DM_tmpdict.items():
                insert_dictID(value[0], value[1], set(DM_lstDict[key]), this_doujinID)                       # [Performance Waring: this step takes a lot of computation]

            if(Common_setDict['Circle'] and Common_setDict['Series']):
                for circle_name in Common_setDict['Circle']:
                    Series2CircleID_lst.extend([(doujin_tmpdict['Circle'][0][circle_name],doujin_tmpdict['Series'][0][each]) for each in Common_setDict['Series']])
            for key, value in list_dict.items():
                if(len(value) > list_bufferSize):
                    cursor.executemany(list_command[key],value)
                    db.commit()
                    list_dict[key] = []
            pbar_record.update(1)
    pbar_record.close()

    print("Leftover Data writing into DATABASE...")
    for key, value in list_dict.items():
        if(value):
            cursor.executemany(list_command[key],value)
            list_dict[key] = []
    if False_RJ_dict:
        insert_RJ(False_RJ_dict.values())
    if False_DM_dict:
        insert_DM(False_DM_dict.values())
    
    print("Data in Memory writing into DATABASE...")
    print("Writing Work_main...")
    cursor.executemany("INSERT DELAYED INTO Work_main (id, title) VALUES (%s, %s);", [(key, value) for key, value in Work_main_dict.items()])
    db.commit()
    insert_tmpdict(RJ_tmpdict,'doujinWork')
    insert_tmpdict(DM_tmpdict,'doujinWork')
    insert_tmpdict(doujin_tmpdict,'doujinWork')

    print("Altering Foreign Keys...")
    cursor.execute('ALTER TABLE doujinWork ADD CONSTRAINT doujin2Workmain FOREIGN KEY (main_id) REFERENCES Work_main(id);')
    cursor.execute('ALTER TABLE RJWork ADD CONSTRAINT RJ2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE DMWork ADD CONSTRAINT DM2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE SalesRecord_daily ADD CONSTRAINT daily2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE SalesRecord_monthly ADD CONSTRAINT monthly2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    cursor.execute('ALTER TABLE doujinPrice ADD CONSTRAINT Price2doujin FOREIGN KEY (doujin_id) REFERENCES doujinWork(id);')
    db.commit()

    print("Writing Relation Charts... --item: {0}".format('doujinWork to Work_main'))
    cursor.executemany("INSERT DELAYED INTO Work_main2doujinWork (Work_main_id, doujinWork_id) VALUES (%s, %s);", [(value, key) for key, value in doujin2WorkID_dict.items()])
    print("Writing Relation Charts... --item: {0}".format('DMWork to doujinWork'))
    cursor.executemany("INSERT DELAYED INTO doujinWork2DMWork (doujinWork_id, DMWork_id) VALUES (%s, %s);", [(value, key) for key, value in DM2doujin_dict.items()])
    print("Writing Relation Charts... --item: {0}".format('RJWork to doujinWork'))
    cursor.executemany("INSERT DELAYED INTO doujinWork2RJWork (doujinWork_id, RJWork_id) VALUES (%s, %s);", [(value, key) for key, value in RJ2doujin_dict.items()])
    print("Writing Relation Charts... --item: {0}".format('Circle to Series'))
    cursor.executemany("INSERT DELAYED INTO Circle2Series (Circle_id, Series_id) VALUES (%s, %s);", set(Series2CircleID_lst))
    db.commit()
    create_indexs()
    # 关闭连接
    cursor.close()
    db.close()
    print("ALL Records in DATABASE!")
    print(deposited_ids_dict)