import pymysql,ijson,datetime
from tqdm.autonotebook import tqdm

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
    simple_element('Work_main','Tag', 128)
    simple_element('Work_main','Genre', 64)
    simple_element('Work_main','Series', 64)
    simple_element('Work_main','Circle', 64)
    simple_element('Work_main','CV', 64)
    simple_element('Work_main','Creator', 64)
    simple_element('Work_main','Music', 64)
    simple_element('Work_main','Senario', 64)
    simple_element('Work_main','WorkType', 64)
    create_relationChart('Circle', 'Series', 'INT', 'INT')
    cursor.execute("""
    CREATE TABLE doujinWork (
                id INT PRIMARY KEY,
                main_id INT,
                `Rank` INT NULL,
                UNIQUE (id)
    )
    """)
    #FOREIGN KEY (main_id) REFERENCES Work_main(id)
    create_relationChart('Work_main','doujinWork', 'INT', 'INT')
    cursor.execute("""
    CREATE TABLE RJWork (
                id VARCHAR(20) NOT NULL PRIMARY KEY,
                doujin_id INT DEFAULT NULL,
                released_date INT DEFAULT NULL,
                ref_price Float(10,2) DEFAULT NULL,
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
                FOREIGN KEY (doujin_id) REFERENCES doujinWork(id),
                UNIQUE (id)
    )
    """)
    create_relationChart('Work_main','RJWork', 'INT', 'VARCHAR(20)')
    cursor.execute("""
    CREATE TABLE DMWork (
                id VARCHAR(20) NOT NULL PRIMARY KEY,
                doujin_id INT NOT NULL,
                released_date INT DEFAULT NULL,
                ref_price Float(10,2) DEFAULT NULL,
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
                FOREIGN KEY (doujin_id) REFERENCES doujinWork(id),
                UNIQUE (id)
    )
    """)
    create_relationChart('Work_main','DMWork', 'INT', 'VARCHAR(20)')
    cursor.execute("""
    CREATE TABLE SalesRecord_daily (
                id INT AUTO_INCREMENT PRIMARY KEY,
                jd_time INT NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                doujin_id INT NOT NULL,
                code_id VARCHAR(20) NOT NULL,
                price Float(10,2) DEFAULT NULL,
                amount INT DEFAULT NULL,
                delta_amount INT DEFAULT NULL,
                delta_revenue Float(10,2) DEFAULT NULL,
                FOREIGN KEY (doujin_id) REFERENCES doujinWork(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE SalesRecord_monthly (
                id INT AUTO_INCREMENT PRIMARY KEY,
                jd_time INT NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                doujin_id INT NOT NULL,
                code_id VARCHAR(20) NOT NULL,
                price Float(10,2) DEFAULT NULL,
                amount INT DEFAULT NULL,
                delta_amount INT DEFAULT NULL,
                delta_revenue Float(18,2) DEFAULT NULL,
                FOREIGN KEY (doujin_id) REFERENCES doujinWork(id)
    )
    """)
    cursor.execute("""
    CREATE TABLE doujinPrice (
                id INT AUTO_INCREMENT PRIMARY KEY,
                doujin_id INT NOT NULL,
                time_str VARCHAR(12) NOT NULL,
                jd_time INT NOT NULL,
                price_DLsite Float(10,2) DEFAULT NULL,
                price_FANZA Float(10,2) DEFAULT NULL,
                FOREIGN KEY (doujin_id) REFERENCES doujinWork(id)
    )
    """)
    db.commit()

def create_indexs():
    print('Creating index of Sales Record sequences ...')
    cursor.execute(f"ALTER TABLE SalesRecord_daily ADD INDEX sales_idx (doujin_id, code_id, jd_time);")
    cursor.execute(f"ALTER TABLE SalesRecord_monthly ADD INDEX sales_idx (doujin_id, code_id, jd_time);")
    
    print('Creating index of relation Charts ...')
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Tag'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Genre'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Series'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Circle'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','CV'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Creator'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Music'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','Senario'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({1}_id, {0}_id);".format('Work_main','WorkType'))

    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Circle','Series'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Work_main','doujinWork'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Work_main','RJWork'))
    cursor.execute("ALTER TABLE {0}2{1} ADD INDEX {1}_to_{0} ({0}_id, {1}_id);".format('Work_main','DMWork'))
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
            INSERT DELAYED INTO RJWork (id, doujin_id, released_date, ref_price, language, Sales, Age_Restrict, Other_info, Favorites, Reviews, CmtReviews, Rating,
                        Rate_5,Rate_4,Rate_3,Rate_2,Rate_1) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, sql_paraments
      )

def insert_DM(sql_paraments):
      cursor.execute("""
            INSERT DELAYED INTO DMWork (id, doujin_id, released_date, ref_price, Sales, Favorites, Reviews, CmtReviews, Rating,
                        Rate_5, CmtRate_5, Rate_4, CmtRate_4, Rate_3, CmtRate_3, Rate_2, CmtRate_2, Rate_1, CmtRate_1) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, sql_paraments)

def insert_tmpdict(tmpdict, parent_entity):
    for key, value in tmpdict.items():
        print("Writing Entities related to Work_main... --item: '{0} items to {1}'".format(key, parent_entity))
        cursor.executemany("INSERT DELAYED INTO {0} (id, name) VALUES (%s, %s);".format(key), [(value, key[:64]) for key, value in value[0].items()])
        cursor.executemany("INSERT DELAYED INTO {1}2{0} ({1}_id, {0}_id) VALUES (%s, %s);".format(key, parent_entity), set(value[1]))

daily_type = {'beforelast','lastmonth','thismonth','30days'}
def record_extract(info, dict, doujin_id):
    for date, info in info.items():
        date = int(date)
        if date not in dict:
            price = float(info['price'])
            amount = round(float(info['amount']))
            code_id = info['code']
            dict[date] = (jd2date(date),doujin_id,code_id,price,amount)

def insert_sales_history(historyData,doujin_id, daily_lst, monthly_lst):
    daily_dict = {}
    monthly_dict = {}
    for type, data in historyData.items():
        if(data):
            if(type in daily_type):
                record_extract(data['log'], daily_dict, doujin_id)
            else:
                record_extract(data['log'], monthly_dict, doujin_id)
    daily_lst.extend([value + (key,) for key, value in daily_dict.items()])
    monthly_lst.extend([value + (key,) for key, value in monthly_dict.items()])
    '''
    cursor.executemany("""
                       INSERT INTO SalesRecord_daily (time_str, doujin_id, code_id, price, amount, jd_time) 
                       VALUES (%s, %s, %s, %s, %s, %s);
                       """,[value + (key,) for key, value in daily_dict.items()])
    cursor.executemany("""
                       INSERT INTO SalesRecord_monthly (time_str, doujin_id, code_id, price, amount, jd_time) 
                       VALUES (%s, %s, %s, %s, %s, %s);
                       """,[value + (key,) for key, value in monthly_dict.items()])
    '''
    
def insert_price_history(priceData,doujin_id, price_lst):
    priceinfo_lst = []
    for idx in range(len(priceData['time'])):
        priceinfo_lst.append((date2jd(priceData['time'][idx]),priceData['time'][idx],doujin_id, \
                            None if priceData['dlsite'][idx] == 'None' else priceData['dlsite'][idx], \
                            None if priceData['fanza'][idx] == 'None' else priceData['fanza'][idx]))
    price_lst.extend(priceinfo_lst)
    #cursor.executemany("INSERT INTO doujinPrice (jd_time, time_str, doujin_id, price_DLsite, price_FANZA) VALUES (%s, %s, %s, %s, %s);",priceinfo_lst)

#file_path = r"D:\2024Spring\DLsite-Analysis\data_20240428.json"
file_path = r"D:\2024Spring\DLsite-Analysis\test.json"
if __name__=='__main__':
    print(f"Loading .json file: {file_path} ...")
    items_numbers = iter_count(file_path)
    print(f"{items_numbers} JSON Records Found!")
    #items_numbers = 325000
    # 连接到MySQL服务器
    db = pymysql.connect(host='localhost',user='root',password='Pathfinder', charset='utf8')
    cursor = db.cursor()
    db_name = 'testDB'
    #db_name = 'Real_test_1'

    # 执行删除数据库的 SQL 命令
    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    # 提交更改
    db.commit()

    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
    result = cursor.fetchone()
    # 如果数据库不存在，则创建数据库
    if not result:
            cursor.execute(f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET utf8 COLLATE utf8_bin")
            print(f"数据库 {db_name} 已创建。")
    else:
            print(f"数据库 {db_name} 已存在。")

    # 选择数据库
    cursor.execute("USE {0}".format(db_name))
    create_tables()
    pbar_record = tqdm(total=items_numbers, desc="Record Progress", leave=True, position = 0, mininterval = 1)
    
    Work_main_dict = {}
    reverse_main_dict = {}
    max_workID = 0
    RJ2WorkID_dict = {}
    DM2WorkID_dict = {}
    doujin2WorkID_dict = {}
    Series2CircleID_lst = []
    WorkMain_tmpdict = {'Tag':({},[]),
                        'Circle':({},[]),
                        'Genre':({},[]),
                        'Series':({},[])}
    RJ_tmpdict = {'CV':({},[]),
                'Creator':({},[]),
                'Music':({},[]),
                'Senario':({},[])}
    DM_tmpdict = {'WorkType':({},[])}

    list_bufferSize = 100000
    daily_lst = []
    monthly_lst = []
    price_lst = []

    with open(file_path, 'r', encoding='UTF-8') as f:
        for record in ijson.items(f, 'item'):
            singular_record = True
            if(len(record) < 9):
                singular_record = False
            if(singular_record):
                this_circle = [record['circle'].strip() if record['circle'] else '_missing_']
                this_title = record['title'].strip()
                WorkMain_setDict = {'Tag':set(record['main_tags'],),'Circle':set(this_circle),
                                    'Genre':[record['main_genre']] if record['main_genre'] else [], 'Series':[]}
            else:
                WorkMain_setDict = {'Tag':set(),'Genre':[], 'Series':[]}
            RJ_lstDict = {'CV':[],'Creator':[], 'Music':[], 'Senario':[]}
            DM_lstDict = {'WorkType':[]}
            RJWork_sql = []
            Work_main_id = False
            is_JPver = True

            RJ_para = []
            DM_para = ()
            Child_RJ = None
            Child_RJ_price = None
            Child_DM = None
            Child_DM_price = None
            Child_RJexist = False
            if('SiteInfo' in record):
                SiteInfo = [{k: (None if v == '' else v) for k, v in d.items()} for d in record['SiteInfo']]
                RJ_set = {each['Site'] for each in SiteInfo if each['Site'][0] != 'D'}
                Missing_RJ_set = RJ_set.difference(RJ2WorkID_dict)                                          # [Performance Waring: this step takes a lot of computation]
                RJ_set = RJ_set.difference(Missing_RJ_set)          # exist RJs
                WorkMain_setDict['Tag'] = {item.strip() for d in record['SiteInfo'] for item in d["Tags"]}.union(WorkMain_setDict['Tag'])
                for each in record['release_dtl'].keys():
                    if(each[0] == 'R'):
                        #cursor.execute("SELECT EXISTS (SELECT 1 FROM RJWork WHERE id = %s);", (each[2:]))
                        #is_RJexist = cursor.fetchone()[0]
                        Child_RJexist = each in RJ_set
                        Child_RJ = each
                        if(singular_record):
                            Child_RJ_jd = date2jd(record['release_dtl'][each]['date'])
                            Child_RJ_price = record['release_dtl'][each]['work price']
                    elif(each[0] == 'd'):
                        Child_DM = each
                        if(singular_record):
                            Child_DM_jd = date2jd(record['release_dtl'][each]['date'])
                            Child_DM_price = record['release_dtl'][each]['work price']
                        if(each[-4:] == 'zero'):
                            WorkMain_setDict['Tag'].add('FANZA無料')
                for each in SiteInfo:
                    if(each["Genre"]):
                        WorkMain_setDict['Genre'].append(each["Genre"])
                    if(each['Site'][0]=='D'):
                        if(singular_record or (Child_DM not in DM2WorkID_dict)):
                            if(each["Sub_Genre"]):
                                WorkMain_setDict['Genre'].append(each["Sub_Genre"].strip())
                            if(each["Series"] != '----'):
                                WorkMain_setDict['Series'].append(each["Series"].strip())
                            for key in DM_lstDict.keys():
                                DM_lstDict[key].append(each[key])
                            if(each['Rating_info']):
                                Rating_info = (each['Rating_info']['5'][0],each['Rating_info']['5'][1],each['Rating_info']['4'][0],each['Rating_info']['4'][1],\
                                                each['Rating_info']['3'][0],each['Rating_info']['3'][1],each['Rating_info']['2'][0],each['Rating_info']['2'][1],\
                                                each['Rating_info']['1'][0],each['Rating_info']['1'][1])
                            else:
                                Rating_info = (None,) * 10
                            DM_para = (Child_DM, each['ID'], Child_DM_jd, Child_DM_price, each['Sales'].replace(',','') if each['Sales'] else None,\
                                        each['Favorites'], each['Reviews'], each['CmtReviews'], each['Rating']) + Rating_info
                    else:
                        if(each['Site'] in Missing_RJ_set):
                            if(each["Series"]):
                                WorkMain_setDict['Series'].extend([s.strip() for s in each["Series"]])
                            for key in RJ_lstDict.keys():
                                if(each[key]):
                                    RJ_lstDict[key].extend(each[key])
                            if(len(each['Rating_info'])):
                                Rating_info = (each['Rating_info']['5'],each['Rating_info']['4'],each['Rating_info']['3'],each['Rating_info']['2'],each['Rating_info']['1'])
                            else:
                                Rating_info = (None,) * 5
                            RJWork_sql.append(each['Site'])
                            RJ_para.append((each['Site'], each['ID'] if (each['Site'] == Child_RJ) else None, Child_RJ_jd if (each['Site'] == Child_RJ) else None, Child_RJ_price,\
                                            each['language'] if each['language'] else '日本語', each['Sales'] if each['Sales'] else None, each['Age_Restrict'],\
                                            each['Other_info'], each['Favorites'], each['Reviews'], each['CmtReviews'], each['Rating']) + Rating_info)
                        elif(each['Site'] == Child_RJ):
                            Work_main_id = RJ2WorkID_dict[Child_RJ]
                            is_JPver = each['language'] == '' or each['language'] == '日本語'
            if(singular_record):
                if(Work_main_id):
                    if(is_JPver):
                        #cursor.execute("Select title From Work_main WHERE id = %s",((Work_main_id)))
                        #print(cursor.fetchone())
                        #cursor.execute("UPDATE Work_main SET title = %s WHERE id = %s",((record['title'], Work_main_id)))
                        previous_title = Work_main_dict[Work_main_id] 
                        reverse_main_dict[this_title] = reverse_main_dict.pop(previous_title)
                        Work_main_dict[Work_main_id] = this_title
                elif(this_title in reverse_main_dict):                                                         # [Performance Waring: this step takes a lot of computation]
                    Work_main_id = reverse_main_dict[this_title]
                else:
                    #cursor.execute("INSERT INTO Work_main (title) VALUES (%s);", (record['title']))
                    #Work_main_id = cursor.lastrowid
                    max_workID += 1
                    reverse_main_dict[this_title] = max_workID
                    Work_main_dict[max_workID] = this_title
                    Work_main_id = max_workID
                cursor.execute("""
                INSERT INTO doujinWork (id, main_id ,`Rank`) VALUES (%s, %s, %s);
                """, (record['ID'], Work_main_id, record['extra_info']['Rank'] if len(record['extra_info']) else None))
                #cursor.execute("INSERT INTO Work_main2doujinWork (Work_main_id, doujinWork_id) VALUES (%s, %s);", (Work_main_id, record['ID']))
                doujin2WorkID_dict[record['ID']] = Work_main_id
                insert_price_history(record['price_data'], record['ID'], price_lst)
                insert_sales_history(record['historyData'], record['ID'],daily_lst, monthly_lst)

                if(len(daily_lst) == list_bufferSize):
                    cursor.executemany("""
                       INSERT DELAYED INTO SalesRecord_daily (time_str, doujin_id, code_id, price, amount, jd_time) 
                       VALUES (%s, %s, %s, %s, %s, %s);
                       """,daily_lst)
                    daily_lst = []
                if(len(monthly_lst) == list_bufferSize):
                    cursor.executemany("""
                       INSERT DELAYED INTO SalesRecord_monthly (time_str, doujin_id, code_id, price, amount, jd_time) 
                       VALUES (%s, %s, %s, %s, %s, %s);
                       """,monthly_lst)
                    monthly_lst = []
                if(len(price_lst) == list_bufferSize):
                    cursor.executemany("INSERT DELAYED INTO doujinPrice (jd_time, time_str, doujin_id, price_DLsite, price_FANZA) VALUES (%s, %s, %s, %s, %s);", price_lst)
                    price_lst = []
                
                if(Child_RJexist):
                    cursor.execute("UPDATE RJWork SET doujin_id = %s, ref_price = %s WHERE id = %s",(record['ID'], Child_RJ_price, each['Site'][2:]))
            else:
                Work_main_id = doujin2WorkID_dict[record['ID']]
            if(RJ_para):
                insert_RJ(RJ_para)
                for each in RJWork_sql:
                    RJ2WorkID_dict[each] = Work_main_id
            for key, value in WorkMain_tmpdict.items():
                if key in WorkMain_setDict:
                    insert_dictID(value[0], value[1], set(WorkMain_setDict[key]), Work_main_id)             # [Performance Waring: this step takes a lot of computation]
            #insert_dictID(CircleID_dict, Circle2WorkID_lst, Circle_set, Work_main_id)
            for key, value in RJ_tmpdict.items():
                insert_dictID(value[0], value[1], set(RJ_lstDict[key]), Work_main_id)                       # [Performance Waring: this step takes a lot of computation]
            for key, value in DM_tmpdict.items():
                insert_dictID(value[0], value[1], set(DM_lstDict[key]), Work_main_id)                       # [Performance Waring: this step takes a lot of computation]
            if(DM_para):
                 DM2WorkID_dict[Child_DM] = Work_main_id
                 insert_DM(DM_para)
            if(singular_record and WorkMain_setDict['Series']):
                CircleID = WorkMain_tmpdict['Circle'][0][this_circle[0]]
                Series2CircleID_lst.extend([(CircleID,WorkMain_tmpdict['Series'][0][each]) for each in WorkMain_setDict['Series']])
            pbar_record.update(1)
    pbar_record.close()

    if(len(daily_lst)):
        cursor.executemany("""
            INSERT DELAYED INTO SalesRecord_daily (time_str, doujin_id, code_id, price, amount, jd_time) 
            VALUES (%s, %s, %s, %s, %s, %s);
            """,daily_lst)
    if(len(monthly_lst)):
        cursor.executemany("""
            INSERT DELAYED INTO SalesRecord_monthly (time_str, doujin_id, code_id, price, amount, jd_time) 
            VALUES (%s, %s, %s, %s, %s, %s);
            """,monthly_lst)
    if(len(price_lst)):
        cursor.executemany("INSERT DELAYED INTO doujinPrice (jd_time, time_str, doujin_id, price_DLsite, price_FANZA) VALUES (%s, %s, %s, %s, %s);", price_lst)
    
    print("Data in Memory writing into DATABASE...")
    print("Writing Work_main...")
    cursor.executemany("INSERT DELAYED INTO Work_main (id, title) VALUES (%s, %s);", [(key, value) for key, value in Work_main_dict.items()])
    cursor.execute("ALTER TABLE doujinWork ADD CONSTRAINT main_id2Workmain FOREIGN KEY (main_id) REFERENCES Work_main(id);")
    insert_tmpdict(RJ_tmpdict,'Work_main')
    insert_tmpdict(DM_tmpdict,'Work_main')
    insert_tmpdict(WorkMain_tmpdict,'Work_main')
    print("Writing Relation Charts... --item: {0}".format('DMWork to Work_main'))
    cursor.executemany("INSERT DELAYED INTO Work_main2DMWork (Work_main_id, DMWork_id) VALUES (%s, %s);", [(value, key) for key, value in DM2WorkID_dict.items()])
    print("Writing Relation Charts... --item: {0}".format('doujinWork to Work_main'))
    cursor.executemany("INSERT DELAYED INTO Work_main2doujinWork (Work_main_id, doujinWork_id) VALUES (%s, %s);", [(value, key) for key, value in doujin2WorkID_dict.items()])
    print("Writing Relation Charts... --item: {0}".format('RJWork to Work_main'))
    cursor.executemany("INSERT DELAYED INTO Work_main2RJWork (Work_main_id, RJWork_id) VALUES (%s, %s);", [(value, key) for key, value in RJ2WorkID_dict.items()])
    print("Writing Relation Charts... --item: {0}".format('Circle to Series'))
    cursor.executemany("INSERT DELAYED INTO Circle2Series (Circle_id, Series_id) VALUES (%s, %s);", set(Series2CircleID_lst))
    db.commit()
    create_indexs()
    # 关闭连接
    cursor.close()
    db.close()
    print("ALL Records in DATABASE!")

'''
def insert_lstID(parent, entity, insert_item, ID):
    ### 此函数为弃案
    cursor.execute("SELECT MAX(id) FROM {0};".format(entity))
    result = cursor.fetchone()[0]
    max_id = result if result else 0
    cursor.execute("SELECT * FROM {0} WHERE name in %s;".format(entity), (insert_item,))
    result = cursor.fetchall()
    relation_lst_existed = []
    if(result):
        existed_item = {item[1] for item in result}
        insert_item = insert_item.difference(existed_item)
        relation_lst_existed = [(ID, item[0]) for item in result]
    relation_lst = [(ID, max_id + i) for i, each in enumerate(insert_item, 1)]
    relation_lst.extend(relation_lst_existed)
    if(len(insert_item)):
        cursor.executemany("INSERT INTO {0} (name) VALUES (%s);".format(entity), insert_item)
        ### 这一句话执行的并不正确，在下面这个dictID里进行了修正，此函数为弃案
    cursor.executemany("INSERT INTO {0}2{1} ({0}_id, {1}_id) VALUES (%s, %s);".format(parent,entity), relation_lst)

def insert_ID(parent,entity,str_info,ID):
    ### 此函数为弃案
    if(not(str_info)):
            str_info = 'Empty'
    cursor.execute("""
        INSERT IGNORE INTO {0} (name) VALUES (%s);
        """.format(entity), (str_info))
        # 检查是否有新记录被插入
    if cursor.lastrowid:
            _id = cursor.lastrowid
            #print("新记录的ID是:", cursor.lastrowid)
    else:
            # 如果没有新记录被插入，查询现有记录的ID
            select_sql = "SELECT id FROM {0} WHERE name = %s".format(entity)
            cursor.execute(select_sql, (str_info))
            _id = cursor.fetchone()[0]
            #print("现有记录的ID是:", _id)
    cursor.execute("""
            INSERT IGNORE INTO {0}2{1} ({0}_id, {1}_id) VALUES (%s, %s);
            """.format(parent,entity), (ID, _id))

def count_records(json_path):
    ### 此函数为弃案
    count = 0
    with open(json_path, 'r', encoding='UTF-8') as f:
        for item in ijson.items(f, 'item'):
            count+=1
            if(not(count%1000)):
                print(f"Loading record: {count} ... \r", end="")
        #print(count)
    print(f"{count} Records Loaded!                     ")
    return count   
'''
