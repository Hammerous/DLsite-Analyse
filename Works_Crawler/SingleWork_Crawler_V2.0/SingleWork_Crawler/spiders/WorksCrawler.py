import scrapy,json,demjson3,datetime,os
from tqdm.autonotebook import tqdm
import pandas as pd
from ..items import SingleWork

def get_julian_day(date_string):
    date_obj = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    julian_day = date_obj.toordinal() + 1721424.5
    return julian_day+1

class WorkcrawlerSpider(scrapy.Spider):
    name = "WorksCrawler"
    allowed_domains = ["dojindb.net"]
    start_urls = ["http://dojindb.net/w/"]
    work_dict = {}
    crawl_folder_path = None
    pbar_request = None
    pbar_download = None

    #Initialize class
    def __init__(self, csv_path = None, *args, **kwargs):
        self.work_dict = {}
        self.crawl_folder_path = None
        self.pbar_request = None
        self.pbar_download = None
        
        super(WorkcrawlerSpider, self).__init__(*args, **kwargs)
        if csv_path is None:
            raise ValueError("A CSV Path must be provided")
        # CSV read in
        self.crawl_folder_path = os.path.splitext(csv_path)[0]
        try:
            df = pd.read_csv(csv_path)
            # check required data
            if "ID" in df.columns:
                for index, row in df.iterrows():
                    self.work_dict[row["ID"]] = {col: str(row[col]) for col in df.columns if col != "ID"}
            else:
                missing_columns = [col for col in ["ID", "rank", "sales"] if col not in df.columns]
                print(f"Missing columns {missing_columns} in {csv_path}")
        except Exception as e:
            print(f"Error reading {csv_path}: {e}")
    #Initialize request
    def start_requests(self):
        self.pbar_request = tqdm(total=len(self.work_dict), desc=" Request Progress", leave=True, position = 0 ,mininterval = 0.5)
        self.pbar_download = tqdm(total=len(self.work_dict), desc="Download Progress", leave=True, position = 1, mininterval = 0.5)
        for key,value in self.work_dict.items():
            first_urls = "https://dojindb.net/w/{}".format(key)
            start_urls = first_urls
            self.pbar_request.update(1)
            yield scrapy.Request(start_urls, callback=lambda response,workID=key,extra_info=value: self.parse(response,workID,extra_info), meta={"dont_redirect": True, "handle_httpstatus_list": [301],"dont_filter": True})

    def dataserie_check(self,time_label_price,dlsite_price,fanza_price):
        if(len(time_label_price)-len(dlsite_price) == 1):
            dlsite_price.append('None')
        if(len(time_label_price)-len(fanza_price) == 1):
            fanza_price.append('None')
        if(len(time_label_price)!=len(fanza_price)!=len(dlsite_price)):
            raise ValueError('price seire ERROR!')

    def convert_url(url):
        parts = url.split('=')
        domains = parts[0].split("/")
        indexs = parts[1].split("/")
        if 'www.dmm.co.jp' == domains[2]:
            cid_index = indexs.index('cid') + 1
            return indexs[cid_index]
        elif 'www.dlsite.com' == domains[2]:
            id_index = parts.index('id') + 1
            return id_index
        else:
            return "Unknown URL format"

    def parse(self, response, workID, extra_info):
        item = SingleWork()
        item["ID"] =  str(workID)
        #obtain main title
        title_span = response.xpath('//span[@class="work_title"]')
        item["main_genre"] = title_span.xpath('.//span[@class="label label-lg label-genre"]/text()').get()
        title = title_span.xpath('.//text()').getall()[-1]
        item["title"] = title.strip()

        release_info = response.xpath('//table[@class="table table-rsp mb15"]/tr')
        item["release_dtl"] = {}
        for sitediff in release_info:
            work_price = sitediff.xpath('.//td/span[@class="work-price"]/text()').get().replace("円","").replace(",","")
            sale_date = sitediff.xpath(".//td/span[@style='font-size:14px;']/text()")
            sales = sale_date[0].get().split(":")[1].replace(",","").replace(" ","")
            released_date = sale_date[1].get().replace("年","-").replace("月","-").replace("日","").replace("\n","").replace(" ","")
            site_url = sitediff.xpath(".//@data-href").get()
            parts = site_url.split("/")
            if 'www.dmm.co.jp' == parts[2]:
                RJ_seiral = parts[-2].split("=")[1]
            elif 'www.dlsite.com' == parts[2]:
                RJ_seiral = parts[-1].split(".")[0]
            item["release_dtl"][RJ_seiral] = {'work price':work_price,'date':released_date,'sales':sales}

        #obtain circle
        select_xpath = response.xpath('//div[@class="col-sm-5 mb20 work_detail"]')
        #link = select_xpath.xpath(".//a[@itemprop="item"]/@href").get()
        circle = select_xpath.xpath('.//table[@class="table mb0"]/tr/td[2]/a/text()').get()
        if(circle):
            item["circle"] = circle.strip()

        #below extracts data of prices
        json_string = response.xpath('//script[contains(., "var barChartData_pc =")]/text()').re_first(r'var barChartData_pc = (\{[\s\S]*?\});')
        data = demjson3.decode(json_string) #convert javascript string to json object
        time_label_price = data["labels"]
        dlsite_price = data["datasets"][0]["data"]
        dlsite_price = ['None' if x is demjson3.undefined else str(x) for x in dlsite_price]
        fanza_price = data["datasets"][1]["data"]
        fanza_price = ['None' if x is demjson3.undefined else str(x) for x in fanza_price]
        self.dataserie_check(time_label_price,dlsite_price,fanza_price)
        item["price_data"] = {"time": time_label_price, 
                              "dlsite": dlsite_price, 
                              "fanza": fanza_price
                              }
        
        #obtain main tags
        main_tags = []
        #xpath below is the relative element path, scrapy can automatically match it to the specific content
        tags_box = response.xpath('//div[@class="tags_box mb15"]')
        # scan all "a" tags
        for tag in tags_box.xpath('.//a[@class="label label-tags"]'):
            # text conten of the tags
            main_tags.append(tag.xpath(".//text()").get())
            #link = tag.xpath(".//@href").get()
        item["main_tags"] = main_tags

        #obtain historic options
        historyData = []
        #xpath below is the relative element path, scrapy can automatically match it to the specific content
        select_xpath = "//div[@class='col-sm-3 text-right']/select[@class='form-control graph-range']"
        options = response.xpath(select_xpath + "/option")
        for option in options:
            option_str = option.xpath("@value").get()
            link_urls = [(option_str, idx, "https://dojindb.net/w/{0}?mode=getgraph&g={1}&site={2}".format(workID, option_str,idx)) for idx in [1,2]]
            historyData.extend(link_urls)
        item["historyData"] = {}
        item["controvertial"] = set()
        item["extra_info"] = extra_info
        # start iterating requests
        return self.parse_links(response, item, historyData)

    def parse_links(self, response, item, historyData):
        # pop out the next item if there is any
        if historyData:
            # period and url in next item
            (period, site_idx, link_url) = historyData.pop()
            request = scrapy.Request(link_url, callback=self.parse_history, dont_filter=True)
            request.meta["item"] = item
            request.meta["site_idx"] = site_idx
            request.meta["period"] = period
            request.meta["links_to_follow"] = historyData
            return request
        else:
            # all items processed
            id_in_page = item["release_dtl"].keys()
            id_in_data = item["controvertial"]
            item["verified"] = list(id_in_data.intersection(id_in_page))
            item["controvertial"] = list(id_in_data.symmetric_difference(id_in_page))
            return item

    def historyRecord(self, log_record):
        # 提前检查必要的键
        required_keys = {"price", "time_ymd", "amount"}
        if not required_keys.issubset(log_record):
            return None, -1

        price = log_record["price"]
        amount = log_record["amount"]
        time_ymd = log_record["time_ymd"]
        code = log_record.get("code", "")
        single_time_stamp = {
            "code": code,
            "price": str(price),
            "amount": str(amount),
            "time_ymd": time_ymd
        }
        julianDate = int(get_julian_day(time_ymd))
        return single_time_stamp, julianDate

    def parse_history(self, response):
        # extract information from responsed data
        link_data = json.loads(response.text)
        item = response.meta["item"]

        if link_data.get("log"):
            single_history_data = {}
            codes_set = set()
            for log_record in link_data["log"]:
                single_time_stamp, julianDate = self.historyRecord(log_record)
                if single_time_stamp:
                    single_history_data[julianDate] = single_time_stamp
                    if(single_time_stamp["code"]):
                        codes_set.add(single_time_stamp["code"])
            '''
            single_history_data = {
                self.historyRecord(log_record)[1]: self.historyRecord(log_record)[0]
                for log_record in link_data["log"] if self.historyRecord(log_record)[0]
            }
            codes_set = {record['code'] for record in single_history_data.values()}
            '''
            site_idx = response.meta["site_idx"]
            period = response.meta["period"]
            item["historyData"].setdefault(period, {})[site_idx] = single_history_data
            item["controvertial"].update(codes_set)

        # process items left
        links_to_follow = response.meta["links_to_follow"]
        return self.parse_links(response, item, links_to_follow)