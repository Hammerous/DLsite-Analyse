import scrapy,time,json,demjson3,datetime,os
import pandas as pd
from SingleWork_Crawler.items import SingleWork

def get_julian_day(date_string):
    date_obj = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    julian_day = date_obj.toordinal() + 1721424.5
    return julian_day+1

def check_identity(list):
    return len(list) and list.count(list[0]) == len(list)

class WorkcrawlerSpider(scrapy.Spider):
    name = "WorkCrawler"
    allowed_domains = ["dojindb.net"]
    start_urls = ["http://dojindb.net/w/"]
    work_dict = {}
    crawl_file_path = None

    #Initialize class
    def __init__(self, csv_path = None, *args, **kwargs):
        super(WorkcrawlerSpider, self).__init__(*args, **kwargs)
        if csv_path is None:
            raise ValueError("A CSV Path must be provided")
        # CSV read in
        self.crawl_file_path = os.path.splitext(csv_path)[0]
        try:
            df = pd.read_csv(csv_path)
            # check required data
            if "ID" in df.columns and "Rank" in df.columns and "Sales" in df.columns:
                for index, row in df.iterrows():
                    self.work_dict[row["ID"]] = {"Rank": str(row["Rank"]), "Sales": str(row["Sales"])}
            else:
                missing_columns = [col for col in ["ID", "rank", "sales"] if col not in df.columns]
                print(f"Missing columns {missing_columns} in {csv_path}")
        except Exception as e:
            print(f"Error reading {csv_path}: {e}")
    
    def historyRecord(self, log_record):
        single_time_stamp = {"site":"",
                            "code":"",
                            "price":"",
                            "amount":"",
                            "time_ymd":"",
                            "campaign":"",
                            "amount_diff":"",
                            "value":""
                            }
        if("price" in log_record and "time_ymd" in log_record and "amount" in log_record):
            single_time_stamp["price"] = str(log_record["price"])
            single_time_stamp["amount"] = str(log_record["amount"])
            single_time_stamp["time_ymd"] = str(log_record["time_ymd"])
            julianDate = str(int(get_julian_day(log_record["time_ymd"])))
        else:
            return None,-1
        if("site" in log_record):
            single_time_stamp["site"] = str(log_record["site"])
        if("code" in log_record):
            single_time_stamp["code"] = str(log_record["code"])
        if("campaign" in log_record):
            single_time_stamp["campaign"] = str(log_record["campaign"])
        if("amount_diff" in log_record):
            single_time_stamp["amount_diff"] = str(log_record["amount_diff"])
        if("value" in log_record):
            single_time_stamp["value"] = str(log_record["value"])
        return single_time_stamp,julianDate

    #Initialize request
    def start_requests(self):
        for key,value in self.work_dict.items():
            first_urls = "https://dojindb.net/w/{}".format(key)
            start_urls = first_urls
            print(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())) + " start request: " + start_urls)
            yield scrapy.Request(start_urls, callback=lambda response,workID=key,extra_info=value: self.parse(response,workID,extra_info), meta={"dont_redirect": True, "handle_httpstatus_list": [301],"dont_filter": True})

    def parse(self, response, workID, extra_info):
        item = SingleWork()
        item["ID"] =  str(workID)
        #obtain main genre and title
        title_span = response.xpath('//span[@class="work_title"]')
        item["main_genre"] = title_span.xpath('.//span[@class="label label-lg label-genre"]/text()').get()
        item["title"] = title_span.xpath('.//text()').getall()[-1].strip()

        #obtain circle
        select_xpath = response.xpath('//div[@style="padding:0px 0;"]')
        #link = select_xpath.xpath(".//a[@itemprop="item"]/@href").get()
        item["circle"] = select_xpath.xpath('.//a[@class="link_circle"]/text()').get()

        #obtain price data
        """
        #below extracts data of sellings in current figure(these data are incorperated in historic data)
        #extract JSON string from JavaScript
        json_string = response.xpath("//script[contains(., "var barChartData =")]/text()").re_first(r"var barChartData = (\{.*?\});")
        data = json.loads(json_string)
        time_labels_sale = data["labels"]
        data_sale = data["datasets"]
        """

        #below extracts data of prices
        json_string = response.xpath('//script[contains(., "var barChartData_pc =")]/text()').re_first(r'var barChartData_pc = (\{[\s\S]*?\});')
        data = demjson3.decode(json_string) #convert javascript string to json object
        time_label_price = data["labels"]
        dlsite_price = data["datasets"][0]["data"]
        dlsite_price = ['None' if x is demjson3.undefined else str(x) for x in dlsite_price]
        if(check_identity(dlsite_price)):
            dlsite_price.append(dlsite_price[0])
        fanza_price = data["datasets"][1]["data"]
        fanza_price = ['None' if x is demjson3.undefined else str(x) for x in fanza_price]
        if(check_identity(fanza_price)):
            fanza_price.append(fanza_price[0])
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
        historyData = {}
        #xpath below is the relative element path, scrapy can automatically match it to the specific content
        select_xpath = "//div[@class='col-sm-3 text-right']/select[@class='form-control graph-range']"
        options = response.xpath(select_xpath + "/option")
        for option in options:
            option_str = option.xpath("@value").get()
            historyData.update({option_str : "https://dojindb.net/w/{0}?mode=getgraph&g={1}".format(workID, option_str)})
            #text = option.xpath("text()").get()
        #print(f"Value: {value}, Text: {text}")
        item["historyData"] = {}
        item["extra_info"] = extra_info
        # start iterating requests
        return self.parse_links(response, item, historyData)

    def parse_links(self, response, item, historyData):
        # pop out the next item if there is any
        if historyData:
            # period and url in next item
            period, link_url = historyData.popitem()
            request = scrapy.Request(link_url, callback=self.parse_history, dont_filter=True)
            request.meta["item"] = item
            request.meta["period"] = period
            request.meta["links_to_follow"] = historyData
            return request
        else:
            # all items processed
            return item

    def parse_history(self, response):
        # extract information from responsed data
        link_data = json.loads(response.text)
        if(len(link_data["log"])):
            single_history_data = {"log":{},"price_sum":-1,"amount_sum":-1}
            for log_record in link_data["log"]:
                single_time_stamp, julianDate = self.historyRecord(log_record)
                if single_time_stamp is None:
                    return item
                single_history_data["log"].update({julianDate:single_time_stamp})
            single_history_data["price_sum"] = str(link_data["price_sum"])
            single_history_data["amount_sum"] = str(link_data["amount_sum"])
            link_data = single_history_data
        else:
            link_data = ""
        # yielding data into item
        item = response.meta["item"]
        period = response.meta["period"]
        item["historyData"][period] = link_data
        
        # process items left
        links_to_follow = response.meta["links_to_follow"]
        return self.parse_links(response, item, links_to_follow)