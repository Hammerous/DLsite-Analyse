import scrapy,ijson,json,os,codecs,re
from ..items import DLsiteCrawlerItem, FANZACrawlerItem
from tqdm.autonotebook import tqdm

class DualsitesSpiderSpider(scrapy.Spider):
    name = "dualsites_spider"
    allowed_domains = ["www.dlsite.com","www.dmm.co.jp"]
    start_urls = ["https://www.dlsite.com/","https://www.dmm.co.jp/"]
    FANZA_cookies =  {'age_check_done':'1'}
    work_dict = {}
    pbar_request = None
    pbar_download = None
    crawl_folder_path = None
    url_sum = 0

    def DLsite_requests(self, RJ):
        workpage_url = "https://www.dlsite.com/maniax/work/=/product_id/{0}.html/?locale=ja_JP".format(RJ)
        reviews_url = "https://www.dlsite.com/maniax/api/review?product_id={0}&limit=1&mix_pickup=true&locale=ja_JP".format(RJ)
        work_details_url = "https://www.dlsite.com/maniax/product/info/ajax?product_id={0}&locale=ja_JP".format(RJ)
        return [workpage_url, reviews_url, work_details_url]

    terms_filter_set = set(['Series','Illust','Creator','Senario','CV','Music'])

    Dlsite_terms = {
        "作品形式" : "Genre",
        "ジャンル" : "Tags",
        "シリーズ名" : "Series",
        "作者" : "Creator",
        "シナリオ" : "Senario",
        "イラスト" : "Illust",
        "声優" : "CV",
        "音楽" : "Music",
        "年齢指定" : "Age_Restrict"
    }

    FANZA_terms = {
        "ゲームジャンル": "Sub_Genre",
        "ジャンル": "Tags",
        "シリーズ": "Series",
        "題材": "WorkType"
    }

    def url_extract(self, json_path):
        print('Loading {0} ...'.format(json_path))
        with open(json_path,"r+", encoding='utf-8') as f:
            for record in ijson.items(f, 'item'):
                if('second status' in record and record['second status'] == 1):
                    continue
                ID = record['ID']
                release_dtl = record['release_dtl']
                urls = {}
                for key,value in release_dtl.items():
                    urls[key] = value['url']
                    self.url_sum += 1
                self.work_dict[ID] = urls
        print('{0} records loaded'.format(len(self.work_dict.items())))

    def __init__(self, json_path = None, *args, **kwargs):
        self.work_dict = {}
        self.pbar_request = None
        self.pbar_download = None
        self.url_extract(json_path)
        self.crawl_folder_path = os.path.splitext(json_path)[0]

    #Initialize request
    def start_requests(self):
        self.pbar_request  = tqdm(total=self.url_sum, desc=" Request Progress", leave=True, position = 0 ,mininterval = 0.5)
        self.pbar_download = tqdm(total=self.url_sum, desc="Download Progress", leave=True, position = 1, mininterval = 0.5)
        for ID, url_info in self.work_dict.items():
            for site_id, url in url_info.items():
                if site_id[0] == 'R':       #RJ serial
                    self.pbar_request.update(1)
                    urls = self.DLsite_requests(site_id)
                    yield scrapy.Request(urls[0], callback=self.parse_DLsite_workpage, meta={"ID": ID, "urls": urls})
                elif site_id[0] == 'd':     #DMM serial
                    self.pbar_request.update(1)
                    yield scrapy.Request(url, callback=self.parse_FANZA, cookies = self.FANZA_cookies, meta={"ID": ID})

    def Dlsite_Extract(self, term, info_item):
        if(term in self.Dlsite_terms):
            item_type = self.Dlsite_terms[term]
            if(item_type in self.terms_filter_set):
                contents = info_item.xpath(".//td/a")
                info_content = []
                for content_item in contents:
                    info_content.append(content_item.xpath(".//text()").get())
            elif(item_type == 'Age_Restrict' or item_type ==  'Genre'):
                info_content = info_item.xpath(".//td/div/a/span/text()").get()
            elif(item_type == 'Tags'):
                contents = info_item.xpath(".//td/div/a")
                info_content = []
                for content_item in contents:
                    info_content.append(content_item.xpath(".//text()").get().strip())
            else:
                info_content = None
            return item_type, info_content
        else:
            return None, None

    def parse_DLsite_workpage(self, response):
        item = DLsiteCrawlerItem()
        item['Site'] = 'RJ'
        item['ID'] = response.meta['ID']
        basic_info = response.xpath("//table[@id='work_outline']/tr")
        for info_item in basic_info:
            info_title = info_item.xpath(".//th/text()").get()
            item_type, info_content = self.Dlsite_Extract(info_title,info_item)
            if(item_type == None):
                continue
            item[item_type] = info_content
        next_url = response.meta['urls'][1]
        request = scrapy.Request(next_url, callback=self.parse_DLsite_reviews, meta=response.meta)
        request.meta["item"] = item
        return request
    
    def parse_DLsite_reviews(self, response):
        data = json.loads(response.text)
        Reviews_info = {}
        for record in data['reviewer_genre_list']:
            Reviews_info[record['name']] = record['genre_count']
        item = response.meta["item"]
        item['Reviews_info'] = Reviews_info
        next_url = response.meta['urls'][2]
        request = scrapy.Request(next_url, callback=self.parse_DLsite_workdetails, meta=response.meta)
        request.meta["item"] = item
        return request
    
    def parse_DLsite_workdetails(self, response):
        data = json.loads(response.text)
        item = response.meta["item"]

        RJ = next(iter(data))
        detail = data[RJ]
        if 'rank' in detail:
            item['Ranking_info'] = detail['rank']
        else:
            item['Ranking_info'] = None
        Rating_info = {}
        for record in detail['rate_count_detail']:
            Rating_level = record['review_point']
            all_rated = record['count']
            Rating_info[Rating_level] = all_rated
        item['Rating_info'] = Rating_info
        item['Reviews'] = detail['rate_count']
        item['Rating'] = detail['rate_average_2dp']
        item['Sales'] = detail['dl_count']
        item['Favorites'] = detail['wishlist_count']
        item['CmtReviews'] = detail['review_count']
        item['Translation_info'] = detail['translation_info']
        self.pbar_download.update(1)
        return item

    def FANZA_Extract(self, term, info_item):
        if(term in self.FANZA_terms):
            item_type = self.FANZA_terms[term]
            if(item_type == 'Sub_Genre'):
                info_content = info_item.xpath(".//dd[@class='informationList__txt']/text()").get().replace("\n","").strip()
            elif(item_type == 'Series' or item_type == 'WorkType'):
                contents = info_item.xpath(".//dd[@class='informationList__txt']")
                content_underA = contents.xpath(".//a/text()").get()
                info_content = (contents.xpath(".//text()").get() if content_underA is None\
                                else content_underA).replace("\n","").strip()
            elif(item_type == 'Tags'):
                contents = info_item.xpath(".//dd[@class='informationList__item']/ul/li")
                info_content = []
                for content_item in contents:
                    info_content.append(content_item.xpath(".//a/text()").get().strip())
            else:
                info_content = None
            return item_type, info_content
        else:
            return None, None

    def parse_FANZA(self, response):
        item = FANZACrawlerItem()
        item['Site'] = 'DM'
        item['ID'] = response.meta['ID']
        Genre = response.xpath("//span[contains(@class, 'c_icon_productGenre')]/text()").get().strip()
        item['Genre'] = Genre

        basic_info = response.xpath("//div[@class='l-areaProductInfo']")
        level0_info = basic_info.xpath(".//div[@class='productSales']")
        item['Sales'] = level0_info.xpath(".//span[@class='numberOfSales__txt']/text()").get()
        string_info = level0_info.xpath(".//span[@class='favorites__txt']/text()").get()
        if(string_info != None):
            item['Favorites'] = re.findall("\d+\.?\d*", string_info)[0]  # regular expression

        level1_info = basic_info.xpath(".//div[@class='productInformation u-common__clearfix']/div")
        for info_item in level1_info:
            info_title = info_item.xpath(".//dt[@class='informationList__ttl']/text()").get()
            item_type, info_content = self.FANZA_Extract(info_title,info_item)
            if(item_type == None):
                continue
            item[item_type] = info_content

        #ranking data extraction
        ranking_data = basic_info.xpath(".//ul[@class='rankingList']/li")
        if(ranking_data.get() != None):
            Ranking_info = {}
            for ranking_item in ranking_data:
                ranking_type = ranking_item.xpath(".//span[@class='rankingList__txt']/text()").get()
                ranking_number = ranking_item.xpath(".//span[@class='rankingList__txt--number']/text()").get()
                # Extract position text
                Ranking_info[ranking_type] = ranking_number
            item['Ranking_info'] = Ranking_info
        
        #rating & reviews data extraction
        review_profile = response.xpath("//div[@class='dcd-review__points']")
        if(review_profile.get() != None):
            item['Rating'] = review_profile.xpath(".//p[1]/strong/text()").get()
            item['Reviews'] = review_profile.xpath(".//p[2]/strong/text()").get()
            item['CmtReviews'] = re.findall("\d+\.?\d*", review_profile.xpath(".//p[2]/text()[2]").get())[0]  # regular expression
            review_details = response.xpath("//div[@class='dcd-review__rating_map']/div")
            Rating_info = {}
            for idx in range(5):
                Rating_level = 5 - idx
                all_rated = review_details[idx].xpath(".//span[3]/text()").get().replace("件","")
                cmt_rate_content = review_details[3].xpath(".//span[3]/following-sibling::a/text()").get()
                if(cmt_rate_content == None):
                    cmt_rated = re.findall("\d+\.?\d*", review_details[3].xpath(".//span[3]/following-sibling::text()").get())[0]
                else:
                    cmt_rated = re.findall("\d+\.?\d*", cmt_rate_content)[0]
                Rating_info[Rating_level] = [all_rated, cmt_rated]
            item['Rating_info'] = Rating_info
        self.pbar_download.update(1)
        return item