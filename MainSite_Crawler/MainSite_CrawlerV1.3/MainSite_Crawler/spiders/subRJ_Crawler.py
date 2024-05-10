import scrapy,json,os
from ..items import DLsiteCrawlerItem
from tqdm.autonotebook import tqdm

class SubrjCrawlerSpider(scrapy.Spider):
    name = "subRJ_Crawler"
    allowed_domains = ["www.dlsite.com"]
    start_urls = ["http://www.dlsite.com/"]
    work_dict = {}

    pbar_request = None
    pbar_download = None
    crawl_folder_path = None
    workdict_path = None

    terms_filter_simple = {'Series','Illust','Creator','Senario','CV','Music'}
    terms_filter_sets = {'Age_Restrict','Genre','Other_info'}

    Dlsite_terms = {
        "作品形式" : "Genre",
        "ジャンル" : "Tags",
        "シリーズ名" : "Series",
        "作者" : "Creator",
        "シナリオ" : "Senario",
        "イラスト" : "Illust",
        "声優" : "CV",
        "音楽" : "Music",
        "年齢指定" : "Age_Restrict",
        "その他" : "Other_info",
    }

    #def __init__(self, json_path = None, RJ2ID_path = None, *args, **kwargs):
    #    [self.workdict_path,RJ2ID_path] = [json_path,RJ2ID_path]
    def __init__(self, path_lst = None, *args, **kwargs):
        [self.workdict_path,RJ2ID_path] = path_lst
        self.work_dict = {}
        self.RJ2IDs = {}
        self.pbar_request = None
        self.pbar_download = None
        self.crawl_folder_path = os.path.dirname(RJ2ID_path)
        with open(RJ2ID_path,"r+", encoding='utf-8') as f:
            self.RJ2IDs = json.load(f)
        print('{0} RJ records found'.format(len(self.RJ2IDs.items())))

    def DLsite_requests(self, RJ):
        workpage_url = "https://www.dlsite.com/maniax/work/=/product_id/{0}.html/?locale=ja_JP".format(RJ)
        reviews_url = "https://www.dlsite.com/maniax/api/review?product_id={0}&limit=1&mix_pickup=true&locale=ja_JP".format(RJ)
        work_details_url = "https://www.dlsite.com/maniax/product/info/ajax?product_id={0}&locale=ja_JP".format(RJ)
        return workpage_url, [reviews_url, work_details_url]

    def url_extract(self, json_path):
        print('Loading {0} ...'.format(json_path))
        with open(json_path,"r+", encoding='utf-8') as f:
            json_data = json.load(f)
            for record in json_data:
                ID = record['ID']
                self.work_dict[ID] = record['release_dtl']
        print('{0} records loaded'.format(len(self.work_dict.items())))
        for RJ, ID in self.RJ2IDs.items():
            tmp_dict = {}
            tmp_dict[RJ] = False
            self.work_dict[ID].update(tmp_dict)

    #Initialize request
    def start_requests(self):
        if(len(self.RJ2IDs)):
            self.url_extract(self.workdict_path)
            self.pbar_request  = tqdm(total=len(self.RJ2IDs), desc=" Request Progress", leave=True, position = 0 ,mininterval = 1)
            self.pbar_download = tqdm(total=len(self.RJ2IDs), desc="Download Progress", leave=True, position = 1, mininterval = 1)
            for RJ, ID in self.RJ2IDs.items():
                self.pbar_request.update(1)
                url, urls = self.DLsite_requests(RJ)
                yield scrapy.Request(url, callback=self.parse_DLsite_workpage, meta={"ID": ID, "RJ": RJ, "urls": urls})
        else:
            return

    def Dlsite_Extract(self, term, info_item):
        if(term in self.Dlsite_terms):
            item_type = self.Dlsite_terms[term]
            if(item_type in self.terms_filter_simple):
                contents = info_item.xpath(".//td/a")
                info_content = []
                for content_item in contents:
                    info_content.append(content_item.xpath(".//text()").get())
            elif(item_type in self.terms_filter_sets):
                info_content = info_item.xpath(".//td/div/a/span/text()").get()
            elif(item_type == 'Tags'):
                contents = info_item.xpath(".//td/div/a")
                info_content = []
                for content_item in contents:
                    info_content.append(content_item.xpath(".//text()").get().strip())
            else:
                info_content = None
            return item_type, info_content
        return None, None

    def parse_DLsite_workpage(self, response):
        item = DLsiteCrawlerItem()
        item['ID'] = response.meta['ID']
        item['Title'] = response.xpath("//div[@class='base_title_br clearfix']/h1/text()").get().strip()
        item['Circle'] = response.xpath("//table[@id='work_maker']/tr/td/span/a/text()").get().strip()
        language_info = response.xpath("//div[@class='work_edition_linklist type_trans']/a")
        if(len(language_info)):
            for each in language_info:
                RJ_serial = each.xpath(".//@href").get().split("/")[-1].split(".")[0]
                if(RJ_serial == response.meta['RJ']):
                    item['language'] = each.xpath(".//text()").get().strip()
                    break                   ### record not in the default dict
        else:
            item['language'] = '日本語(DEFAULT)'

        basic_info = response.xpath("//table[@id='work_outline']/tr")
        if(len(basic_info)):
            item['Site'] = response.meta['RJ']
            for info_item in basic_info:
                info_title = info_item.xpath(".//th/text()").get()
                item_type, info_content = self.Dlsite_Extract(info_title,info_item)
                if(item_type == None):
                    continue
                item[item_type] = info_content
            next_url = response.meta['urls'][0]
            request = scrapy.Request(next_url, callback=self.parse_DLsite_reviews, meta=response.meta)
            request.meta["item"] = item
            yield request
        else:
            item['Site'] = 'ERROR'
            yield item
    
    def parse_DLsite_reviews(self, response):
        data = json.loads(response.text)
        Reviews_info = {}
        if(data['is_success']):
            for record in data['reviewer_genre_list']:
                Reviews_info[record['name']] = record['genre_count']
            item = response.meta["item"]
            item['Reviews_info'] = Reviews_info
            item['Title'] = data['product_name']
        next_url = response.meta['urls'][1]
        request = scrapy.Request(next_url, callback=self.parse_DLsite_workdetails, meta=response.meta)
        request.meta["item"] = item
        yield request
    
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
        item['Price'] = detail['price']
        item['Reviews'] = detail['rate_count']
        item['Rating'] = detail['rate_average_2dp']
        item['Sales'] = detail['dl_count']
        item['Favorites'] = detail['wishlist_count']
        item['CmtReviews'] = detail['review_count']
        item['Translation_info'] = detail['translation_info']
        return item
