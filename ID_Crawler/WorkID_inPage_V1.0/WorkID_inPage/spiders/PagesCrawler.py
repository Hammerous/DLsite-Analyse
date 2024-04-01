import scrapy,os,re
from tqdm.autonotebook import tqdm
from ..items import PageItem
import pandas as pd

class PagescrawlerSpider(scrapy.Spider):
    name = "PagesCrawler"
    allowed_domains = ["dojindb.net"]
    start_urls = ["https://dojindb.net/s/"]
    crawl_folder_path = None
    target_tags = None
    form_request = {}
    pbar_lst = []
    #initially allow the search of AI works

    #Initialize class
    def __init__(self, csv_path = None, *args, **kwargs):
        super(PagescrawlerSpider, self).__init__(*args, **kwargs)
        if csv_path is None:
            raise ValueError("A CSV Path must be provided")
        # CSV read in
        self.crawl_folder_path = os.path.splitext(csv_path)[0] + '\\'
        if os.path.exists(self.crawl_folder_path) is False:
            os.makedirs(self.crawl_folder_path)
        self.target_tags = self.extract_unique_tags_from_csv(csv_path)

    def extract_unique_tags_from_csv(self, csv_file_path):
        """
        to filter the 'Tag' domain with unique tags
        """
        try:
            df = pd.read_csv(csv_file_path)
            tags = df['Tag'].tolist()
            unique_tags = list(set(tags))  
            return unique_tags
        except Exception as e:
            raise LookupError("Tag Filteration ERROR")

    def get_initail_param(self, response, pbar_id):
        item = PageItem()
        page_info = response.xpath("//span[@class='bar_h1']")
        item['Title'] = page_info.xpath(".//text()").get().strip()
        item['Item_Num'] = page_info.xpath(".//span[@class='badge badge-default']/text()").get().replace("件","").replace(",","")
        item['Current_page'] = 0
        item['ID'] = []
        item['Page_avail'] = True
        sub_pbar = tqdm(total=int(item['Item_Num']), desc="Scanning {0}".format(self.target_tags[pbar_id]), leave=True,\
                                position = pbar_id ,mininterval = 0.5)
        self.pbar_lst.append(sub_pbar)
        return self.page_turning(item, response.url, pbar_id)

    def start_requests(self):
        for tag in self.target_tags:
            self.start_urls.append(self.start_urls[0]+"?t={0}".format(tag))
        self.start_urls.pop(0)
        for idx in range(len(self.start_urls)):
            yield scrapy.Request(url=self.start_urls[idx], callback = lambda response, pbar_id = idx : self.get_initail_param(response,pbar_id), dont_filter = True)
        
        '''        
        yield scrapy.Request(main_url, callback=self.get_initail_param)
        ### request directly will create a cookie, initially disactivate the AI filter

        yield scrapy.FormRequest(main_url, callback=self.get_initail_param, formdata={"hide_ai":"0"}, dont_filter=True)
        ### request with a form with "hide_ai" not NULL will create a cookie, initially activate the AI filter
        '''

    def page_turning(self, item, main_url, pdar_idx):
        if item['Page_avail']:
            item['Current_page'] += 1
            last_prompt = main_url.split("/")[-1]
            if(item['Current_page'] != 1):
                if(last_prompt):
                    page_link = main_url + "&p={0}".format(item['Current_page'])
                else:
                    page_link = main_url + "?p={0}".format(item['Current_page'])
            else:
                page_link = main_url
            request = scrapy.Request(page_link, callback=self.parse, meta={'main_url':main_url,"item":item,'pbar_idx': pdar_idx},dont_filter=True)
            return request
        else:
            return item

    def parse(self, response):
        '''
        ### To check of AI generated works, abandoned
        input_selector = response.css("input")
        fd = {}
        for selector in input_selector:
            name = selector.css("input::attr(name)").extract_first()
            if name is None:
                name = "null"
            value = selector.css("input::attr(value)").extract_first()
            if value is None:
                    value = ""
            fd[name] = value
        print(fd.items())
        '''
        work_list = response.xpath('//div[@class="col-xs-5 col-sm-3 col-md-3"]')
        if(work_list):
            IDs = []
            for work in work_list:
                numbers = work.xpath(".//a/@href").get()
                IDs.append(re.search(r'/w/(\d+)', numbers).group(1))
                self.pbar_lst[response.meta['pbar_idx']].update(1)
            response.meta['item']['ID'].extend(IDs)
        else:
            response.meta['item']['Page_avail'] = False
        return self.page_turning(response.meta['item'], response.meta['main_url'],response.meta['pbar_idx'])