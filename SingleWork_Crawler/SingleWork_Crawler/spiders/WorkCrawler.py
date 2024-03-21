import scrapy,time,json,demjson3
from SingleWork_Crawler.items import SingleWork

class WorkcrawlerSpider(scrapy.Spider):
    name = "WorkCrawler"
    allowed_domains = ["dojindb.net"]
    start_urls = ["http://dojindb.net/w/"]
    workID = None

    #Initialize class
    def __init__(self, workID = None, *args, **kwargs):
        super(WorkcrawlerSpider, self).__init__(*args, **kwargs)
        if workID is None:
            raise ValueError("A workID must be provided")
        self.workID = workID

    #Initialize request
    def start_requests(self):
        first_urls = "https://dojindb.net/w/{}".format(self.workID)
        start_urls = first_urls
        print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + " start request: " + start_urls)
        yield scrapy.Request(start_urls, callback=self.parse, meta={'dont_redirect': True, 'handle_httpstatus_list': [301],'dont_filter': True})


    def parse(self, response):
        item = SingleWork()
        item["ID"] = self.workID
        #obtain main genre and title
        title_span = response.xpath('//span[@class="work_title"]')
        item["main_genre"] = title_span.xpath('.//span[@class="label label-lg label-genre"]/text()').get()
        item["title"] = title_span.xpath('.//text()').getall()[-1].strip()

        #obtain circle
        select_xpath = response.xpath('//div[@style="padding:0px 0;"]')
        #link = select_xpath.xpath('.//a[@itemprop="item"]/@href').get()
        item["circle"] = select_xpath.xpath('.//a[@class="link_circle"]/text()').get()

        #obtain price data
        '''
        #below extracts data of sellings in current figure(these data are incorperated in historic data)
        #extract JSON string from JavaScript
        json_string = response.xpath('//script[contains(., "var barChartData =")]/text()').re_first(r'var barChartData = (\{.*?\});')
        data = json.loads(json_string)
        time_labels_sale = data['labels']
        data_sale = data['datasets']
        '''

        #below extracts data of prices
        json_string = response.xpath('//script[contains(., "var barChartData_pc =")]/text()').re_first(r'var barChartData_pc = (\{[\s\S]*?\});')
        data = demjson3.decode(json_string) #convert javascript string to json object
        time_label_price = data['labels']
        dlsite_price = data['datasets'][0]['data']
        fanza_price = data['datasets'][1]['data']
        item["price_data"] = {'time': time_label_price, 'dlsite': dlsite_price, 'fanza': fanza_price}
        
        #obtain main tags
        main_tags = []
        #xpath below is the relative element path, scrapy can automatically match it to the specific content
        tags_box = response.xpath('//div[@class="tags_box mb15"]')
        # scan all 'a' tags
        for tag in tags_box.xpath('.//a[@class="label label-tags"]'):
            # text conten of the tags
            main_tags.append(tag.xpath('.//text()').get())
            #link = tag.xpath('.//@href').get()
        item['main_tags'] = main_tags

         #obtain historic options
        historyData = {}
        #xpath below is the relative element path, scrapy can automatically match it to the specific content
        select_xpath = '//div[@class="col-sm-3 text-right"]/select[@class="form-control graph-range"]'
        options = response.xpath(select_xpath + '/option')
        for option in options:
            option_str = option.xpath('@value').get()
            historyData.update({option_str : 'https://dojindb.net/w/{0}?mode=getgraph&g={1}'.format(self.workID, option_str)})
            #text = option.xpath('text()').get()
        #print(f'Value: {value}, Text: {text}')
        item['historyData'] = {}
        # start iterating requests
        return self.parse_links(response, item, historyData)

    def parse_links(self, response, item, historyData):
        # pop out the next item if there is any
        if historyData:
            # period and url in next item
            period, link_url = historyData.popitem()
            request = scrapy.Request(link_url, callback=self.parse_history, dont_filter=True)
            request.meta['item'] = item
            request.meta['period'] = period
            request.meta['links_to_follow'] = historyData
            return request
        else:
            # all items processed
            return item

    def parse_history(self, response):
        # extract information from responsed data
        link_data = json.loads(response.text)
        
        # yielding data into item
        item = response.meta['item']
        period = response.meta['period']
        item['historyData'][period] = link_data
        
        # process items left
        links_to_follow = response.meta['links_to_follow']
        return self.parse_links(response, item, links_to_follow)