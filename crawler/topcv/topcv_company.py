import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
import html
from normalize_company_name import normalize_company_name
from address import parse_address_company

def clean_html(text):
    return html.unescape(text).replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n")

class IndexSpider(scrapy.Spider):
    name = 'topcv_company'
    start_urls = ["https://www.topcv.vn/viec-lam/quan-ly-ca-nha-hang-mcdonalds-khu-vuc-vung-tau/1795558.html?ta_source=BoxFeatureJob_LinkDetail"]
    crawled_company_id = set()
    custom_settings = {
        'ROTATING_PROXY_LIST': [
            'http://mobi8:Infi2132@api.yourproxy.click:5108',
            'http://mobi7:Infi2132@api.yourproxy.click:5107',
            'http://mobi6:Infi2132@api.yourproxy.click:5106',
            'http://mobi5:Infi2132@api.yourproxy.click:5105',
            'http://mobi4:Infi2132@api.yourproxy.click:5104',
            'http://mobi3:Infi2132@api.yourproxy.click:5103',
            'http://mobi2:Infi2132@api.yourproxy.click:5102'
        ],

        'DOWNLOADER_MIDDLEWARES': {
    'topcv.topcv.middlewares.SimpleProxyMiddleware': 100, 
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 120,
},


        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 12,
        'COOKIES_ENABLED': True,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }


    def start_requests(self):
        print("holy")
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        industries = response.xpath('(//div[@class="ctn-list-jobs"])[1]//div/div/a/@href').getall()
        industries = set(industries)
        for industry in industries:
            industry = response.urljoin(industry)
            yield scrapy.Request(url=industry, callback=self.parse_industry)

    def parse_industry(self, response):
        company_links = response.xpath('//a[@class="company"]/@href').getall()
        company_links = set(company_links)
        for link in company_links:
            link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_company)

    def parse_company(self, response):
        company_address = response.xpath('normalize-space(//div[@class="desc"]/text())').get()
        province = parse_address_company(company_address, type='province')
        ward = parse_address_company(company_address, type='ward')
        district = parse_address_company(company_address, type='district')
        street = parse_address_company(company_address, type='street')
        
        size = ""
        subtext = response.xpath("//span[@class='company-subdetail-info-text']/text()").getall()
        for compt in subtext:
            compt = compt.strip()
            if "nhân viên" in compt:
                size = compt.replace("nhân viên","").strip()
        domain  = response.xpath(
    '//div[contains(@class,"company-subdetail-info") and contains(@class,"website")]//a/@href'
).get()
        import re
        domain = re.sub(r'/+$', '', domain ) if domain else ""

            
        


        company_data = {
            "source_company_url": response.url,
            "company_id": response.url.split('/')[-1].replace('.html',''),
            "company_name": response.xpath("//h1[@class='company-detail-name text-highlight']/text()").get(),
            "company_only_name": normalize_company_name(response.xpath("//h1[@class='company-detail-name text-highlight']/text()").get()),
            "company_description": "\n".join(response.xpath("//div[@class='content']/p/text()").getall()),
            "company_address": company_address,
            "province": province,
            "district": district,
            "ward": ward,
            "street": street,
            "company_domain": domain,
            "company_size": size,
            
            
        }
        
        print(company_data)
        yield company_data
        

def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "job.json": {"format": "json"},
        },
    })

    def stop_scrapy():
        logging.info("Stopping Scrapy Spider")
        process.stop()

    dispatcher.connect(stop_scrapy, signal=signals.spider_closed)
    process.crawl(IndexSpider)
    process.start()
    logging.info("Scrapy Spider finished")

if __name__ == "__main__":
    run_index_crawler()
