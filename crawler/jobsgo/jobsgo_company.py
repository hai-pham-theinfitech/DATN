import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import re
import json
from address import parse_address_company
from normalize_company_name import normalize_company_name
import html

def remove_brackets(text: str) -> str:
    # Loại bỏ tất cả nội dung trong (), bao gồm dấu ()
    return re.sub(r'\s*\(.*?\)\s*', '', text).strip()
# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'careerlink'
    start_urls = [f"https://jobsgo.vn"]
    crawled_company_id = set()
    custom_settings = {
#         'ROTATING_PROXY_LIST': [
#             'http://mobi8:Infi2132@api.yourproxy.click:5108',
#             'http://mobi7:Infi2132@api.yourproxy.click:5107',
#             'http://mobi6:Infi2132@api.yourproxy.click:5106',
#             'http://mobi5:Infi2132@api.yourproxy.click:5105',
#             'http://mobi4:Infi2132@api.yourproxy.click:5104',
#             'http://mobi3:Infi2132@api.yourproxy.click:5103',
#             'http://mobi2:Infi2132@api.yourproxy.click:5102'
#         ],

#         'DOWNLOADER_MIDDLEWARES': {
#     'careerlink.careerlink_proxy.middlewares.SimpleProxyMiddleware': 100, 
#     'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
#     'scrapy.downloadermiddlewares.retry.RetryMiddleware': 120,
# },


        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 32,
        'COOKIES_ENABLED': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        links =  response.xpath('//a[contains(@href, "/viec-lam-")]/@href').getall()
        for link in links:
            yield scrapy.Request(url="https://jobsgo.vn" + link, callback=self.parse_page)
        

    def parse_page(self, response):
        job_links =  response.xpath('//div[@class="col-grid"]//@href').getall()
        job_links = list(set(job_links))
        for link in job_links:
            yield scrapy.Request(url=link, callback=self.parse_detail)

        
    def parse_detail(self, response):
        company_url =  response.xpath('//a[contains(@href, "/tuyen-dung/")]/@href').get()
        job_company_id = company_url.split('/')[-1] if company_url and len(company_url)>0 else None
        if job_company_id and job_company_id not in self.crawled_company_id:
            self.crawled_company_id.add(job_company_id)
            yield scrapy.Request(url=company_url, callback=self.parse_company_detail, meta={'job_company_id': job_company_id})
            
    def parse_company_detail(self, response):
        item = {
    "source_company_url": response.url,
    "company_id": response.url.split('/')[-1],
    "company_name": remove_brackets(response.xpath("//div[@class='company-name']/h1/text()").get()),
                    # or response.xpath("//title/text()").get(),
    "source_company_url": response.xpath("//link[@rel='canonical']/@href").get(),
    "company_description": response.xpath("//meta[@name='description']/@content | //meta[@property='og:description']/@content").get(),

    "company_address": response.xpath(
        "normalize-space(//div[contains(@class,'get-touch')]//li[i[contains(@class,'pb-heroicons-map-pin')]]/span[1])"
    ).get(),
    "province": parse_address_company(response.xpath(
        "normalize-space(//div[contains(@class,'get-touch')]//li[i[contains(@class,'pb-heroicons-map-pin')]]/span[1])"
    ).get(), type="province"),
    "district": parse_address_company(response.xpath(
        "normalize-space(//div[contains(@class,'get-touch')]//li[i[contains(@class,'pb-heroicons-map-pin')]]/span[1])"
    ).get(), type="district"),
    "ward": parse_address_company(response.xpath(
        "normalize-space(//div[contains(@class,'get-touch')]//li[i[contains(@class,'pb-heroicons-map-pin')]]/span[1])"
    ).get(), type="ward"),
    "street": parse_address_company(response.xpath(
        "normalize-space(//div[contains(@class,'get-touch')]//li[i[contains(@class,'pb-heroicons-map-pin')]]/span[1])"
    ).get(), type="street"),
    "company_only_name": normalize_company_name(
       remove_brackets(response.xpath("//div[@class='company-name']/h1/text()").get())),
    
    "company_domain": response.xpath(
        "//div[contains(@class,'get-touch')]//li[i[contains(@class,'pb-heroicons-globe-alt')]]//a/@href"
    ).get(),
   
    "company_industry": response.xpath("//span[@class='company-category-list']/a/span/text()").get()
}


        yield item



        
def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "/opt/airflow/job.json": {"format": "json"},
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