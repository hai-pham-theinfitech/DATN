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
from urllib.parse import urljoin
# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'careerlink'
    start_urls = [f"https://timviec365.vn"]
    crawled_job_id = set()
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
        'CONCURRENT_REQUESTS': 8,
        'COOKIES_ENABLED': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        links = response.xpath('//a[contains(@href, "/viec-lam-")]/@href').getall()
        for link in links:
            print(link)
            yield scrapy.Request(url= link, callback=self.parse_page)
        

    def parse_page(self, response):
        job_links = response.xpath('//a[@class="tag_th title_cate "]//@href').getall()
        job_links = list(set(job_links))
        for link in job_links:
            print(link)
            if not 'http' in link:
                link = urljoin('https://timviec365.vn', link)
            yield scrapy.Request(url=link, callback=self.parse_detail)
            self.crawled_job_id.add(link)
        next_page = response.xpath('//li[@class="pagi_pre"]/a/@href').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            self.log(f'Following next index page: {next_page_url}')
            yield scrapy.Request(url=next_page_url, callback=self.parse_page)

        
            
    def parse_detail(self, response):
        shiet = {
        "job_id": response.xpath('normalize-space(//h1[contains(@class,"com_post")]/@data-id)').get(),
        "job_title":  response.xpath('normalize-space(//h1[contains(@class,"com_post")])').get(),
        "job_salary": response.xpath('normalize-space(//p[contains(@class,"mluong_mbi")])').get()
                  or response.xpath('normalize-space(//p[contains(@class,"mluong")])').get(),

        "job_company_name": response.xpath('normalize-space(//a[contains(@class,"com_name")]//p[contains(@class,"com_name_text")])').get(),
        "job_company_url":  response.xpath('normalize-space((//a[contains(@class,"com_name")]/@href)[1])').get(),
        "job_company_id": response.xpath('normalize-space((//a[contains(@class,"com_name")]/@href)[1])').get().split('/')[-1],

        "industries": response.xpath('//p[contains(@class,"index")][contains(.,"Ngành nghề")]//a[@class="tag"]/text()').getall(),
        "fields":     response.xpath('//p[contains(@class,"index")][contains(.,"Lĩnh vực")]//a[@class="tag"]/text()').getall(),

        "address": response.xpath('normalize-space(//*[@class="diachi"]//text())').get(),

        "job_employment_type": response.xpath('normalize-space(//p[@class="item_if" and normalize-space()="Hình thức làm việc"]/following-sibling::span[1])').get(),
        "headcount":       response.xpath('normalize-space(//p[@class="item_if" and contains(.,"Số lượng cần tuyển")]/following-sibling::span[1])').get(),
        "probation":       response.xpath('normalize-space(//p[@class="item_if" and normalize-space()="Thời gian thử việc"]/following-sibling::span[1])').get(),
        "degree":          response.xpath('normalize-space(//p[@class="item_if" and normalize-space()="Bằng cấp"]/following-sibling::span[1])').get(),
        "gender":          response.xpath('normalize-space(//p[@class="item_if" and normalize-space()="Giới tính"]/following-sibling::span[1])').get(),
        "age":             response.xpath('normalize-space(//p[@class="item_if" and normalize-space()="Độ tuổi"]/following-sibling::span[1])').get(),
        "job_description": "\n".join(response.xpath('//div[h2[@class="tit_detail_post" and normalize-space()="Mô tả công việc"]]/following-sibling::div[contains(@class,"text_content")][1]//text()[normalize-space()]').getall()),
        "requirements":  "\n".join(response.xpath('//div[contains(@class,"ycau_tdung")]//text()[normalize-space()]').getall()),
        "benefits":     "\n".join(response.xpath('//div[h2[@class="tit_detail_post" and normalize-space()="Quyền lợi"]]/following-sibling::div[contains(@class,"text_content")][1]//text()[normalize-space()]').getall()),
        "source_url": response.url,
}
        yield(shiet)
        print(shiet)
        
        
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