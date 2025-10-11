import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import re
import json
import random

from address import parse_address_company

# Định nghĩa Spider

class IndexSpider(scrapy.Spider):
    name = 'vieclam24_recruit'
    start_urls = ["https://vieclam24h.vn/tim-kiem-viec-lam-nhanh"]
    crawled_company_id = set()

    handle_httpstatus_list = [403, 429, 500, 502, 503, 504]

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
    'vieclam24h.vieclam24h_proxy.middlewares.SimpleProxyMiddleware': 100, 
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


    def parse(self, response):
        self.log(f'crawling:  {response.url}')

        # Lấy danh sách các liên kết từ trang index
        links = response.xpath('//a[@target="_blank"]/@href').getall()[:30]
        for link in links:
            link = "https://vieclam24h.vn" + link 
            self.log(f'Found link: {link}')
            yield scrapy.Request(url=link, callback=self.parse_detail)
            
        next_page = "https://vieclam24h.vn" + response.xpath('//a[@class="h-8 w-8 rounded-full bg-white border border-se-blue-10 flex justify-center items-center cursor-pointer hover:bg-se-accent-100 hover:text-white"]/@href').getall()[-1]
        if next_page:
            next_page_url = response.urljoin(next_page)
            self.log(f'Following next index page: {next_page_url}')
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse_detail(self, response):
        self.log(f'Crawling detail page: {response.url}')
        json_ld_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for json_raw in json_ld_scripts:
            if "JobPosting" not in json_raw:
                continue
            json_ld = json.loads(json_raw)
          
            # Lấy dữ liệu từ trang chi tiết
            yield {
                'type': 'job',
                'source_job_url': response.url,
                'job_title': json_ld.get('title', ''),
                'job_id': response.url.split('/')[-1].replace('.html', ''),
                'job_employment_type': json_ld.get('employmentType', ''),
                'job_date_posted': json_ld.get('datePosted', ''),
                'job_valid_through': json_ld.get('validThrough', ''),
                'job_industry': json_ld.get('industry', ''),
                'job_education_requirements': json_ld.get('qualifications', ''),
                'job_min_salary': json_ld.get("baseSalary", {}).get("value", {}).get("minValue", 0),
                'job_max_salary': json_ld.get("baseSalary", {}).get("value", {}).get("maxValue", 0),
                'job_currency': json_ld.get("baseSalary", {}).get("value", {}).get("currency", ''),
                'job_description': json_ld.get('description', ''),
                'job_benefits': json_ld.get('benefits', ''),
                'job_skills': ", ".join(json_ld['skills']) if isinstance(json_ld.get('skills'), list) else json_ld.get('skills', ''),
                'job_street_address': json_ld.get('jobLocation', [{}])[0].get('address', {}).get('streetAddress', ''),
                'job_postal_code': json_ld.get('jobLocation', [{}])[0].get('address', {}).get('postalCode', ''),
                'job_company_name':  json_ld.get("hiringOrganization", {}).get("name", ""),
                'job_company_url': json_ld.get("hiringOrganization", {}).get("sameAs", ""),
                'job_company_id': json_ld.get("identifier", {}).get("value", ""),
                'job_company_logo': json_ld.get("hiringOrganization", {}).get("logo", ""),

            }
           
        

    
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