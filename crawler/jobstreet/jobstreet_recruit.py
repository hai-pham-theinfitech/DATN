import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
import html
import re
import uuid


def clean_html(text):
    return html.unescape(text).replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n")

class IndexSpider(scrapy.Spider):
    name = 'job_street_recruit'
    start_urls = ["https://www.jobstreet.vn"]
    crawled_recruit_id = set()
    custom_settings = {
        # 'ROTATING_PROXY_LIST': [
        #     'http://mobi8:Infi2132@api.yourproxy.click:5108',
        #     'http://mobi7:Infi2132@api.yourproxy.click:5107',
        #     'http://mobi6:Infi2132@api.yourproxy.click:5106',
        #     'http://mobi5:Infi2132@api.yourproxy.click:5105',
        #     'http://mobi4:Infi2132@api.yourproxy.click:5104',
        #     'http://mobi3:Infi2132@api.yourproxy.click:5103',
        #     'http://mobi2:Infi2132@api.yourproxy.click:5102'
        # ],

        'DOWNLOADER_MIDDLEWARES': {
    # 'job_street.middlewares.SimpleProxyMiddleware': 100, 
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 120,
},

        
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 20,
        'COOKIES_ENABLED': True,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }


    def start_requests(self):
        print("holy")
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        locations = response.xpath('//a[@data-sp="trending_location"]/@href').getall()
        locations = set(locations)
        for location in locations:
            location = response.urljoin(location)
            yield scrapy.Request(url=location, callback=self.parse_range_limit)

    def parse_range_limit(self, response):
        unlimit_range  = response.xpath('//li[contains(text(), "Tại địa điểm này")]/@data-href').get()
        range = response.urljoin(unlimit_range)
        yield scrapy.Request(url=range, callback=self.parse_job_page)
        
    def parse_job_page(self, response):
        jobs = response.xpath('//a[@class="job-link -mobile-only"]/@href').getall()
        next_page = response.xpath('//a[@class="next-page-button"]/@href').get()
        if next_page:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url=next_page, callback=self.parse_job_page)
        for job in jobs:
            job = response.urljoin(job)
            yield scrapy.Request(url=job, callback=self.parse_job)
    
    def parse_job(self, response):
        raw_json = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        if not raw_json:
            logging.warning(f"No JSON-LD found for {response.url}")
            return
        for json_ld in raw_json:
            if json_ld.strip() and 'JobPosting' in json_ld:
                data = json.loads(json_ld)
                salary = data.get("baseSalary", {}).get("value", {})
                address = data.get("jobLocation", {}).get("address", {})
                id_pattern = r'-([0-9a-fA-F]{32})(?:\?|$)'
                job_id= re.findall(id_pattern, response.url)

                job =  {
            "source_job_url": response.url,
            "job_id": job_id,
            "job_title": data.get("title"),
            "job_description": clean_html(data.get("description", "")),
            "job_date_posted": data.get("datePosted"),
            "job_valid_through": data.get("validThrough"),
            "job_employment_type": data.get("employmentType", []),
            "job_salary_min": float(salary.get("minValue", 0)),
            "job_salary_max": float(salary.get("maxValue", 0)),
            "job_salary_currency": data.get("baseSalary", {}).get("currency", "VND"),
            "job_direct_apply": data.get("directApply", False),
            "job_company_name": data.get("hiringOrganization", {}).get("name"),
            "job_street_address": address.get("streetAddress"),
            "job_district_address": address.get("addressLocality"),
            "job_city_address": address.get("addressRegion"),
            "job_postal_code": address.get("postalCode"),
            "job_country_address": address.get("addressCountry"),
            "job_company_url": "www.jobstreet.vn" + response.xpath("//a[@class='company']/@href").get(),
            "job_company_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, data.get("hiringOrganization", {}).get("name"))),
            }
        
                print(job)
                yield job
        
        
        

    

def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "DEBUG",
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
