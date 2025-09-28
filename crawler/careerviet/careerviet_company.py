import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'index_crawler'
    start_urls = ["https://careerviet.vn/viec-lam-noi-bat-trong-tuan-l4a10p"]  # Trang index ban đầu
    crawled_ids = set()  
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 0.3,
        'CONCURRENT_REQUESTS': 32,
        'RETRY_TIMES': 5,
        'DOWNLOAD_TIMEOUT': 30,
        
    }

    def parse(self, response):
        self.log(f'Crawling index page: {response.url}')

        captions = response.xpath('//div[@class="caption"]')
        for caption in captions:
            company_id = caption.xpath('.//a[@class="company-name"]/@data-empid').get()
            link = caption.xpath('.//a[@class="job_link"]/@href').get()
            if company_id not in self.crawled_ids and link:
                self.crawled_ids.add(company_id)
                self.log(f'Found link: {link} with company ID: {company_id}')
                yield scrapy.Request(url=link, callback=self.parse_detail)
        
        next_page = response.xpath('//li[@class="next-page"]/a/@href').get()
        if next_page: 
            next_page_url = response.urljoin(next_page)
            self.log(f'Following next index page: {next_page_url}')
            yield scrapy.Request(url=next_page_url, callback=self.parse)
    
    def parse_detail(self, response):
        self.log(f'Crawling detail page: {response.url}')
        json_ld_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for json_raw in json_ld_scripts:
            json_raw = json_raw.replace('\r', '').replace('\n', '').replace('\t', '')
            if "JobPosting" not in json_raw:
                continue
            json_ld = json.loads(json_raw)
     
            
            company = json_ld.get("hiringOrganization", {})
            yield {
                'type': 'company',
                'company_id': json_ld.get("identifier", {}).get("value", ""),
                'company_name': company.get("name", ""),
                'company_url': company.get("url", ""),
                'company_homepage': company.get("sameAs", ""),
                'company_logo': company.get("logo", ""),
                'company_street_address': json_ld.get("jobLocation", {}).get("streetAddress", ""),
                'company_locality_address': json_ld.get("jobLocation", {}).get("addressLocality", ""),
                'company_region_address': json_ld.get("jobLocation", {}).get("addressRegion", ""),
                'company_country_address': json_ld.get("jobLocation", {}).get("addressCountry", ""),
                'company_postal_code': json_ld.get("jobLocation", {}).get("postalCode", ""),
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