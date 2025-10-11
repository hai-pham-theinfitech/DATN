import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
from address import parse_address_company
from normalize_company_name import normalize_company_name
class IndexSpider(scrapy.Spider):
    name = 'index_crawler'
    start_urls = ["https://careerviet.vn/viec-lam-noi-bat-trong-tuan-l4a10p"]  
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
            job_locations = json_ld.get("jobLocation", [])

            # Khởi tạo trường company_address
            company_address = ""
            company_postal_code = ""

            if isinstance(job_locations, list) and len(job_locations) > 0:
                company_address = job_locations[0].get("address", {}).get("streetAddress", "")
                company_postal_code = job_locations[0].get("postalCode", "")
            elif isinstance(job_locations, dict):
                company_address = job_locations.get("address", {}).get("streetAddress", "")
                company_postal_code = job_locations.get("postalCode", "")
            
            company = json_ld.get("hiringOrganization", {})
            data = {
                'type': 'company',
                'company_id': json_ld.get("identifier", {}).get("value", ""),
                'company_name': company.get("name", ""),
                'company_description': company.get("description", ""),
                'company_only_name': normalize_company_name(company.get("name", "")) if company.get("name", "") else None,
                'company_url': company.get("url", ""),
                'company_domain': response.xpath('//li[contains(text(), "Website:")]/text()').get().replace('Website:', '').strip() if response.xpath('//li[contains(text(), "Website:")]/text()').get() else "",
                'company_logo': company.get("logo", ""),
                'company_address': company_address,
                'province': parse_address_company(company_address) if company_address else "",
                'district': parse_address_company(company_address, type="district") if company_address else "",
                'ward': parse_address_company(company_address, type="ward") if company_address else "",
                'street': parse_address_company(company_address, type="street") if company_address else "",
                'source_company_url': response.url,
                'company_postal_code': company_postal_code if company_postal_code else "",
            }
            print(data)
            yield data



def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "/opt/airflow/master_company.json": {"format": "json"},
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