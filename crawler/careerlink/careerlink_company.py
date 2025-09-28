import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import re
import json

# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'careerlink'
    start_urls = [f"https://www.careerlink.vn/tim-viec-lam-nhanh"]
    crawled_company_id = set()
    custom_settings = {
       

        'DOWNLOADER_MIDDLEWARES': {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 120,
},


        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 8,
        'COOKIES_ENABLED': True,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def parse(self, response):
       categories = response.xpath('//div[@class="jobs-quick-category"]//a')
       for category in categories:
           category_url = response.urljoin(category.xpath('@href').get())
           yield scrapy.Request(url=category_url, callback=self.parse_page)

    def parse_page(self, response):
        company_links = response.xpath('//a[contains(@href, "/viec-lam-cua/")]/@href').getall()

        for link in company_links:
            link = response.urljoin(link)
            id = link.split('/')[-1]
            if id not in self.crawled_company_id:
                self.crawled_company_id.add(id)
                self.log(f'Found company link: {link}')
                yield scrapy.Request(url=link, callback=self.parse_detail)
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            next_page = 'https://www.careerlink.vn'+next_page
            yield scrapy.Request(url=next_page,callback=self.parse_page)
        
            
    def parse_detail(self, response):
        
        name = response.xpath("//h5[@itemprop='name']/text()").get()
        print(f"Crawling company: {response.url}")
        json_data = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        
        for json_data in json_data:
            if 'Professionalservice' in json_data:
                json_ld = json.loads(json_data)
                open_hours_dict = {}

                for spec in json_ld.get("openingHoursSpecification", []):
                    days = spec.get("dayOfWeek", [])
                    opens = spec.get("opens")
                    closes = spec.get("closes")
                    for day in days:
                        open_hours_dict[day] = f"{opens} - {closes}"

                
                company_data = {
                    'company_id': response.url.split('/')[-1],
                    'company_name': json_ld.get('name', ''),
                    'company_representative': json_ld.get("founder", ''),
                    'company_description': json_ld.get('description', ''),
                    'company_email': json_ld.get('email', ''),
                    'company_phone': json_ld.get('telephone', ''),
                    'company_homepage_url': json_ld.get('url', ''),
                    'company_logo': json_ld.get('logo', ''),
                    'company_price_range': json_ld.get('priceRange', ''),
                    'company_street_address': json_ld.get('location', {}).get('address',{}).get('streetAddress', ''),
                    'company_adrress_district': json_ld.get('location', {}).get('address',{}).get('addressLocality', ''),
                    'company_address_address': json_ld.get('location', {}).get('address',{}).get('addressRegion', ''),
                    'company_address_country': json_ld.get('location', {}).get('address',{}).get('addressCountry', ''),
                    'company_postal_code': json_ld.get('location', {}).get('address',{}).get('postalCode', ''),
                    'company_address': response.xpath('//i[@class="cli-map-pin-line d-flex mr-2"]/text()').get(),
                    'company_social_url': json_ld.get('sameAs', []),
                    'company_open_hour': json_ld.get('open_hours',),
                    'company_working_hours': open_hours_dict,
                    
        
                }
                yield company_data
        
        
        
        
        
            
          


    
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