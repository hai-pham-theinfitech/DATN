import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import re
import json
from address import parse_address_company
from normalize_company_name import normalize_company_name


# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'vieclam24_company'
    start_urls = [f"https://vieclam24h.vn/tim-kiem-viec-lam-nhanh"]
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
            if link:
                yield scrapy.Request(url=link, callback=self.parse_detail)
            
        next_page = "https://vieclam24h.vn" + response.xpath('//a[@class="h-8 w-8 rounded-full bg-white border border-se-blue-10 flex justify-center items-center cursor-pointer hover:bg-se-accent-100 hover:text-white"]/@href').getall()[-1]
        if next_page:
            next_page_url = response.urljoin(next_page)
            self.log(f'Following next index page: {next_page_url}')
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse_detail(self, response):
       company_link = response.xpath("//a[contains(@href, '/danh-sach-tin-tuyen-dung')]/@href").get()
       link = "https://vieclam24h.vn" + company_link if company_link else None
       yield scrapy.Request(url=link, callback=self.parse_job)
    
    def parse_job(self, response):  
        next_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not next_data:
            return  
        
    
        data = json.loads(next_data)
        company = data.get("props", {}).get("initialState", {}).get("api", {}).get("employerDetail", {}).get("data", {})
        company_id = company.get('id', '')
        if company_id in self.crawled_company_id:
            self.log(f'Skipping already crawled company ID: {company_id}')
            return
        address = company.get('address', '')
        if address:
            province = parse_address_company(address, type="province")
            district = parse_address_company(address, type="district")
            street = parse_address_company(address, type="street")
            ward = parse_address_company(address, type="ward")
                    
        
        data_company =  {
            'company_id': company.get('id', ''),
            'source_company_url': response.url,
            'company_name': company.get('name', ''),
            'company_domain': response.xpath('//div[@class="truncate" and contains(@title, "https")]/@title').get(),
            'company_only_name': normalize_company_name(company.get('name', '')),
            'company_slug': company.get('slug', ''),
            'company_address': company.get('address', ''),
            'company_province_id': company.get('province', ''),
            'company_description': company.get('description', ''),
            'company_description_html': company.get('description_html', ''),
            'company_tax_code': company.get('tax_code', ''),
            'province': province if address else '',
            'district': district if address else '',
            'ward': ward if address else '',
            'street': street if address else '',
            
            
        }
        self.log(f'Company data: {data_company}')
        self.crawled_company_id.add(company_id)
        yield data_company
            
                
          


    
def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "/opt/airflow/company.json": {"format": "json"},
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