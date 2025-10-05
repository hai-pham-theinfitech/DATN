import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import json
import os
import logging
from fake_useragent import UserAgent
from vietnamadminunits import parse_address

from fake_useragent import UserAgent

ua = UserAgent()

file_path = os.path.join(os.path.dirname(__file__), 'output.jl')

BATCH_SIZE = 100

def clean_text(text):
    if text:
        return text.strip().replace('\r\n', '').replace('\n', '').strip()
    return text

def parse_address_company(address, type: str = "full"):
    result = parse_address(address)
    address_dict = {
        'street': result.street,
        'ward': result.ward,
        'district': result.district,
        'province': result.province
    }
    if type == "full":
        return address_dict
    return address_dict.get(type)

# Load tax numbers
tax_numbers = []
with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        if line.strip():
            obj = json.loads(line)
            tax_numbers.append(obj["number"])

class MySpider(scrapy.Spider):
    name = "my_spider"

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'DOWNLOAD_DELAY': 0.1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.5,
        'AUTOTHROTTLE_MAX_DELAY': 5.0,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 4.0,
        'ROBOTSTXT_OBEY': False,
    } 
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
    'Referer': 'https://esgoo.net',  
}

    def start_requests(self):
        for i in range(0, len(set(tax_numbers)), BATCH_SIZE):
            batch = tax_numbers[i:i+BATCH_SIZE]
            for tax in batch:
                url = f"https://api.vietqr.io/v2/business/{tax}"
                
                yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        data = response.json()
        if data['code'] == '00':
            tax_id = data['data']['id']
            name = data['data']['name']
            address = data['data']['address']
            international_name = data['data']['internationalName']
        info = {
            'tax_id': tax_id,
            'name': name,
            'address': address,
            'international_name': international_name,
            'parsed_address': parse_address_company(address, type="full"),
        }
        print(info)
        
        
        

 
     

def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "/opt/airflow/company.json": {"format": "json"},
        },
    })

    dispatcher.connect(lambda: process.stop(), signal=signals.spider_closed)
    process.crawl(MySpider)
    process.start()
    logging.info("Scrapy Spider finished")

if __name__ == "__main__":
    run_index_crawler()
