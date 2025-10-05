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
def clean_text(text):
    """Clean text by removing extra whitespace, newlines, and trim"""
    if not text:
        return ''
    text = re.sub(r'[\n\r\t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def get_field_value(response, label_text):
        xpath = f'//div[@class="element_detail"][.//span[contains(text(), "{label_text}")]]//p//text()'
        texts = response.xpath(xpath).getall()
        if texts:
            full_text = ' '.join(texts)
            for text in texts:
                if label_text in text:
                    full_text = full_text.replace(text, '')
            return clean_text(full_text)
        return ''
# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'careerlink'
    start_urls = [f"https://timviec365.vn"]
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
        'CONCURRENT_REQUESTS': 8,
        'COOKIES_ENABLED': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        links = response.xpath('//a[contains(@href, "/viec-lam-")]/@href').getall()
        for link in links:
            print(link)
            yield scrapy.Request(url=link, callback=self.parse_page)
        

    def parse_page(self, response):
        job_links = response.xpath('//h2[@class="box_mb box_new_left_mb"]//@href').getall()
        job_links = list(set(job_links))
        for link in job_links:
         yield scrapy.Request(url=link, callback=self.parse_detail)

        
    def parse_detail(self, response):
        company_url =  response.xpath('normalize-space((//a[contains(@class,"com_name")]/@href)[1])').get()
        job_company_id = company_url.split('/')[-1] if company_url and len(company_url)>0 else None
        if job_company_id and job_company_id not in self.crawled_company_id:
            self.crawled_company_id.add(job_company_id)
            yield scrapy.Request(url=company_url, callback=self.parse_company_detail, meta={'job_company_id': job_company_id})
            
    def parse_company_detail(self, response):
        company_only_name = normalize_company_name( clean_text(response.xpath('//a[@class="name_com"]/h1/text()').get() or ''))
        province = parse_address_company(get_field_value(response, 'Trụ sở chính'), type= 'province')
        district = parse_address_company(get_field_value(response, 'Trụ sở chính'), type= 'district')
        street = parse_address_company(get_field_value(response, 'Trụ sở chính'), type= 'street')
        ward = parse_address_company(get_field_value(response, 'Trụ sở chính'), type= 'ward')
        company_info = {
            'company_id': response.url.split('/')[-1],
            'company_name': clean_text(response.xpath('//a[@class="name_com"]/h1/text()').get() or ''),
            'company_only_name': company_only_name,
            'company_url': clean_text(response.xpath('//a[@class="name_com"]/@href').get() or ''),
            'company_industry': get_field_value(response, 'Lĩnh vực hoạt động'),
            'company_scale': get_field_value(response, 'Quy mô'),
            'province': province,
            'district': district,
            'ward': ward,
            'street': street,
            'tax_code': get_field_value(response, 'Mã số thuế'),
            'company_domain': get_field_value(response, 'Website'),
            'company_address': get_field_value(response, 'Trụ sở chính'),
            'company_established_date': get_field_value(response, 'Ngày thành lập'),
            'company_representative': get_field_value(response, 'Người đại diện'),
            'company_phone': get_field_value(response, 'Điện thoại'),
            'company_email': get_field_value(response, 'Email')
        }
        yield company_info
        print(company_info)



        
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