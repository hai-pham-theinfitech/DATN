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
    'careerlink.careerlink_proxy.middlewares.SimpleProxyMiddleware': 100, 
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
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
           category_text = category.xpath('text()').get()
           yield scrapy.Request(url=category_url, callback=self.parse_page, meta={'category': category_text})

    def parse_page(self, response):
        self.log(f"crawling category: {response.meta['category']} ")
        job_links = response.xpath('//a[@class="job-link clickable-outside"]/@href').getall()
        for link in job_links:
            link = response.urljoin(link)
            self.log(f'Found job link: {link}')
            yield scrapy.Request(url=link, callback=self.parse_detail, meta={'category': response.meta['category']})
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            next_page = 'https://www.careerlink.vn'+next_page
            yield scrapy.Request(url=next_page,callback=self.parse_page, meta={'category': response.meta['category']})
        
            
    def parse_detail(self, response):
        
        job_title = response.xpath("//h1[@id='job-title']/text()").get()
        job_id = response.url.split('/')[-1]
        address = " ".join(response.css('#job-location *::text').getall())
        full_address = re.sub(r'\s+', ' ', address)
        desc_parts = response.xpath('//div[@id="job-description"]/following-sibling::div[1]//text()').getall()
        job_description = "\n".join([part for part in desc_parts if part])
        benefit_parts = response.xpath('//*[@id="section-job-benefits"]//div[contains(@class, "job-benefit-item")]//span//text()').getall()
        benefits = " ".join(part for part in benefit_parts if part)
        skill_parts = response.xpath('//*[@id="section-job-skills"]//div[@class="raw-content rich-text-content"]//text()').getall()
        skills = [
        s
        for s in skill_parts
        if s and not s.startswith("*")
        ]
        categories = response.xpath('//div[contains(@class, "job-summary-item") and .//div[contains(text(), "Ngành nghề")]]//a/span/text()').getall()
        category_str = ", ".join([c for c in categories if c])
        job_company_url = response.xpath('//a[contains(@href, "/viec-lam-cua/")]/@href').get()
        job_company_id = job_company_url.split('/')[-1] if job_company_url else None
        recruit_detail = {
            'type': 'job',
            'job_id': job_id,
            'source_job_url': response.url,
            'job_title': job_title,
            'job_company_name': response.xpath('//a[contains(@href, "/viec-lam-cua/")]/@title').get(),
            'job_company_url': job_company_url,
            'job_company_id': job_company_id,
            'job_salary': response.xpath("//span[@class='text-primary']/text()").get(),
            'job_location': full_address,
            'job_experience_requirement': response.xpath('//i[contains(@class, "cli-suitcase-simple")]/following-sibling::span/text()').get(),
            'job_description': job_description,
            'job_benefits': benefits,
            'job_skills': skills,
            'job_employment_type':  response.xpath('//div[contains(text(), "Loại công việc")]/following-sibling::div/text()').get(),
            'job_position':  response.xpath('//div[contains(text(), "Cấp bậc")]/following-sibling::div/text()').get(),
            'job_education_requirements': response.xpath('//div[contains(text(), "Học vấn")]/following-sibling::div/text()').get(),
            'job_industry': category_str,  
        }
        yield recruit_detail
        
        
            
          


    
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