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

def remove_brackets(text: str) -> str:
    # Loại bỏ tất cả nội dung trong (), bao gồm dấu ()
    return re.sub(r'\s*\(.*?\)\s*', '', text).strip()
# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'careerlink'
    start_urls = [f"https://jobsgo.vn"]
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
        'CONCURRENT_REQUESTS': 32,
        'COOKIES_ENABLED': False,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        links =  response.xpath('//a[contains(@href, "/viec-lam-")]/@href').getall()
        for link in links:
            yield scrapy.Request(url="https://jobsgo.vn" + link, callback=self.parse_page)
        

    def parse_page(self, response):
        job_links =  response.xpath('//div[@class="col-grid"]//@href').getall()
        job_links = list(set(job_links))
        for link in job_links:
            yield scrapy.Request(url=link, callback=self.parse_detail)
            
    
        
    def parse_detail(self, response):
        json_data = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for json_data in json_data:
            if "JobPosting" in json_data:
                json_ld = json.loads(json_data)
                
                address = json_ld.get("jobLocation", [{}])[0].get("address", {})
                salary = json_ld.get("baseSalary", {})
                company = json_ld.get("hiringOrganization", {})
                exp_req = json_ld.get("experienceRequirements")

                if isinstance(exp_req, dict):
                    months = exp_req.get("monthsOfExperience")
                    job_exp_requirement = f"{months} tháng" if months else "Không rõ"
                elif isinstance(exp_req, str):
                    job_exp_requirement = exp_req  # lấy nguyên text
                else:
                    job_exp_requirement = "Không rõ"


                job_info = {
                    "source_job_url": response.url,
                    "job_id": response.url.split('/')[-1].split('-')[-1],
                    "job_title": json_ld.get("title"),
                    "posted_at": json_ld.get("datePosted"),
                    "end_at": json_ld.get("validThrough"),
                    "job_employment_type": json_ld.get("employmentType", []),
                    "job_exp_requirement": job_exp_requirement,
                    "job_edu_requirement": json_ld.get("educationRequirements", {}).get("credentialCategory"),
                    "job_min_salary": salary.get("minValue"),
                    "job_max_salary": salary.get("maxValue"),
                    "job_salary_unit": salary.get("currency"),
                    "job_industry": ", ".join(json_ld.get("industry", [])),
                    "job_description": json_ld.get("description"),
                    "job_benefit": json_ld.get("jobBenefits"),
                    "job_company_name": company.get("name"),
                    "job_company_url": company.get("url"),
                    "job_company_id": company.get("url").split('/')[-1] if company.get("url") else None,
                    "postal_code": address.get("postalCode"),
                }
                
                print(job_info)
                yield job_info
                
            
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