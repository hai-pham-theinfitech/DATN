import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import re
import json



# Định nghĩa Spider
class IndexSpider(scrapy.Spider):
    name = 'career_viet'
    start_urls = [f"https://careerviet.vn/viec-lam/tat-ca-viec-lam-vi.html"]
    crawled_company_id = set()
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 0.3,
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 30,
        'CONCURRENT_REQUESTS': 25,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        self.log(f'crawling:  {response.url}')

        # Lấy danh sách các liên kết từ trang index
        links = response.xpath('//a[@class="job_link"]/@href').getall()
        for link in links:
            self.log(f'Found link: {link}')
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
            company_address = ""
            if isinstance(job_locations, list) and len(job_locations) > 0:
                company_address = job_locations[0].get("address", {}).get("streetAddress", "")
            elif isinstance(job_locations, dict):
                company_address = job_locations.get("address", {}).get("streetAddress", "")
            yield {
                'type': 'job',
                'job_id':  response.url.split('/')[-1].replace('.html', ''),
                'job_title': response.xpath("//div[@class='job-desc']//h1/text()").get(),
                'job_company_name': response.xpath("//a[@class='employer job-company-name']/text()").get(),
                'job_company_url': response.xpath("//a[@class='employer job-company-name']/@href").get(),
                'job_benefits': json_ld.get("jobBenefits", ""),
                'job_industry': json_ld.get("industry", ""), 
                'job_created_at': json_ld.get("datePosted", ""),
                'job_valid_through': json_ld.get("validThrough", ""),
                'job_description': ' '.join(json_ld.get("description", "").replace('\xa0', ' ').replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').split()),
                'source_job_url': response.url,
                'job_company_id': response.xpath("//a[@class='employer job-company-name']/@href").get().split('.')[-2],
                'job_salary': json_ld.get("baseSalary", {}).get("value", {}).get("value", ""),
                'job_min_salary': json_ld.get("baseSalary", {}).get("value", {}).get("minValue", ""),
                'job_max_salary': json_ld.get("baseSalary", {}).get("value", {}).get("maxValue", ""),
                'job_salary_level': json_ld.get("baseSalary", {}).get("value", {}).get("unitText", ""),
                'job_exp_requirement': json_ld.get("experienceRequirements", {}).get("monthsOfExperience", ""),
                'job_salary_currency': json_ld.get("baseSalary", {}).get("value", {}).get("currency", ""),
                'job_employment_type': json_ld.get("employmentType", ""),
                'job_skills': json_ld.get("skills", ""),
                'job_working_hours': json_ld.get("workHours", ""),
                'job_education_requirement': json_ld.get("educationRequirements", {}).get("credentialCategory",""),
                'job_street_address': company_address,
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