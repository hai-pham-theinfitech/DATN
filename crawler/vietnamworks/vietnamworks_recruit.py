import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
import html

from crawler.match_company import generate_ingest_id


def clean_html(text):
    return html.unescape(text).replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n")


class IndexSpider(scrapy.Spider):
    name = 'vietnamworks_recruit'
   
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 3,
        'RETRY_TIMES': 20,
        'DOWNLOAD_TIMEOUT': 30,
    }
    headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Origin": "https://www.vietnamworks.com",
            "Referer": "https://www.vietnamworks.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            # "x-source": "Page-Container"
        }
    api = "https://vietnamworks.com/job-search/v1.0/search"
    crawled_recruit_ids = set()
    def start_requests(self):

        payload_index = {
            "userId": 0,
            "query": "",
            "filter": [],
            "ranges": [],
            "order": [],
            "hitsPerPage": 50,
            "page": 0
        }

        yield scrapy.Request(
                url=self.api,
                method="POST",
                body=json.dumps(payload_index),
                headers=self.headers,
                callback=self.parse_page,
            )
    
    def parse_page(self,response):
        data = json.loads(response.text)
        total_pages = int(data.get("meta", {}).get("nbPages", 1))
        print(f"Total pages: {total_pages}")
        for page in range(0, total_pages + 1):
            payload = {
                "userId": 0,
                "query": "",
                "filter": [],
                "ranges": [],
                "order": [],
                "hitsPerPage": 50,
                "page": page,
        }
            yield scrapy.Request(
                    url=self.api,
                    method="POST",
                    body=json.dumps(payload),
                    headers=self.headers,
                    callback=self.parse_job_full,
                
            )
        
        
        
        
   

    def clean_html(text):
        return html.unescape(text or "").replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n").strip()

    def parse_job_full(self, response):
        data = json.loads(response.text)
        total_pages = data.get("meta", {}).get("nbPages", 1)
        self.total = total_pages
        
        for job in data.get("data", []):
            job_id = job.get("jobId")
            if job_id in self.crawled_recruit_ids:
                logging.info(f"Skipping already crawled job ID: {job_id}")
                continue
            job =  {
    "type": "job",
    "job_id": job.get("jobId", ""),
    "job_url": job.get("jobUrl", ""),
    "job_title": job.get("jobTitle", ""),
    "job_company_name": job.get("companyName", ""),
    "job_company_id": job.get("companyId", ""),
    "job_locations": [loc.get("address", "") for loc in job.get("workingLocations", [])],
    "job_address": job.get("address", ""),
    "job_language_vi": job.get("languageSelectedVI", ""),

    "job_salary_min": job.get("salaryMin", ""),
    "job_salary_max": job.get("salaryMax", ""),
    "job_salary_display": job.get("prettySalary", ""),
    "job_salary_text": job.get("salary", ""),
    
    "job_level": job.get("jobLevel", ""),
    "job_level_vi": job.get("jobLevelVI", ""),
    
    "job_skills": [s.get("skillName", "") for s in job.get("skills", [])],
    "job_benefits": [b.get("benefitValue", "") for b in job.get("benefits", [])],

    "job_description": clean_html(job.get("jobDescription", "")),
    "job_requirement": clean_html(job.get("jobRequirement", "")),
    
    "job_posted_at": job.get("createdOn", ""),
    "job_expired_at": job.get("expiredOn", ""),
    
    "type_working_id": job.get("typeWorkingId", ""),
    "job_industry": job.get("industriesV3", []),
    "visibility_display": job.get("visibilityDisplay", ""),
    'ingest_id': generate_ingest_id()
}

            
            self.crawled_recruit_ids.add(job_id)
            yield job
            print(job)




def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "job.json": {"format": "json", "encoding": "utf-8"},
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
