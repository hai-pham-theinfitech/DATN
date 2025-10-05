import scrapy
from address import parse_address
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
from address import parse_address_company
from normalize_company_name import normalize_company_name
import html


# def check_crawled_company(media: str):
#     dt = DeltaTable(
#         f"s3://datn/raw/{media}/company",
#         storage_options={
#             "AWS_ACCESS_KEY_ID": "minioadmin",
#             "AWS_SECRET_ACCESS_KEY": "minioadmin",
#             "AWS_ENDPOINT_URL": "http://localhost:9000",
#             "AWS_REGION": "us-east-1",
#             "AWS_ALLOW_HTTP": "true",
#             "AWS_S3_FORCE_PATH_STYLE": "true"
#         }
#     )
#     df = dt.to_pandas(columns=["company_id"])
#     job_ids = df["company_id"].tolist()
#     return job_ids



def clean_html(text):
    return html.unescape(text).replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n")


class IndexSpider(scrapy.Spider):
    name = 'vietnamworks_company'
   
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
    api = "https://ms.vietnamworks.com/job-search/v1.0/search"
    crawled_company_ids = set()
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
                    callback=self.parse_company_full,
                
            )
        
        
        

    def clean_html(text):
        return html.unescape(text or "").replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n").strip()

    def parse_company_full(self, response):
        data = json.loads(response.text)
        total_pages = data.get("meta", {}).get("nbPages", 1)
        self.total = total_pages
        
        # crawled_company_ids = check_crawled_company("vietnamworks")
        for company in data.get("data", []):
            company_id = company.get("companyId")
        
         
            company =  {
    "type": "company",
     "company_id": company_id,
    "company_name": company.get("companyName"),
    "company_only_name": normalize_company_name(company.get("companyName")) if company.get("companyName") else None,
    "company_profile": clean_html(company.get("companyProfile")),
    "company_size": company.get("companySize"),
    "company_logo": company.get("companyLogo"),
    "company_address": company.get("address"),
    "province": parse_address_company(company.get("address"), type="province") if company.get("address") else "",
    "district": parse_address_company(company.get("address"), type="district") if company.get("address") else "",
    "ward": parse_address_company(company.get("address"), type="ward") if company.get("address") else "",
    "street": parse_address_company(company.get("address"), type="street") if company.get("address") else "",
    "industry": company.get("industriesV3")[0]["industryV3Name"] if company.get("industriesV3") else "",
    
}

            
            self.crawled_company_ids.add(company_id)

            print(company)
            yield company




def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "company.json": {"format": "json", "encoding": "utf-8"},
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
