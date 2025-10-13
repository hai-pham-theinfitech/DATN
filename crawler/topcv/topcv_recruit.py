import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import json
import html


def clean_html(text):
    return html.unescape(text).replace("<li>", "- ").replace("</li>", "\n").replace("<br>", "\n")

class IndexSpider(scrapy.Spider):
    name = 'topcv_company'
    start_urls = ["https://www.topcv.vn/viec-lam/quan-ly-ca-nha-hang-mcdonalds-khu-vuc-vung-tau/1795558.html?ta_source=BoxFeatureJob_LinkDetail"]
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
    'topcv.topcv.middlewares.SimpleProxyMiddleware': 100, 
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


    def start_requests(self):
        print("holy")
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        industries = response.xpath('(//div[@class="ctn-list-jobs"])[1]//div/div/a/@href').getall()
        industries = set(industries)
        for industry in industries:
            industry = response.urljoin(industry)
            yield scrapy.Request(url=industry, callback=self.parse_industry)

    def parse_industry(self, response):
        job_links = response.xpath('//div[@data-box="BoxSearchResult"]//a[contains(@href, "/viec-lam/")]/@href').getall()
        job_links = set(job_links)
        for link in job_links:
            link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_job)

    def parse_job(self, response):
        job_id = response.url.split("/")[-1].split(".")[0]
        json_data = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if not json_data:
            self.log(f"No JSON data found for job ID: {job_id}")
            return
        job_data = json.loads(json_data)
        job_info = {
            "job_id": job_id,
            "source_job_url": response.url,
            "job_company_id": job_data.get("identifier", {}).get("value"),
            "job_title": job_data.get("title", ''),
            "job_company_name": job_data.get("hiringOrganization", {}).get("name", ''),
            "job_company_url": job_data.get("hiringOrganization", {}).get("sameAs"),
            "job_company_logo": job_data.get("hiringOrganization", {}).get("logo"),
            "job_description": clean_html(job_data.get("description", "")),
            "job_industry": job_data.get("industry"),

            "job_street_address": job_data.get("jobLocation", {}).get("address", {}).get("streetAddress"),
            "job_district_address": job_data.get("jobLocation", {}).get("address", {}).get("addressLocality"),
            "job_city_address": job_data.get("jobLocation", {}).get("address", {}).get("addressRegion"),
            "job_postal_code": job_data.get("jobLocation", {}).get("address", {}).get("postalCode"),
            "job_country_address": job_data.get("jobLocation", {}).get("address", {}).get("addressCountry"),

            "job_min_salary": job_data.get("baseSalary", {}).get("value", {}).get("minValue"),
            "job_max_salary": job_data.get("baseSalary", {}).get("value", {}).get("maxValue"),
            "job_salary_currency": job_data.get("baseSalary", {}).get("currency"),

            "job_posted_date": job_data.get("datePosted"),
            "job_valid_date": job_data.get("validThrough"),
            "job_employment_type": job_data.get("employmentType"),
            "job_level": job_data.get("occupationalCategory"),
            "job_experience_requirements": job_data.get("experienceRequirements", {}).get("monthsOfExperience"),
            "job_skills": job_data.get("skills"),
            "job_total_openings": job_data.get("totalJobOpenings"),
            "job_benefits": clean_html(job_data.get("jobBenefits", "")),
        }
        yield job_info

def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "job.json": {"format": "json"},
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
