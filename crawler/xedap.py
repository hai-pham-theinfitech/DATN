import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import logging
import csv

class IndexSpider(scrapy.Spider):
    name = 'index_crawler'
    start_urls = ["https://thongnhat.com.vn/muc/san-pham"] 
    crawled_ids = set()  
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 0.3,
        'CONCURRENT_REQUESTS': 32,
        'RETRY_TIMES': 5,
        'DOWNLOAD_TIMEOUT': 30,
    }

    def parse(self, response):
        self.log(f'Crawling index page: {response.url}')

        fields = response.xpath('//a[@class="woocommerce-LoopProduct-link woocommerce-loop-product__link"]/@href').getall()
        for field in fields:
            if field:
                self.log(f'Link chi tiet: {field}')
                yield scrapy.Request(url=field, callback=self.parse_detail)
        
        next_page = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page: 
            self.log(f'Parse trang: {next_page}')
            yield scrapy.Request(url=next_page, callback=self.parse)
    
    def parse_detail(self, response):
        self.log(f'Parsing detail page: {response.url}')
        
        product_name = response.xpath('//h1[@class="product_title entry-title"]/text()').get()
        
        price = response.xpath('//p[@class="price"]/span[@class="woocommerce-Price-amount amount"]/bdi/text()').get()
        
        thongtinxe = response.xpath('//div[@id="product-spec"]//ul[@class="ttsp"]/li')
        
        bike_info = {
            'url': response.url,
            'product_name': product_name.strip() if product_name else None,
            'price': price.strip() if price else None,
            'doituongsudung': None,
            'cokhung': None,
            'khungsuon': None,
            'phuoc': None,
            'taylac': None,
            'detruoc': None,
            'desau': None,
            'diaxich': None,
            'phanh': None,
            'sizebanh': None,
            'lop': None,
            'vanh': None,
            'taylai': None,
            'trucmayo': None
        }
        
        if len(thongtinxe) > 0:
            for idx, li in enumerate(thongtinxe):
                label = li.xpath('.//span[@class="lable-col"]/text()').get()
                content = li.xpath('.//span[@class="content-col"]/text()').get()
                
                if label and content:
                    label = label.strip()
                    content = content.strip()
                    
                    if 'Đối tượng sử dụng' in label:
                        bike_info['doituongsudung'] = content
                    elif 'Chiều cao phù hợp' in label or 'Size' in label:
                        bike_info['cokhung'] = content
                    elif 'Khung xe' in label:
                        bike_info['khungsuon'] = content
                    elif 'Phuộc' in label:
                        bike_info['phuoc'] = content
                    elif 'Tay bấm đề' in label:
                        bike_info['taylac'] = content
                    elif 'Gạt đĩa' in label:
                        bike_info['detruoc'] = content
                    elif 'Gạt líp' in label:
                        bike_info['desau'] = content
                    elif 'Đùi đĩa' in label:
                        bike_info['diaxich'] = content
                    elif 'Phanh' in label:
                        bike_info['phanh'] = content
                    elif 'Size bánh' in label:
                        bike_info['sizebanh'] = content
                    elif 'Lốp' in label:
                        bike_info['lop'] = content
                    elif 'Vành' in label:
                        bike_info['vanh'] = content
                    elif 'Ghi đông' in label:
                        bike_info['taylai'] = content
                    elif 'Moay ơ' in label:
                        bike_info['trucmayo'] = content
        
        self.log(f'Data : {bike_info}')
        yield bike_info


def run_index_crawler():
    logging.info("Starting Scrapy Spider")
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "INFO",
        "FEEDS": {
            "bike_data.csv": {
                "format": "csv",         # Định dạng CSV
                "overwrite": True,       # Ghi đè lên tệp nếu nó tồn tại
                "encoding": "utf-8",     # Đảm bảo mã hóa UTF-8 cho CSV
            },
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
