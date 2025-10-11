# middlewares.py
import random

class SimpleProxyMiddleware:
    def __init__(self, proxy_list):
        self.proxy_list = proxy_list

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('ROTATING_PROXY_LIST'))

    def process_request(self, request, spider):
        proxy = random.choice(self.proxy_list)
        request.meta['proxy'] = proxy
        spider.logger.info(f"[Proxy] Using proxy: {proxy}")
