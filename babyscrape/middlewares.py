# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from random import choice
from scrapy.http import HtmlResponse
from selenium import webdriver
from scrapy.exceptions import NotConfigured
from scrapy.exceptions import CloseSpider
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import importlib
import os
from pathlib import Path

class BabyscrapeSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class BabyscrapeDownloaderMiddleware(object):
    visited_pages =[]
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        if 'robots.txt' not in request.url:
            if spider.name == 'hotel' and not spider.readmore_clicked:
                return self.readmore_click_response(request, spider)
            else:
                pass
        else:
            return None

    def readmore_click_response(self, request, spider):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(executable_path=str(Path(str(os.getcwd())+'/chromedriver.exe')), chrome_options=options)
        driver.get(request.url)
        readmore_css = 'span._3maEfNCR:nth-of-type(1)'
        attempts = 0
        while attempts < 2:
            try:
                readmore_present = EC.presence_of_element_located((By.CSS_SELECTOR, readmore_css))
                element = WebDriverWait(driver, 5).until(readmore_present)
                element.click()
                break
            except:
                attempts += 1
                print('Did not locate the "Read more" element, retrying: {}/2 '.format(attempts))
        if attempts == 2:
            raise CloseSpider(reason='Readmore Element not Found')

        body = driver.page_source
        drive_url = driver.current_url
        driver.close()
        driver.quit()
        spider.readmore_clicked = True
        return HtmlResponse(url=drive_url, body=body, encoding='utf-8', request=request)

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class RotateUserAgentMiddleware(object):
    """Rotate user-agent for each request."""
    def __init__(self, user_agents):
        self.enabled = False
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        user_agents = crawler.settings.get('USER_AGENT_CHOICES', [])

        if not user_agents:
            raise NotConfigured("USER_AGENT_CHOICES not set or empty")

        o = cls(user_agents)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        return o

    def spider_opened(self, spider):
        self.enabled = getattr(spider, 'rotate_user_agent', self.enabled)

    def process_request(self, request, spider):
        if not self.enabled or not self.user_agents:
            return
        request.headers['user-agent'] = choice(self.user_agents)
        print('User agent switched to : ' + request.headers['user-agent'].decode("utf-8"))

