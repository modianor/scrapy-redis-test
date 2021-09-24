#!/usr/bin/env python
# encoding: utf-8
"""
File Description: 
Author: nghuyong
Mail: nghuyong@163.com
Created Time: 2019-12-07 21:27
"""
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from spiders.comment import CommentSpider
from spiders.tweet import TweetSpider

if __name__ == '__main__':
	# mode = sys.argv[1]
	os.environ['SCRAPY_SETTINGS_MODULE'] = f'settings'
	settings = get_project_settings()
	process = CrawlerProcess(settings)
	# process.crawl(TweetSpider)
	process.crawl(CommentSpider)
	# the script will block here until the crawling is finished
	process.start()
