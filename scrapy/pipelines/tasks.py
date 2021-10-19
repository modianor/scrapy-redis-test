# -*- coding: utf-8 -*-
import logging

import logstash

task_logger = logging.getLogger('scrapy_venom')
task_logger.addHandler(logstash.TCPLogstashHandler('localhost', 5000, version=1))


class TaskPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        try:
            pipe = cls.from_settings(crawler.settings)
        except AttributeError:
            pipe = cls()
        pipe.crawler = crawler
        return pipe

    def process_task(self, task, spider):
        spider.logger.info(f'process task: {str(task)}')
        return task
