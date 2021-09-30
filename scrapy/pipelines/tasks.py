# -*- coding: utf-8 -*-


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
