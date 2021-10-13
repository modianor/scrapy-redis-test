#!/usr/bin/env python
# encoding: utf-8
"""
File Description: 
Author: nghuyong
Mail: nghuyong@163.com
Created Time: 2020/4/14
"""
import datetime
import hashlib
import json
import re
import sys
import time
import traceback

import requests
from bs4 import BeautifulSoup

from items import HeiMaoTouSuItem
from scrapy import FormRequest
from scrapy.http import Request
from scrapy.task import Task, TaskStatus
from scrapy_redis.spiders import RedisSpider


class HeiMaoTouSuSpider(RedisSpider):
    name = "heimaotousu_spider"
    base_url = "https://weibo.cn"
    redis_key = "heimaotousu_spider:start_urls"

    start_time = datetime.datetime.utcnow()
    spider_config_dict = {
        'heimaotousu3': ('黑猫投诉', 'crawler.crawler_sina_tousu'),
    }
    need_proxy_list = ['tousu.sina.com.cn']
    complaint_status = ['处理中', '已回复', '已完成']

    last_update_time = ''

    headers = {
        'authority': 'n.sinaimg.cn',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-dest': 'script',
        'referer': 'https://tousu.sina.com.cn/',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cookie': 'SUB=_2A25MUGUADeRhGeFJ61UU-SnLzzmIHXVvChVIrDV9PUJbitCOLUXykWtNfLmCJJgsuhkrsAtufQ1C9RVk-LaXivV8;SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5gf-OcZTHZo0ciYO2F_sPi5NHD95QNS05NSK.NS0BfWs4DqcjMi--NiK.Xi-2Ri--ciKnRi-zNS0M7S0-4S0MXSBtt;Path=/;Domain=.tousu.sina.cn;Expries=Tue Jan',
    }

    def parse(self, response):
        request_task = response.request.task
        page_nums = self.get_company_page_num(response.text)
        for page_num in range(page_nums):
            url = f'https://tousu.sina.com.cn/api/company/main_search?sort_col=1' \
                  f'&sort_ord=2&page_size=10&page={page_num + 1}&_=1632714612466'
            task = Task(spider_name=self.name, task_type='List', url=url, filter=True)
            yield Request(task=task, callback=self.parse_companies, headers=self.headers)
        kibanalog = f'name:{self.name} callback:{sys._getframe(0).f_code.co_name} 解析{page_nums}页'
        request_task.task_status = TaskStatus.SUCCESS
        request_task.kibanalog = kibanalog
        yield request_task
        self.logger.info(f'首页共生成{page_nums}个公司翻页页面')

    def parse_detail(self, response):
        try:
            item = response.meta['item']
            status = response.xpath('//ul[@class="ts-q-list"]/li[last()]/b/text()').get()
            abstract = item['abstract']
            abstract['status'] = status

            steplist = response.xpath('//div[@class="ts-d-item"]').getall()
            steps = list()
            for step in steplist:
                soup = BeautifulSoup(step, 'lxml')
                name = soup.find('span', {'class': 'u-name'}).text
                status = soup.find('span', {'class': 'u-status'}).text
                date = soup.find('span', {'class': 'u-date'}).text
                if '评价' in status:
                    evaluate_u = response.meta['evaluate_u']
                    content = f'服务态度: {evaluate_u["attitude"]}星,处理速度: {evaluate_u["process"]}星,满意度: {evaluate_u["satisfaction"]}星 \n {evaluate_u["evalContent"]}'
                else:
                    content = soup.find('div', {'class': 'ts-d-cont'}).text
                data = {
                    'name': name,
                    'status': status,
                    'date': date,
                    'detail': content,
                }
                steps.append(data)
            item['step_list'] = steps

            item['api_name'] = self.name

            relategroupts_url = response.xpath('//a[@data-sudaclick="relategroupts_view"]/@href').get()
            if relategroupts_url:
                item['group_complaint_id'] = re.search('.*?/view/(.*)', '/grp_comp/view/G17354926946').group(1)

            item['insert_time'] = datetime.datetime.utcnow()
            # item['document_id'] = generate_document_id(dict(item), ['url', 'complaint_status'])
            yield item
        except:
            self.logger.error(traceback.format_exc())

    def parse_companies(self, response):
        try:
            request_task = response.request.task
            json_data = json.loads(response.text)
            lists = json_data['result']['data']['lists']
            for data in lists:
                uid = data['uid']
                company_name = data['title']

                # if company_name != '拼多多':
                #     break

                for idx, status in enumerate(self.complaint_status):
                    url = 'https://tousu.sina.com.cn/api/company/received_complaints'
                    timestamp = str(int(time.time() * 1000))
                    params = self.init_params(couid=uid, c_type=str(idx + 1), page=str(1),
                                              timestamp=timestamp)
                    item = HeiMaoTouSuItem()
                    item['product_name'] = company_name
                    item['complaint_status'] = self.complaint_status[idx]
                    last_update_time = self.get_last_time(product_name=company_name,
                                                          complaint_status=self.complaint_status[idx])
                    meta = {
                        'item': item,
                        'c_type_idx': idx,
                        'uid': uid,
                        'last_update_time': last_update_time,
                        'this_page': 1
                    }
                    task = Task(spider_name=self.name, task_type='List', url=url,
                                param1=json.dumps(params, ensure_ascii=False),
                                # param2=json.dumps(meta, ensure_ascii=False),
                                filter=True)
                    yield FormRequest(task=task, method='GET', formdata=params, headers=self.headers, priority=0,
                                      callback=self.parse_list, meta=meta)
            kibanalog = f'name:{self.name} callback:{sys._getframe(0).f_code.co_name} 解析{len(lists)}家Company'
            request_task.task_status = TaskStatus.SUCCESS
            request_task.kibanalog = kibanalog
            yield request_task
        except:
            self.logger.error(traceback.format_exc())

    def parse_list(self, response):
        try:
            request_task = response.request.task
            item = response.meta['item']
            c_type_idx = response.meta['c_type_idx']
            uid = response.meta['uid']
            last_update_time = response.meta['last_update_time']
            this_page = response.meta['this_page']
            json_data = json.loads(response.text)
            complaints = json_data['result']['data']['complaints']
            pager = json_data['result']['data']['pager']
            next_page = pager['next']
            current_page = pager['current']
            page_amount = pager['page_amount']

            self.logger.info(
                f'公司:{item["product_name"]}, 类型:{item["complaint_status"]}, '
                f'最新更新日期:{last_update_time} 正在解析第{current_page}页投诉翻页')
            for complaint in complaints:
                # 解析投诉列表信息
                complaint_item = item.deepcopy()
                complaint_item['url'] = 'https:' + complaint['main']['url']
                complaint_item['title'] = complaint['main']['title']
                complaint_item['comsumer_name'] = complaint['author']['title']
                complaint_item['wb_profile'] = complaint['author']['wb_profile']
                complaint_item['time'] = self.time_tanser(complaint['main']['timestamp'])

                self.logger.info(f'需要更新：{complaint_item["title"]}')
                abstract = {
                    'complaint_id': complaint['main']['sn'],
                    'complaint_target': complaint['main']['cotitle'],
                    'appeal': complaint['main']['appeal'],
                    'amount_involved': float(complaint['main']['cost'])
                }
                complaint_item['abstract'] = abstract
                evaluate_u = complaint['main']['evaluate_u']

                meta = {
                    'item': complaint_item,
                    'evaluate_u': evaluate_u
                }
                task = Task(spider_name=self.name, task_type='List', url=complaint_item['url'],
                            param2=json.dumps(meta, ensure_ascii=False),
                            filter=True)

                # 生成解析详情页Request
                yield Request(task=task, callback=self.parse_detail, dont_filter=False,
                              headers=self.headers,
                              priority=5, meta=meta)

            # 检查该公司当前列表页最后一条投诉，与最新更新日期比较（经过观察，投诉列表是按照日期递减排序）
            last_complaint = complaints[-1]
            if self.time_tanser(last_complaint['main'][
                                    'timestamp']) >= last_update_time and page_amount >= next_page > this_page:
                next_item = item.deepcopy()
                url = 'https://tousu.sina.com.cn/api/company/received_complaints'
                timestamp = str(int(time.time() * 1000))
                params = self.init_params(couid=uid, c_type=str(c_type_idx + 1), page=str(next_page),
                                          timestamp=timestamp)
                yield FormRequest(url=url, method='GET', formdata=params, headers=self.headers, priority=0,
                                  callback=self.parse_list, meta={
                        'item': next_item,
                        'c_type_idx': c_type_idx,
                        'uid': uid,
                        'last_update_time': last_update_time,
                        'this_page': next_page,
                    })
            else:
                self.logger.warn(
                    f'公司:{item["product_name"]}, 类型:{item["complaint_status"]}, '
                    f'最新更新日期:{last_update_time}, 当前日期:{self.time_tanser(last_complaint["main"]["timestamp"])},'
                    f' 停止投诉翻页解析')
            kibanalog = f'name:{self.name} callback:{sys._getframe(0).f_code.co_name} 解析{len(lists)}家Company'
            request_task.task_status = TaskStatus.SUCCESS
            request_task.kibanalog = kibanalog
            yield request_task
        except:
            self.logger.error(traceback.format_exc())

    def get_company_page(self, url):
        response = requests.get(url, headers=self.headers)
        json_data = response.json()
        return json_data

    def get_company_page_num(self, content):
        json_data = json.loads(content)
        page_nums = json_data['result']['data']['pager']['page_amount']
        return page_nums
        # return page_nums if page_nums and page_nums < 100 else 100

    def get_complaint_page(self, url, params):
        response = requests.get(url, params=params, headers=self.headers)
        json_data = response.json()
        page_nums = json_data['result']['data']['pager']['page_amount']
        return page_nums
        # return page_nums if page_nums and page_nums < 100 else 100

    def get_complaint_page_num(self, url, params):
        page_nums = self.get_complaint_page(url, params)
        return page_nums

    def get_signature(self, params_list):
        params_list.sort()
        s = hashlib.sha256()
        s.update(''.join(params_list).encode('utf-8'))  # Hash the data.
        signature = s.hexdigest()
        return signature

    def init_params(self, couid, c_type='1', page='1', page_size='10', timestamp=''):
        random_str = 'QNT4vu8q79XzrcdM'
        const_str = '$d6eb7ff91ee257475%'

        params_list = [timestamp, random_str, couid, const_str, c_type, page_size, page]
        signature = self.get_signature(params_list)

        return {
            'ts': timestamp,
            'rs': random_str,
            'couid': couid,
            'type': str(c_type),
            "page_size": str(page_size),
            'page': str(page),
            'signature': signature
        }

    def time_tanser(self, timestamp):
        localtime = time.localtime(int(timestamp))
        time_ = time.strftime('%Y-%m-%d %H:%M:%S', localtime)
        return time_

    def get_last_time(self, product_name, complaint_status):
        # db = get_mongo_connection(MONGO_URI_SH_INET, MONGO_DATABASE)
        try:
            # collection: Collection = db['crawler.crawler_sina_tousu']
            # item = next(collection.find({'product_name': product_name, 'complaint_status': complaint_status}).sort(
            #     [('time', -1)]).limit(1))
            return '2000-01-1 00:00:00'
        except StopIteration:
            return '2000-01-1 00:00:00'
