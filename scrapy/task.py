import json


class Task(object):

	def __init__(self, spider_name, task_type, url, param1=None, param2=None, param3=None, **kwargs) -> None:
		self.spider_name = spider_name
		self.task_type = task_type
		self.url = url
		self.param1 = param1
		self.param2 = param2
		self.param3 = param3
		self.task_status = 4
		self.exception = None
		self.data = None
		self.kibanalog = None

	def __str__(self) -> str:
		data = {
			'spider_name': self.spider_name,
			'task_type': self.task_type,
			'url': self.url,
			'param1': self.param1,
			'param2': self.param2,
			'param3': self.param3,
			'task_status': self.task_status,
			'exception': self.exception,
			'data': self.data,
			'kibanalog': self.kibanalog
		}
		return json.dumps(data, ensure_ascii=False)


class Status:
	SUCCESS = 4
	FAIL = 5
