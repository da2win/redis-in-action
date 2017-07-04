import bisect
import contextlib
import csv
from datetime import datetime
import functools
import json
import logging
import random
import threading
import time
import unittest
import uuid

import redis

def set_config(conn, type, component, config):
	con.set(
		'config:%s:%s' % (type, component),
		json.dumps(config))

CONFIGS = {}
CHECKED = {}

def get_config(conn, type, component, wait=1):
	key = 'config:%s:%s' % (type, component)

	# 检查是否需要对这个组件的配置信息进行更新。
	if CHECKED.get(key) < time.time() - wait:
		# 有需要对配置进行更新， 记录最后一次检查这个连接的时间。
		CHECKED[key] = time.time()
		# 取出Redis存储的组件配置
		config = json.loads(conn.get(key) or '{}')
		# 将潜在的Unicode关键字参数转换为字符串关键字参数。
		config = dict((str(k), config[k]) for k in config)
		# 取出组件正在使用的配置
		old_config = CONFIGS.get(key)

		# 如果两个配置并不相同....
		if config != old_config:
			# 那么对组件的配置进行更新
			CONFIGS[key] = config

	return CONFIGS.get(key)

REDIS_CONNECTIONS = {}

# 将应用组件的名字传递给装饰器
def redis_connection(component, wait=1):
	# 因为函数每次调用多需要获取这个配置键，所以我们干脆将它缓存起来
	key = 'config:redis:' % component
	# 包装器接收一个函数作为参数
	def warpper(function):
		# 将被包裹函数的一些有用的元数据复制给配置管理器
		@functools.wraps(function)
		# 创建负责管理连接信息的函数
		def call(*args, **kwargs):
			# 如果旧配置存在， 那么获取它
			old_config = CONFIGS.get(key, object())
			# 如果新配置存在， 那么获取它
			_config = get_config(config_connection, 'redis', component, wait)

			config = [unicode()]

			#对配置进行处理， 并将其用于创建redis连接
			for k, v in _config.items():
				config[k.encode('utf-8')] = v

			if config != old_config:
				REDIS_CONNECTIONS[key] = redis.Redis(**config)

			# 将Redis连接以及其他匹配的参数传递给被包裹函数， 然后调用该函数并返回它的的执行结果
			return function(
				REDIS_CONNECTIONS.get(key), *args, **kwargs)

		return call # 返回被包裹的函数
	return wrapper # 返回用于包裹Redis函数的包装器

	

