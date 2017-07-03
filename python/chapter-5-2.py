
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

def update_stats(conn, context, type, value, timeout=5):
	# 负责存储统计数据的键
	destination = 'stats:%s:%s' % (context, type)

	# 像commmon_log() 函数一样，处理当前这一个小时的数据和上一个小时的数据。
	start_key = destination + ':start'
	pipe = conn.pipeline(True)
	end = time.time() + timeout
	while time.time() < end:
		try:
			pipe.watch(start_key)
			now = datetime.utcnow().timetuple()
			hour_start = datetime(*now[:4]).isoformat()

			existing = pipe.get(start_key)
			pipe.multi()
			if existing and existing < hour_start:
				pipe.rename(destination, destination + ':last')
				pipe.rename(start_key, destination + ':pstart')
				pipe.set(start_key, hour_start)

			tkey1 = str(uuid.uuid4())
			tkey2 = str(uuid.uuid4())

			# 将值添加到临时键里面
			pipe.zadd(tkey1, 'min', value)
			pipe.zadd(tkey2, 'max', value)

			# 使用聚合函数 MIN 和 MAX， 对存储统计数据的键以及两个临时键进行并集运算
			pipe.zunionstore(destination, [destination, tkey1], aggregate='min')
			pipe.zunionstore(destination, [destination, tkey2], aggregate='max')

			# 删除临时键
			pipe.delete(tkey1, tkey2)

			# 对有序集合中的样本数量、值的和、值得平方之和3个成员进行更新。
			pipe.zincrby(destination, 'count')
			pipe.zincrby(destination, 'sum', value)
			pipe.zincrby(destination, 'sumsq', value * value)

			# 返回基本的技术信息， 以便函数调用者在有需要时做进一步的处理.
			return pipe.execute()[-3:]
		except redis.exceptions.WatchError:
			# 如果新的一个小时已经开始， 并且旧的数据已经被归档， 那么进行重试
			continue

def get_stats(conn, context, type):
	# 程序姜葱这个键里面去除统计数据
	key = 'stats:%s:%s' % (context, type)
	# 获取基本的统计数据， 并将它们都放在一个字典里面
	data = dict(conn.zrange(key, 0, -1, withscores=True))
	# 计算平均值
	data['average'] = data[b'sum'] / data[b'count']
	# 计算标准差的第一个步骤	
	numerator = data[b'sumsq'] - data[b'sum'] ** 2 / data[b'count']
	# 完成标准差的计算工作
	data['stddev'] = (numerator / (data[b'count'] - 1 or 1)) ** .5
	return data

# 将这个Python生成器用作上下文管理器。
@contextlib.contextmanager
def access_time(conn, context):
	# 记录代码块执行前的时间
	start = time.time()
	# 运行被包裹的代码块
	yield
	delta = time.time() - start
	# 更新这一上下文的统计数据
	stats = update_stats(conn, context, 'AccessTime', delta)
	# 计算页面的平均访问时长
	average = stats[1] / stats[0]

	pipe = conn.pipeline(True)
	# 将页面的平均访问时长添加到记录最长访问时间的有序集合里。
	pipe.zadd('slowest:AccessTime', context, average)
	# AccessTime 有序集合只会保留最慢的100条记录
	pipe.zremrangebyrank('slowest:AccessTime', 0, -101)
	pipe.execute()


# 这个视图（view）接受一个Redis链接以及一个生成内容的回调函数作为参数
def process_view(conn, callback):
	# 计算并记录访问时长的上下文管理器就是这样包围代码块的。
	with access_time(conn, request.path):
		# 当上下文管理器中的yield语句被执行时，这个语句就会被执行
		return callback

class request:
	pass

class TestCh05(unittest.TestCase):

	def setUp(self):
		global config_connection
		import redis
		self.conn = config_connection = redis.Redis(db=15)
		self.conn.flushdb()

	def tearDown(self):
		self.conn.flushdb()
		del self.conn
		global config_connection, QUIT, SAMPLE_COUNT
		config_connection = None
		QUIT = False
		SAMPLE_COUNT = 100
		print()
		print()

	def test_stats(self):
		import pprint
		conn = self.conn

		print("Let's add some data for our statistics!")
		for i in range(5):
			r = update_stats(conn, 'temp', 'example', random.randrange(5, 15))
		print("We have some aggregate statistics:", r)
		rr = get_stats(conn, 'temp', 'example')
		print("Which we can also fetch manually:")
		pprint.pprint(rr)

		self.assertTrue(rr[b'count'] >= 5)

	def test_access_time(self):
		import pprint
		conn = self.conn

		print("Let's calculate some access times...")
		for i in range(10):
			with access_time(conn, 'req-%s'%i):
				time.sleep(.5 + random.random())
		print("The slowest access times are:")
		atimes = conn.zrevrange('slowest:AccessTime', 0, -1, withscores=True)
		pprint.pprint(atimes[:10])
		self.assertTrue(len(atimes) >= 10)
		print()

		def cb():
			time.sleep(1 + random.random())

		print("Let's use the callback version...")
		for i in range(5):
			request.path = 'cbreq-%s'%i
			process_view(conn, cb)

		print("The slowest access times are:")
		atimes = conn.zrevrange('slowest:AccessTime', 0, -1, withscores = True)
		pprint.pprint(atimes[:10])
		self.assertTrue(len(atimes) >= 10)

if __name__ == '__main__':
    unittest.main()