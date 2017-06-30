
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

QUIT = False
SAMPLE_COUNT = 100

config_connection = None

# 设置一个字典， 将大部分日志的安全级别映射为字符串
SEVERITY_ORG = {                                                    #A
    logging.DEBUG: 'debug',                                     #A
    logging.INFO: 'info',                                       #A
    logging.WARNING: 'warning',                                 #A
    logging.ERROR: 'error',                                     #A
	logging.CRITICAL: 'critical',                               #A
}    

SEVERITY = {}
# 尝试将日志的安全级别转换为简单字符串
SEVERITY.update((name, name) for name in SEVERITY_ORG.values())

def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
	severity = str(SEVERITY.get(severity, severity)).lower()

	# 创建负责存储消息的键
	destination = 'recent:%s:%s' % (name, severity)
	# 将当前时间添加到消息里面，用于记录消息的发送时间
	message = time.asctime() + ' ' + message
	# 使用流水线来将通信往返次数降低为一次
	pipe = pipe or conn.pipeline()
	# 将消息添加到列表的最前面
	pipe.lpush(destination, message)
	# 对日志列表进行修剪，让它包含最新的100条消息
	pipe.ltrim(destination, 0, 99)
	# 执行两个命令
	pipe.execute()

def log_common(conn, name, message, severity=logging.INFO, timeout=5):
	# 设置日志的安全级别
	severity = str(SEVERITY.get(severity, severity)).lower()
	# 负责存储近期的常见日志消息的键
	destination = 'common:%s:%s' % (name, severity)
	# 因为程序每小时需要轮换一次日志， 所以它使用一个键来记录当前所处的小时数
	start_key = destination + ':start'
	pipe = conn.pipeline()
	end = time.time() + timeout
	while time.time() < end:
		try:
			# 对记录当前小时数的键进行监视，确保轮换操作可以正确地执行
			pipe.watch(start_key)
			# 取得当前时间
			now = datetime.utcnow().timetuple()
			# 取得当前所处的小时数
			hour_start = datetime(*now[:4]).isoformat()

			# 取得当前所处的小时数
			existing = str(pipe.get(start_key))
			# 创建一个事务
			pipe.multi()

			# 如果这个常见日志消息列表记录的是上一个小时的日志
			if existing and existing < hour_start:
				# 那么将这些旧的常见日志消息归档
				pipe.rename(destination, destination + ':last')
				pipe.rename(start_key, destination + ':plast')
				# 更新当前所处的小时数
				pipe.set(start_key, hour_start)
			elif not existing:
				pipe.set(start_key, hour_start)
			# 对记录日志出现次数的计数器执行自增操作
			pipe.zincrby(destination, message)
			# log_recent() 函数负责记录日志并调用execute()函数
			log_recent(pipe, name, message, severity, pipe)
			return
		except redis.exceptions.WatchError:
			# 如果程序因为其他客户端正在执行归档操作而出现监视错误， 那么进行重试
			continue



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

	def test_log_recent(self):
		import pprint
		conn = self.conn

		print("Let's write a few logs to the recent log")
		for msg in range(5):
			log_recent(conn, 'test', 'this is message %s' % msg)
		recent = conn.lrange('recent:test:20', 0, -1)
		print("The current recent message log has this many messages:", len(recent))
		print("Those messages includes")
		pprint.pprint(recent[:10])
		self.assertTrue(len(recent) >= 5)

	def test_log_common(self):
		import pprint
		conn = self.conn

		print("Let's write some items to the common log")
		for count in range(1, 6):
			for i in range(count):
				log_common(conn, 'test', "message-%s" % count)
		common = conn.zrevrange('common:test:20', 0, -1, withscores = True)
		print("The current number of common messages is:", len(common))
		print("Those common messages are:")
		pprint.pprint(common)
		self.assertTrue(len(common) >= 5)

if __name__ == '__main__':
	unittest.main()

