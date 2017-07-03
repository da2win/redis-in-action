
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

# 以秒为单位的计数器经度，分别为1秒、5秒、1分钟、5分钟、1小时、5小时、一天
# 用户可以按需调整这些精度
PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]

def update_counter(conn, name, count = 1, now = None):
	# 通过取得当前时间来判断应该对哪个时间片执行自增操作
	now = now or time.time()
	# 为了保证之后的清理工作可以正确地执行，这里需要创建一个事务型流水线
	pipe = conn.pipeline()
	# 为我们记录的每一种精度都创建一个计数器
	for prec in PRECISION:
		# 取得当前时间片的开始时间
		pnow = int(now / prec) * prec
		# 创建负责存储技术信息的散列
		hash = '%s:%s' % (prec, name)
		# 将计数器的引用信息添加到有序集合里面，并将其分值
		# 设置为0， 以便在之后执行清理操作
		pipe.zadd('known:', hash, 0)
		# 对给定名字和精度的计数器进行更新
		pipe.hincrby('count:' + hash, pnow, count)
	pipe.execute()

def get_counter(conn, name, precision):
	# 取得存储计数器数据的键的名字
	hash = '%s:%s' % (precision, name)
	# 从Redis里面取出计数器数据
	data = conn.hgetall('count:' + hash)
	to_return = []
	# 将计数器数据转换成指定的格式
	for key, value in data.items():
		to_return.append((int(key), int(value)))
	# 对数据进行排序，把旧的数据样本排在前面
	to_return.sort()
	return to_return 

def clean_counters(conn):
	pipe = conn.pipeline(True)
	# 为了平等地处理更新频率各不相同的多个计数器，
	# 程序需要记录清理操作执行的次数。
	passes = 0

	# 持续地对计数器进行清理，直到退出为止。
	while not QUIT:
		# 记录清理操作开始执行的时间，这个值将被用于计算清理操作的执行时长。
		start = time.time
		# 渐进地遍历所有已知的计数器。
		index = 0
		while index < conn.zcard('known:'):
			# 取得被检查计数器的数据。
			hash = conn.zrange('known:', index, index)
			index += 1
			if not hash:
				break
			hash = hash[0].decode('utf-8')
			print(hash)
			# 取得计数器的精度
			prec = int(hash.partition(':')[0])

			# 因为清理程序每60秒就会循环一次， 所以这里需要根据计数器的更新频率
			# 来判断是否真的有必要对计数器进行清理。
			bprec = int(prec // 60) or 1

			# 如果这个计数器在这次循环里不需要进行清理，那么检查下一个计数器。（
			# 举个例子，如果清理程序只循环了3次，而计数器的更新频率为每5分钟一次，
			# 那么程序暂时还不需要对这个计数器进行清理。
			if passes % bprec:
				continue

			hkey = 'count:' + hash
			# 根据给定的精度以及需要保留的样本数量，计算出我们需要保留什么时间
			# 之前的样本
			cutoff = time.time - SAMPLE_COUNT * prec
			#  获取样本的开始时间，并将其从字符串转为整数
			samples = map(int, conn.hkeys(hkey))
			samples = sorted(samples)
			remove = bisect.bisect_right(samples, cutoff)
			# 按需移除技术样本
			if remove:
				conn.hdel(hkey, *samples[:remove])
				# 这个散列可能已经被清空
				if remove == len(samples):
					try:
						# 在尝试修改计数器三列之前, 对其进行监视.
						pipe.watch(hkey)
						# 验证计数器散列是否为空, 如果是的话, 那么从记录
						# 一直计数器的有序集合里面移除它. 计数器散列并不
						# 为空, 继续让他留在记录一直计数器的有序集合里面
						if not pipe.hlen(hkey):
							pipe.multi()
							pipe.zrem('known:', hash)
							pipe.execute()
							# 在删除一个计数器的情况下, 下次循环可以使用
							# 与本次循环相同的索引
							index -= 1
						else:
							pipe.unwatch()
					except redis.exceptions.WatchError:
						pass

		# 为了让清理操作的执行频率与计数器更新的频率保持一致, 
		# 对记录循环次数的变量以及记录执行时长的变量进行更新
		passes += 1
		duration = min(int(time.time - start) + 1, 60)
		# 如果这次循环未耗尽60秒, 那么在余下的时间内进行休眠;
		# 如果60秒已经耗尽,那么休眠一秒以便稍作休息
		time.sleep(max(60 - duration, 1))



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

	def test_counters(self):
		import pprint
		global QUIT, SAMPLE_COUNT
		conn = self.conn

		print("Let's update some counters for now and a little in the future")
		now = time.time()
		for delta in range(10):
			update_counter(conn, 'test', count=random.randrange(1, 5), now=now+delta)
		counter = get_counter(conn, 'test', 1)
		print("We have some per-second counters:", len(counter))
		self.assertTrue(len(counter) >= 10)
		counter = get_counter(conn, 'test', 5)
		print("We have some per-5-seconde counter:", len(counter))
		print("These counters include:")
		self.assertTrue(len(counter) >= 2)
		print()

		tt = time.time

		def new_tt():
			return tt() + 2 * 86400	
		time.time = new_tt()

		print("Let's clean out some counters by setting our sample count to 0")
		SAMPLE_COUNT = 0
		t = threading.Thread(target=clean_counters, args=(conn, ))
		t.setDaemon(1)
		t.start()
		time.sleep(1)
		QUIT = True
		time.time = tt
		counter =  get_counter(conn, 'test', 86400)
		print("Did we clean out all of the counters? ", not counter)


if __name__ == '__main__':
	unittest.main()

