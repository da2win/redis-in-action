
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

LAST_CHECKED = 0
IS_UNDER_MAINTENANCE = False

def is_under_maintenance(conn):
	# 将两个变量设置为全局变量以便之后对他们进行写入
	global LAST_CHECKED, IS_UNDER_MAINTENANCE

	# 距离上次检查是否已经超过一秒
	if LAST_CHECKED < time.time() - 1:
		# 更新最后检查时间
		LAST_CHECKED = time.time()

		# 检查系统是否正在进行维护
		IS_UNDER_MAINTENANCE = bool(
			conn.get('is-under-maintenance'))
	# 返回一个布尔值， 用于表示系统是否正在进行维护。
	return IS_UNDER_MAINTENANCE

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

	def test_is_under_maintenance(self):
		print("Are we under maintenance (we shouldn't be)?", is_under_maintenance(self.conn))
		self.conn.set('is-under-maintenance', 'yes')
		print("We cached this, so it should be the same:", is_under_maintenance(self.conn))
		time.sleep(1)
		print("But after a sleep, it should change:", is_under_maintenance(self.conn))
		print("Cleaning up...")
		self.conn.delete('is-under-maintenance')
		time.sleep(1)
		print("Should be False again:", is_under_maintenance(self.conn))

if __name__ == '__main__':
    unittest.main()