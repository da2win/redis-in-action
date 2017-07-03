
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

def ip_to_score(ip_address):
	score = 0
	for v in ip_address.split('.'):
		score = score * 256 + int(v, 10)
	return score

# 这个函数在执行时需要输入GeoLiteCity-Blocks.csv文件所在的路径
def import_ips_to_redis(conn, filename):
	csv_file = csv.reader(open(filename, 'r'))
	for count, row in enumerate(csv_file):
		# 按需将IP地址转换为分值
		start_ip = row[0] if row else ''
		if 'i' in start_ip.lower():
			continue
		if '.' in start_ip:
			start_ip = ip_to_score(start_ip)
		elif start_ip.isdigit():
			start_ip = int(start_ip, 10)
		else:
			# 略过文件的第一行以及各式不正确的条目
			continue

		# 构建唯一城市ID
		city_id = row[2] + '_' + str(count)
		conn.zadd('ip2cityid:', city_id, start_ip)

# 这个函数在执行时需要输入GeoLiteCIty-location.csv文件所在的路径
def import_cities_to_redis(conn, filename):
	for row in csv.reader(open(filename, 'r')):
		if len(row) < 4 or not row[0].isdigit():
			continue
		row = [i.decode('latin-1') for i in row]
		# 准备好需要添加到散列里面的信息
		city_id = row[0]
		country = row[1]
		region = row[2]
		city = row[3]
		# 将城市信息添加到 Redis 里面。
		conn.hset('cityid2city:', city_id, json.dumps([city, region, country]))

def find_city_by_ip(conn, ip_address):
	# 将ip地址转换为分值以便执行 ZREVRANGEBYSCORE命令
	if isinstance(ip_address, str):
		ip_address = ip_to_score(ip_address)

	# 查找唯一城市id
	city_id = conn.zrevrangebyscore(
		'ip2cityid:', ip_address, 0, start=0, num=1)

	if not city_id:
		return None

	# 将唯一城市ID转换为普通城市ID
	city_id = city_id[0].partition('_')[0]
	# 从散列里面取出城市信息
	return json.loads(conn.hget('cityid2city:', city_id))


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

	def test_ip_lookup(self):
		conn = self.conn

		try:
		    open('GeoLiteCity-Blocks.csv', 'r')
		    open('GeoLiteCity-Location.csv', 'r')
		except:
		    print("********")
		    print("You do not have the GeoLiteCity database available, aborting test")
		    print("Please have the following two files in the current path:")
		    print("GeoLiteCity-Blocks.csv")
		    print("GeoLiteCity-Location.csv")
		    print("********")
		    return

		print("Importing IP addresses to Redis... (this may take a while)")
		import_ips_to_redis(conn, 'GeoLiteCity-Blocks.csv')
		ranges = conn.zcard('ip2cityid:')
		print("Loaded ranges into Redis:", ranges)
		self.assertTrue(ranges > 1000)
		print()

		print("Importing Location lookups to Redis... (this may take a while)")
		import_cities_to_redis(conn, 'GeoLiteCity-Location.csv')
		cities = conn.hlen('cityid2city:')
		print("Loaded city lookups into Redis:", cities)
		self.assertTrue(cities > 1000)
		print()

		print("Let's lookup some locations!")
		rr = random.randrange
		for i in xrange(5):
		    print(find_city_by_ip(conn, '%s.%s.%s.%s'%(rr(1,255), rr(256), rr(256), rr(256))))

if __name__ == '__main__':
    unittest.main()