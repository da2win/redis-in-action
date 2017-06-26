import json
import threading
import time
import unittest
from urllib import parse
import uuid


# 尝试获取并返回令牌对应的用户
def check_token(conn, token):
	return conn.hget('login:', token)

# 更新令牌
def update_token(conn, token, user, item = None):
	# 获取当前的时间戳
	timestamp = time.time()
	# 维持令牌与已登录用户之间的映射
	conn.hset('login:', token, user)
	# 记录令牌最后一次出现的时间
	conn.zadd('recent:', token, timestamp)
	if item:
		# 记录用户浏览过的商品
		conn.zadd('viewed:' + token, item, timestamp)
		# 移除旧的记录, 只保持用户最近浏览过的25个商品
		conn.zremrangebyrank('viewed:' + token, 0, -26)
		conn.zincrby('viewed:', item, -1)

# 清理旧的会话
QUIT = False
LIMIT = 10000000
def clean_sessions(conn):
	while not QUIT:
		# 找出目前已有令牌的数量
		size = conn.zcard('recent:')
		# 令牌数量未超过限制, 休眠并在之后重新检查
		if size <= LIMIT:
			time.sleep(1)
			continue

		end_index = min(size - LIMIT, 100)
		# 获取需要移除的令牌ID
		tokens = conn.zrange('recent:', 0, end_index-1)

		session_keys = []
		# 为那些将要被删除的令牌构建键名
		for token in tokens:
			session_keys.append('viewed:' + token)

		conn.delete(*session_keys)
		# 移除最旧的键名
		conn.hdel('login:', *tokens)
		conn.zrem('recent:', *tokens)

# 更新购物车
def add_to_cart(conn, session, item, count):
	if count <= 0:
		# 从购物车里面移除指定的商品
		conn.hrem('cart:' + session, item)
	else:
		# 将指定的商品添加购物车中
		conn.hset('cart:' + session, item, count)

def clean_full_sessions(conn):
	while not QUIT:
		size = conn.zcard('recent')
		if size <= LIMIT:
			time.sleep(1)
			continue
		end_index = min(size - LIMIT, 100)
		sessions = conn.zrange('recent:', 0, end_index - 1)

		session_keys = []
		for sess in sessions:
			session_keys.append('viewed:' + sess)
			# 用于删除旧的会话对应用户的购物车
			session_keys.append('cart:' + sess)
		conn.delete(*session_keys)
		conn.hdel('login:', *sessions)
		conn.zrem('recent:', *sessions)

def cache_request(conn, request, callback):
	# 对于不能被缓存的请求, 直接调用回调函数
	if not can_cache(conn, request):
		return callback(request)

	#将请求转换成一个简单的字符串键,方便之后进行查找
	page_key = 'cache:' + hash_request(request)
	# 尝试查找被缓存的页面
	content = conn.get(page_key)

	if not content:
		# 如果页面还没有被缓存, 那么生成页面
		content = callback(request)
		# 将新生成的页面放到缓存里面
		conn.setex(page_key, content, 300)

	return content #返回页面

def schedule_row_cache(conn, row_id, delay):
	# 先设置数据行的延迟值
	conn.zadd('delay:', row_id, delay)
	# 立即对需要缓存的数据行进行调度
	conn.zadd('schedule:', row_id, time.time())

def cache_rows(conn):
	while not QUIT:
		next = conn.zrange('schedule:', 0, 0, withscores=True)
		now = time.time()

		# 尝试获取下一个需要被缓存的数据行以及该行的调度时间戳,
		# 命令会返回一个包含零个或一个元组(tuple)的列表
		if not next or next[0][1]:
			# 暂时没有行需要被缓存, 休息50毫秒后重试
			time.sleep(.05)
			continue

		# 提前获取下一次调度的延迟时间
		delay = conn.zscore('delay:', row_id)
		if delay <= 0:
			# 不必再缓存这个行, 将它从缓存中移除
			conn.zrem('delay:', row_id)
			conn.zrem('schedule:', row_id)
			conn.delete('inv:' + row_id)
			continue

		# 读取数据行
		row = Inventory.get(row_id)
		conn.zadd('schedule:', row_id, now + delay)
		# 更新调度时间并设置缓存值
		conn.set('inv:' + row_id, json.dumps(row.to_dict()))

def rescale_viewed(conn):
	while not QUIT:
		# 删除所有排名在 20 000 名之后的商品.
		conn.zremrangebyrank('viewed:', 0, -20001)
		# 将浏览次数降低为原来的一半
		conn.zinterstore('viewed:', {'viewed:': .5})
		# 5分钟以后再次进行操作
		time.sleep(300)

def can_cache(conn, request):
	# 尝试从页面里面取出商品ID
	item_id = extract_item_id(request)
	# 检查这个页面能否被缓存一级这个页面是否为商品页面
	if not item_id or is_dynamic(request):
		return False
	# 取得商品的浏览次数排名
	rank = conn.zrank('viewed:', item_id)
	# 根据商品的浏览次数排名来判断是否需要缓存这个页面
	return rank is not None and rank < 10000

#--------------- 以下是用于测试代码的辅助函数 --------------------------------

def extract_item_id(request):
	parsed = parse.urlparse(request)
	query = parse.parse_qs(parsed.query)
	return (query.get('item') or [None])[0]

def is_dynamic(request):
	parsed = parse.urlparse(request)
	query = parse.parse_qs(parsed.query)
	return '_' in query

def hash_request(request):
	return str(hash(request))

class Inventory(object):
	def __init__(self, id):
		self.id = id

	@classmethod
	def get(cls, id):
		return Inventory(id)

	def to_dict(self):
		return {'id': self.id, 'data': 'data to cache...', 'cached': time.time()}


class TestCh02(unittest.TestCase):
	def setUp(self):
		import redis
		self.conn = redis.Redis(db=15)

	def tearDown(self):
		conn = self.conn
		to_del = (
			conn.keys("login:*") + conn.keys("recent:*") + conn.keys('viewed:*') +
			conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') +
			conn.keys('schedule:*') + conn.keys('inv:*'))
		if to_del:
			self.conn.delete(*to_del)
		del self.conn
		global QUIT, LIMIT
		QUIT = False
		LIMIT = 10000000
		print()
		print()

	def test_login_cookies(self):
		conn = self.conn
		global LIMIT, QUIT
		token = str(uuid.uuid4())

		update_token(conn, token, 'username', 'itemX')
		print("We just logged-in/updated token:")
		print("For user:", 'username')
		print()

		print("What username do we get when we look-up that token?")
		r = check_token(conn, token)
		print(r)
		print()
		self.assertTrue(r)

		print("Let's drop the maximum number of cookies to 0 to clean them out")
		print("We will start a thread to do the cleaning, while we stop it later")

		LIMIT = 0
		t = threading.Thread(target=clean_sessions, args=(conn,))
		t.setDaemon(1)
		t.start()
		time.sleep(1)
		QUIT = True
		time.sleep(2)

		if t.isAlive():
			raise Exception("The clean sessions thread is still alive?!?")

		s = conn.hlen('login:')
		print("The current number of sessions still available is:")
		self.assertFalse(s)

	def test_shopping_cart_cookies(self):
		conn = self.conn
		global LIMIT, QUIT
		token = str(uuid.uuid4())

		print("We'll refresh our session...")
		update_token(conn, token, 'username', 'itemX')
		print("And add an item to the shopping cart")
		add_to_cart(conn, token, "itemY", 3)
		r = conn.hgetall('cart:' + token)
		print("Our shopping cart currently has:", r)
		print()

		self.assertTrue(len(r) >= 1)

		print("Let's clean out our sessions and carts")
		LIMIT = 0
		t = threading.Thread(target=clean_full_sessions, args=(conn,))
		t.setDaemon(1)
		t.start()
		time.sleep(1)
		QUIT = True
		time.sleep(2)
		if t.isAlive():
			raise Exception("The clean sessions thread is still alive?!?")

		r = conn.hgetall('cart:' + token)
		print("Our shopping cart now contains:", r)

		self.assertFalse(r)

	def test_cache_request(self):
		conn = self.conn
		token = str(uuid.uuid4())

		def callback(request):
			return "content for " + request

		update_token(conn, token, 'username', 'itemX')
		url = 'http://test.com/?item=itemX'
		print("We are going to cache a simple request against", url)
		result = cache_request(conn, url, callback)
		print("We got initial content:", repr(result))
		print()

		self.assertTrue(result)

		print("To test that we've cached the request, we'll pass a bad callback")
		result2 = cache_request(conn, url, None)
		print("We ended up getting the same response!", repr(result2))

		self.assertTrue(result, result2)

		self.assertFalse(can_cache(conn, "http://test.com/"))
		self.assertFalse(can_cache(conn, 'http://test.com/?item=itemX&_=1234536'))

	def test_cache_row(self):
		import pprint
		conn = self.conn
		global QUIT

		print("First, let's schedule caching of itemX every 5 seconds")
		schedule_row_cache(conn, 'itemX', 5)
		print("Our schedule looks like:")
		s = conn.zrange('schedule:', 0, -1, withscores=True)
		pprint.pprint(s)
		self.assertTrue(s)

		print("We'll start a caching thread that will cache the data...")
		t = threading.Thread(target=cache_rows, args=(conn,))
		t.setDaemon(1)
		t.start()

		time.sleep(1)
		print("Our cached data looks like:")
		r = conn.get('inv:itemX')
		print(repr(r))
		self.assertTrue(r)
		print()
		print("We'll check again in 5 seconds...")
		time.sleep(5)
		print("Notice that the data has changed...")
		r2 = conn.get('inv:itemX')
		print(repr(r2))
		print()
		self.assertTrue(r2)
		self.assertTrue(r != r2)

		print("Let's force un-caching")

		schedule_row_cache(conn, 'itemX', -1)
		time.sleep(1)
		r = conn.get('inv:itemX')
		print("The cache was cleared?", not r)
		print()
		self.assertFalse(r)

		QUIT = True
		time.sleep(2)
		if t.isAlive():
			raise Exception("The database caching thread is still alive?!?")

if __name__ == '__main__':
    unittest.main()