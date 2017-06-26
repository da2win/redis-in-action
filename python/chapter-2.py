import redis
import time

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
	if not can_cache(conn, request, callback):
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