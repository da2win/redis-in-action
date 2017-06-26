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
