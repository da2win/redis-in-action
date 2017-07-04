import redis
import bisect
import uuid

def add_update_contact(conn, user, contact):
	ac_list = 'recent:' + user
	# 准备执行原子操作
	pipeline = conn.pipeline(True)
	# 如果联系人已经存在， 那么移除它
	pipeline.lrem(ac_list, conatct)
	# 将联系人推入列表的最前端
	pipeline.lpush(ac_list, contact)
	# 只保留列表里面的前100个联系人
	pipeline.ltrim(ac_list, 0, 99)
	# 实际地执行以上操作
	pipeline.execute()

def remove_contact(conn, user, contact):
	conn.lrem('recent:' + user, contact)

def fetch_autocomplete_list(conn, user, prefix):
	# 获取自动补全列表
	candidates = conn.lrange('recent:' + user, 0, -1)
	matchs = []
	# 检查每个候选联系人
	for candidate in candidates:
		if candidate.lower().startswith(prefix):
			# 发现一个匹配的联系人
			matchs.append(candidate)
	# 返回所有匹配的联系人
	return matchs

# 准备一个由已知字符组成的列表。
valid_characters = '`abcdefghijklmnopqrstuvwxyz{'  

def find_prefix_range(prefix):
	# 在字符列表中查找前缀字符所处的位置。
	posn = bisect.bisect_left(valid_characters, prefix[-1:])
	# 找到前驱字符
	suffix = valid_characters[(posn or 1) - 1]
	# 返回范围
	return prefix[:-1] + suffix + '{', prefix + '{'

def autocomplete_on_prefix(conn, guild, prefix):
	# 根据给定的前缀计算出查找范围的起点和终点
	start, end = find_prefix_range(prefix)
	identifier = str(uuid.uuid4())
	start += identifier
	end += identifier
	zset_name = 'memebers:' + guild

	# 将范围的起始元素和结束元素添加到有序集合里面。
	conn.zadd(zset_name, start, 0, end, 0)
	pipeline = conn.pipeline(True)
	while 1:
		try:
			pipeline.watch(zset_name)
			# 找到两个被插入元素在有序集合中的排名
			sindex = pipeline.zrank(zset_name, start)
			eindex = pipeline.zrank(zset_name, end)
			erange = min(sindex + 9, eindex - 2)
			pipeline.multi()

			# 获取范围内的值， 然后删除之前插入的起始元素和结束元素
			pipeline.zrem(zset_name, start, end)
			pipeline.zrange(zset_name, sindex, erange)
			items = pipeline.execute()[-1]
		except redis.exceptions.WatchError:
			# 如果自动补全有序集合已经被其他客户端修改过了，那么重试。
			continue
	# 如果有其他自动补全操作正在执行， 那么从获取到的元素里
	# 面移除起始元素和结束元素
	return [item for item in items if '{' not in item]
