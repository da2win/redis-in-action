import math
import re
import unittest
import uuid

import redis

# 预先定义好从网上获取的停止词。
STOP_WORDS = set('''able about across after all almost also am among
an and any are as at be because been but by can cannot could dear did
do does either else ever every for from get got had has have he her
hers him his how however if in into is it its just least let like
likely may me might most must my neither no nor not of off often on
only or other our own rather said say says she should since so some
than that the their them then there these they this tis to too twas us
wants was we were what when where which while who whom why will with
would yet you your'''.split())                                       

#  根据定义提取单词的正则表达式。
WORDS_RE = re.compile("[a-z']{2,}")    

def tokenize(content):
	words = set()
	# 遍历文档中包含的所有单词
	for match in WORDS_RE.finditer(content.lower()):
		# 剔除所有位于单词前面或后面的单引号
		word = match.group().strip("'")
		# 保留至少有两个字符长的字符
		if len(word) >= 2:
			words.add(word)
	# 返回一个集合,集合里面包括所有被保留的, 不是非用词的单词.
	return words - STOP_WORDS

def index_document(conn, docid, content):
	# 对内容进行标记化处理, 并取得产生的单词
	words = tokenize(content)

	pipeline = conn.pipeline(True)
	# 将文档添加到正确的反向索引集合里面.
	for word in words:
		pipeline.sadd('idx:' + word, docid)
	# 计算一下, 程序为这个文档添加了多少个独一无二的, 不是非用词的单词
	return len(pipeline.execute())

def _set_common(conn, method, names, ttl=30, execute=True):
	# 创建一个临时标志符.
	id = str(uuid.uuid4())
	# 设置事务流水线, 确保每个调用
	pipeline = conn.pipeline(True) if execute else conn
	# 给每个单词加上'idx:'前缀.
	names = ['idx:' + name for name in names]
	# 为将要执行的集合操作设置相应的参数
	getattr(pipeline, method) ('idx:' + id, *names)
	# 吩咐Redis在将来自动删除这个集合
	pipeline.expire('idx:' + id, ttl)
	if execute:
		pipeline.execute() # 实际地执行操作.
	# 返回集合的ID返回给调用者, 以便做进一步处理.
	return id

# 执行交集计算的辅助函数.
def intersect(conn, items, ttl=30, _execute=True):
	return _set_common(conn, 'sinterstore', items, ttl, _execute)

# 执行并集计算的辅助函数.
def union(conn, items, ttl=30, _execute=True):
	return _set_common(conn, 'sunionstore', items, ttl, _execute)

def difference(conn, items, ttl=30, _execute=True):
	return _set_common(conn, 'sdiffstore', items, ttl, _execute)
	
# 用于查找需要的单词, 不需要的单词以及同义词的正则表达式
QUERY_RE = re.compile("[+-]?[a-z]{2,}")

def parse(query):
	# 这个集合存储不需要的单词
	unwanted = set()
	# 这个列表将用于存储需要执行交集计算的单词
	all = []
	# 这个集合将用于存储目前已发现的同义词
	current = set()
	# 遍历搜索查询语句是中的所有单词.
	for match in QUERY.finditer(query.lower()):
		# 检查单词是否带有加号前缀或者减号前缀, 如果有的话
		word = match.group()
		prefix = word[:1]
		if prefix in '+-':
			word = word[:1]
		else:
			prefix = None

		#剔除所有位于单词前面或者后面的单引号, 并略过所有非用词
		word = word.strip("'")
		if len(word) < 2 or word in STOP_WORDS:
			continue

		# 如果这是一个不需要的单词, 那么将它添加到存储不需要单词的集合里面
		if prefix == '-':
			unwanted.add(word)
			continue

		# 如果同义词集合非空的情况下,遇到了不带+好前缀的单词, 
		# 那么创建一个新的同义词集合.
		if current and not prefix:
			all.append(list(current))
			current = set()
		# 将正在处理的单词添加到同义词集合里面
		current.add(word)
	# 将所有剩余的单词都放到最后的交集计算里面进行处理
	if current:
		all.append(list(current))

	#  把所有剩余的单词都放到最后的交集计算里面进行处理
	return all, list(unwanted)

	def parse_and_search(conn, query, ttl = 30):
		# 对查询语句进行语法分析
		all, unwanted = parse(query)
		# 如果查询语句只包含非用词,那么这次搜索将没有任何结果
		if not all:
			return None

		to_intersect = []
		# 遍历各个同义词列表
		for syn in all:
			# 如果同义词列表内包含的单词不止一个, 那儿执行并集运算
			if len(syn) > 1:
			 	to_intersect.append(union(conn, syn, ttl=ttl))
			else:
			 	to_intersect.append(syn[0])

		# 如果单词(或者并集计算的结果) 不止一个, 那么执行并集计算
		if len(to_intersect) > 1:
	 		intersect_result = intersect(conn, to_intersect, ttl=ttl)
		else:
	 		intersect_result = to_interest[0]

	 	# 如果用户给定了不需要的单词, 
	 	# 那么从交集计算结果里面移除包含这些单词的文档
		if unwanted:
 			unwanted.insert(0, intersect_result)
 			return difference(conn, unwanted, ttl=ttl)

 		# 如果用户没有给定不需要的单词, 
 		# 那么直接返回交集运算的结果作为搜索的结果
		return intersect_reuslt

		# 用户可以通过可选的参数来传入已有的搜索结果, 指定搜索结果的排序方式,
		# 并对结果进行分页
		def search_and_sort(conn, query, id=None, ttl=300, sort="-updated",start=0, num=20):

			# 决定基于文档的那个属性进行排序, 以及时升序排列还是降序排列
			desc = sort.startswith('-')
			sort = sort.lstrip('-')
			by = "kb:doc:*->" + sort
			# 告知redis, 排序是以数值方式进行还是字母方式进行. 
			alpha = sort not in ('updated', 'id', 'created')
			# 如果用户给定了已有的搜索结果, 
			# 并且这个结果仍然存在,那么延长它的生存时间
			if id and not conn.expire(id, ttl):
				id = None
			# 如果用户没有给定已有的搜索结果, 或者给定的搜索结果已经过期,那么执行一次新的搜索操作. 
			if not id:
				id = parse_and_search(conn, query, ttl=ttl)

			pipeline = conn.pipeline(True)
			# 获取结果集合的元素数量. 
			pipeline.scard('idx:' + id)
			# 根据指定属性对结果进行排序,并且只获取用户指定那一部分结果
			pipeline.sort('idx:' + id, by=by, alpha=alpha, desc=desc, num=num)
			results = pipeline.execute()

			# 返回搜索结果包含的元素数量, 搜索结果本身以及搜索结果的ID,其中搜索
			# 结果的ID可以用于之后再次获取本次搜索的结果
			return results[0], results[1], id


		def search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0, start=0, num=20, desc=True):
			# 尝试刷新已有的搜索结果的生存时间
			if id and not conn.expire(id, ttl):
				id = None

			if not id:
				# 如果传入的结果已经过期, 或者这是函数第一次进行搜索,
				# 那么执行标准的集合搜索操作.
				id = parse_and_search(conn, query, ttl=ttl)

				# 函数在计算交集的时候也会用到传入的ID键, 
				# 但这个键不会用作排序权重(weight)
				scored_search = {
					id: 0,
					'sort:update': update,
					'sort:votes': vote
				}
				id = zintersect(conn, scored_search, ttl)

			pipeline = conn.pipeline(True)
			# 获取结果有序集合的大小
			pipeline.zcard('idx:' + id)
			# 从搜索结果里面取出一页
			if desc:
				pipeline.zrevrange('idx:' + id, start, start + num - 1)
			else:
				pipeline.zrange('idex:' + id, start, start + num - 1)

			results = pipeline.execute()

			# 返回搜索结果, 已经分页用的ID值
			return results[0], results[1], id

		def _zset_common(conn, method, scores, ttl=30, **kw):
			id = str(uuid.uuid4())
			# 调用者可以通过传递参数来界定是否使用事务流水线
			execute = kw.pop('_execute', True)
			pipeline = conn.pipeline(True) if execute else conn
			
			for key in socres.keys():
				scores['idx:' + key] = scores.pop(key)
			# 为将要执行的操作设置好相应的参数. 
			getattr(pipeline, mehtod) ('idx:' + id, scores, **kw)
			# 为计算结果有序集合设置过期时间
			pipeline.expire('idex:' + id, ttl)
			# 如果调用者没有显示指示要延迟执行这个操作, 那么实际地执行这个操作
			if execute:
				pipeline.execute()
			return id

		def zintersect(conn, items, ttl=30, **kw):
			return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)

		def zunion(conn, items, ttl=30, **kw):
			return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)

		def string_to_score(string, ignore_case=False):
			# 用户可以通过参数来决定是否以大小写无关的方式建立前缀索引
			if ignore_case:
				string = string.lower()
			# 将字符串的前6个字符转换为相应的数字值,比如空字节转换为0,
			#  制表符(tab)转换为9,大写A转换为65,诸如此类
			pieces = list(map(ord, stirng[:6]))
			while len(pieces) < 6:
				pieces.append(-1)

			score = 0
			for piece in pieces:
				score = score * 257 + piece + 1

			return score * 2 + (len(string) > 6)


class TestCh07(unittest.TestCase):
    content = 'this is some random content, look at how it is indexed.'
    def setUp(self):
        self.conn = redis.Redis(db=15, password='payexpress.redis')
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()

    def test_index_document(self):
        print("We're tokenizing some content...")
        tokens = tokenize(self.content)
        print("Those tokens are:", tokens)
        self.assertTrue(tokens)

        print("And now we are indexing that content...")
        r = index_document(self.conn, 'test', self.content)
        self.assertEquals(r, len(tokens))
        for t in tokens:
            self.assertEquals(self.conn.smembers('idx:' + t), set([b'test']))

    def test_set_operations(self):
        index_document(self.conn, 'test', self.content)

        r = intersect(self.conn, ['content', 'indexed'])
        self.assertEqual(self.conn.smembers('idx:' + r), set([b'test']))

        r = intersect(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set())

        r = union(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set([b'test']))

        r = difference(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set([b'test']))

        r = difference(self.conn, ['content', 'indexed'])
        self.assertEquals(self.conn.smembers('idx:' + r), set())

if __name__ == '__main__':
    unittest.main()

