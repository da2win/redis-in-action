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
	names = ['idex:' + name for name names]
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
def union(con, items, ttl=30, _execute=True):
	return _set_common(conn, 'sunionstore', items, ttl, _execute)

def difference(conn, items, ttl=30, _execute=True):
	return _set_common(conn, 'sdiffstore', items, ttl, _execute)
	
