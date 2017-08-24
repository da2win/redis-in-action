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

def cpc_to_ecpm(views, clicks, cpc):
	return 1000. * cpc * clicks / views

# 因为点击通过率是由点击次数除以展示次数计算出来的, 
# 而动作的执行概率则由动作执行次数除以点击次数计算出来的,
# 所以这两个概率相乘的结果等于动作执行次数除以展示次数
def cpa_to_ecpm(views, actions, cpa):
	return 1000. * cpa * actions

TO_ECPM = {
	'cpc': cpc_to_ecpm,
	'cpa': cpa_to_ecpm,
	'cpm': lambda *args:args[-1]
}

def index_ad(conn, id, locations, content, type, value):
	# 设置流水线, 使得程序可以在一次通信往返里面完成整个索引操作
	pipeline = conn.pipeline(True)

	# 为了进行定向操作, 把广告ID添加到所有相关的位置集合里面. 
	for location in locations:
		pipeline.sadd('idx:req:' + location, id)

	words = tokenize(content)
	# 对广告包含的单词进行索引
	for word in tokenize(content):
		pipeline.zadd('idx:' + word, id, 0)

	# 为了评估新广告的效果, 程序会使用字典来存储广告
	# 每1000次展示的平均点击次数或平均动作磁性次数
	rvalue = TO_ECPM[type](
		1000, AVERAGE_PER_1k.get(type, 1), value)
	# 记录这个广告的类型
	pipeline.hset('type:', id, type)
	# 将广告的eCPM添加到一个记录了所有广告的eCPM的有序集合里面
	pipeline.zadd('idx:ad:value:', id, rvalue)
	# 将广告的基本价格(base value)添加到一个记录了所有过高的基本价格的有序集合里面.
	pipeline.zadd('ad:base_value:', id, value)
	# 把能够对广告进行定向的单词全部记录起来. 
	pipeline.sadd('terms:' + id, *list(words))
	pipeline.execute()
