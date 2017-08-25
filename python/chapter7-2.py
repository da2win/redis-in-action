import math
import re
import unittest
import uuid

import redis

AVERAGE_PER_1K = {}

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

def cpc_to_ecpm(views, clicks, cpc):
	return 1000. * cpc * clicks / views

def _zset_common(conn, method, scores, ttl=30, **kw):
	id = str(uuid.uuid4())
	# 调用者可以通过传递参数来界定是否使用事务流水线
	execute = kw.pop('_execute', True)
	pipeline = conn.pipeline(True) if execute else conn
	
	for key in scores.keys():
		scores['idx:' + key] = scores.pop(key)
	# 为将要执行的操作设置好相应的参数. 
	getattr(pipeline, method) ('idx:' + id, scores, **kw)
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

def union(conn, items, ttl=30, _execute=True):
	return _set_common(conn, 'sunionstore', items, ttl, _execute)

#######################################################################
#######################################################################
#######################################################################

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
		1000, AVERAGE_PER_1K.get(type, 1), value)
	# 记录这个广告的类型
	pipeline.hset('type:', id, type)
	# 将广告的eCPM添加到一个记录了所有广告的eCPM的有序集合里面
	pipeline.zadd('idx:ad:value:', id, rvalue)
	# 将广告的基本价格(base value)添加到一个记录了所有过高的基本价格的有序集合里面.
	pipeline.zadd('ad:base_value:', id, value)
	# 把能够对广告进行定向的单词全部记录起来. 
	pipeline.sadd('terms:' + id, *list(words))
	pipeline.execute()

def target_ads(conn, locations, content):
	pipeline = conn.pipeline(True)
	# 根据用户传入的位置定向参数, 找到所有匹配该位置的广告, 以及这些广告eCPM
	macthed_ads, base_ecpm = match_location(pipeline, locations)
	# 基于匹配的内容计算附加值
	words, targeted_ads = finish_scoring(
		pipeline, macthed_ads, base_ecpm, content)

	pipeline.incr('ads:served:')
	# 找到ecpm最高的广告, 并获取这个广告的ID
	pipeline.zrevrange('idx:' + targeted_ads, 0, 0)
	target_id, targeted_ad = pipeline.execute()[-2:]

	# 如果没有任何广告与目标位置相匹配, 那么返回空值
	if not targeted_ad:
		return None, None

	_ad_id = targeted_ad[0]
	ad_id = _ad_id.decode('utf-8')
	# 记录一系列定向操作的执行结果, 作为学习用户行为的其中一个步骤
	record_targeting_result(conn, target_id, ad_id, words)

	# 向调用者返回记录本次定向操作相关信息的ID, 以及被选中的广告的ID
	return target_id, ad_id


# 基于位置执行广告定向操作的辅助函数
def match_location(pipe, locations):
	# 根据所有给定的位置, 找出需要执行并集操作的集合键
	required = ['req:' + loc for loc in locations]
	# 找出与指定地区相匹配的广告, 并将它们存储到集合里面
	matched_ads = union(pipe, required, ttl=300, _execute=False)
	# 找到存储着所有被匹配广告的集合,
	# 以及存储着所有被匹配广告的基本eCPM的有序集合, 然后返回它们的ID
	return matched_ads, zintersect(pipe, 
		{matched_ads: 0, 'ad:value:': 1}, _execute=False)


# 计算 包含了内容匹配附加值的广告eCPM
def finish_scoring(pipe, matched, base, content):
	bonus_ecpm = {}
	words = tokenize(content)
	for word in words:
		# 找出那些既位于定向位置之内, 又拥有页面内容其中一个单词的广告
		word_bonus = zintersect(
			pipe, {matched: 0, word: 1}, _execute=False)
		bonus_ecpm[word_bonus] = 1

	if bonus_ecpm:
		# 计算每个广告的最小eCPM附加值和最大eCPM, 附加值. 
		minimum = zunion(
			pipe, bonus_ecpm, aggregate='MIN', _execute=False)
		maximum = zunion(
			pipe, bonus_ecpm, aggregate='MAX', _execute=False)
		# 将广告的基本价格, 最小eCPM附加值的一半以及最大eCPM附加值的一半这三者相加起来
		return words, zunion(
			pipe, {base:1, minimum:.5, maximum:.5}, _execute=False)
	# 如果页面内容中没有出现任何可匹配的单词,  那么返回广告的基本eCPM
	return words, base

# 负责在广告定向操作执行完毕之后记录执行结果的函数
def record_targeting_result(conn, target_id, ad_id, words):
	pipeline = conn.pipeline(True)

	# 找出内容与广告之间相匹配的那些单词
	terms = conn.smembers('terms:' + ad_id)
	matched = list(words & terms)

	if matched:
		matched_key = 'terms:matched:%s' % target_id
		# 如果有相匹配的单词出现,就记录它们,并设置15分钟的生存时间
		pipeline.sadd(matched_key, *matched)

	# 为每种类型的广告分别记录它们的展示次数
	_type = conn.hget('type:', ad_id)
	type = _type.decode('utf-8')
	pipeline.incr('type:%s:views:' % type)

	# 记录广告以及广告包含的单词的展示信息
	for word in matched:
		pipeline.zincrby('views:%s' % ad_id, word)
	pipeline.zincrby('views:%s' % ad_id, '')

	# 广告每展示1--次就更新一次它的eCPM.
	if not pipeline.execute()[-1] % 100:
		update_cpms(conn, ad_id)


# 记录广告被点击信息的函数
def record_click(conn, target_id, ad_id, action=False):
	pipeline = conn.pipeline(True)
	click_key = 'clicks:%s' % ad_id

	match_key = 'terms:matched:%s' % target_id

	type = conn.hget('type:', ad_id)
	if type == 'cpa':
		# 如果这是一个按动作计费的广告, 并且被匹配的单词仍然存在,
		# 那么刷新这些单词的过期时间
		pipeline.expire(match_key, 900)
		if action:
			# 记录动作信息, 而不是点击信息
			click_key = 'actions:%s' % ad_id

	# 根据广告的类型, 维持一个全局的点击/动作
	if action and type == 'cpa':
		pipeline.incr('type:%s:actions:' % type)
	else:
		pipeline.incr('type:%s:clicks:' % type)

	# 为广告以及所有被定向至该广告的单词记录本次点击(或动作)
	matched = list(conn.smembers(match_key))
	matched.append('')
	for word in matched:
		pipeline.zincrby(click_key, word)
	pipeline.execute()

	# 对广告中出现的所有单词的eCPM进行更新
	update_cpms(conn, ad_id)


# 负责对广告eCPM以及每个单词的eCPM附加值进行更新的函数
def update_cpms(conn, ad_id):
	print("This is ad_id:", ad_id)
	pipeline = conn.pipeline(True)

	# 获取广告的类型和价格, 以及广告包含的所有单词
	pipeline.hget('type:', ad_id)
	pipeline.zscore('ad:base_value:', ad_id)
	pipeline.smembers('terms:' + ad_id)
	_type, base_value, words = pipeline.execute()

	type = _type.decode('utf-8')
	# 判断广告的eCPM应该基于点击次数进行计算还是基于动作执行次数进行计算
	which = 'clicks'
	if type == 'cpa':
		which = 'actions'

	# 根据给定广告的类型, 获取广告的展示次数和点击次数(或者动作执行次数)
	pipeline.get('type:%s:views:' % type)
	pipeline.get('type:%s:%s' % (type, which))
	type_views, type_clicks = pipeline.execute()
	# 将广告的点击率或动作执行率重新写入全局字典里面
	AVERAGE_PER_1K[type] = (
		1000. * int(type_clicks or '1') / int(type_views or '1'))

	# 如果正在处理的一个是CPM广告, 那么它的eCPM已经更新完毕, 无需再做其他处理
	if type == 'cpm':
		return

	view_key = 'views:%s' % ad_id
	click_key = '%s:%s' % (which, ad_id)

	to_ecpm = TO_ECPM[type]

	# 获取每个广告的展示次数和点击次数(或者动作执行次数)
	pipeline.zscore(view_key, '')
	pipeline.zscore(click_key, '')
	ad_views, ad_clicks = pipeline.execute()
	# 如果广告还没有被点击过, 那么使用已有eCPM
	if (ad_clicks or 0) < 1:
		ad_ecpm = conn.zscore('idx:ad:value', ad_id)
	else:
		# 计算广告的eCPM并更新它的价格
		ad_ecpm = to_ecpm(ad_views or 1, ad_clicks or 0, base_value)
		pipeline.zadd('idx:ad:value:', ad_id, ad_ecpm)

	for word in words:
		# 获取单词的展示次数和点击次数(或者动作执行次数)
		pipeline.zscore(view_key, word)
		pipeline.zscore(click_key, word)
		views, clicks = pipeline.execute()[-2:]

		# 如果广告还未被点击过, 那么不对eCPM进行更新
		if (clicks or 0) < 1:
			continue

		# 计算单词的eCPM
		word_ecpm = to_ecpm(views or 1, clicks or 0, base_value)
		# 计算单词的附加值
		bonus = word_ecpm - ad_ecpm
		# 将单词的附加值重新写入为广告包含的每个单词分别记录附加值的有序集合里面
		pipeline.zadd('idx:' + word, ad_id, bonus)
		pipeline.zadd('idx:' + word, ad_id, bonus)
	pipeline.execute()

class TestCh07(unittest.TestCase):
    content = 'this is some random content, look at how it is indexed.'
    def setUp(self):
        self.conn = redis.Redis(db=15, password='payexpress.redis')
        self.conn.flushdb()
    def tearDown(self):
        self.conn.flushdb()

    def test_index_and_target_ads(self):
        index_ad(self.conn, '1', ['USA', 'CA'], self.content, 'cpc', .25)
        index_ad(self.conn, '2', ['USA', 'VA'], self.content + ' wooooo', 'cpc', .125)

        for i in range(100):
            ro = target_ads(self.conn, ['USA'], self.content)
        # self.assertEquals(ro[1], '1')

        r = target_ads(self.conn, ['VA'], 'wooooo')
        # self.assertEquals(r[1], '2')

        self.assertEquals(self.conn.zrange('idx:ad:value:', 0, -1, withscores=True), [(b'2', 0.125), (b'1', 0.25)])
        self.assertEquals(self.conn.zrange('ad:base_value:', 0, -1, withscores=True), [(b'2', 0.125), (b'1', 0.25)])

        record_click(self.conn, ro[0], ro[1])

        #self.assertEquals(self.conn.zrange('idx:ad:value:', 0, -1, withscores=True), [(b'2', 0.125), (b'1', 2.5)])
        self.assertEquals(self.conn.zrange(b'ad:base_value:', 0, -1, withscores=True), [(b'2', 0.125), (b'1', 0.25)])

if __name__ == '__main__':
    unittest.main()