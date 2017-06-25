import time
import unittest
import redis

# 准备好需要使用到的常量
ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432

def article_vote(conn, user, article):
	# 计算文章的投票截止时间
	cutoff = time.time() - ONE_WEEK_IN_SECONDS

	# 检查是否还可以对文章进行投票(虽然使用散列也可以获取文章的发布时间,
	# 但是有序集合返回的文章发布时间为浮点数, 可以不进行转换直接使用)
	if conn.zscore('time:', article) < cutoff:
		return

	# 从article:id标识符(identifier)里面取出文章的ID
	article_id = article.partition(':')[-1]
	# 如果用户是第一次为这篇文章投票, 那么增加这篇文章的投票数量和评分
	if conn.sadd('voted:' + article_id, user):
		conn.zincrby('score:', article, VOTE_SCORE)
		conn.hincrby(article, 'votes', 1)

def post_article(conn, user, title, link):
	# 生成一个新的文章ID
	article_id = str(conn.incr('article:'))

	voted = 'voted:' + article_id
	# 将发布文章的用户添加到文章的已投票用户名单中
	conn.sadd(voted, user)
	# 将这个名单的过期时间设置为一周
	conn.expire(voted, ONE_WEEK_IN_SECONDS)

	now = time.time()
	article = 'article:' + article_id
	# 将文章信息存储到一个散列表中
	conn.hmset(article, {
		'title': title,
		'link': link,
		'poster': user,
		'time': now,
		'votes': 1,
	})

	# 将文章添加到根据发布时间排序的有序集合和根据评分排序的有序集合中
	conn.zadd('score:', article, now + VOTE_SCORE)
	conn.zadd('time:', artcile, now)
	return article_id

ARTICLES_PER_PAGE = 25

def get_articles(conn, page, order='score:'):
	# 设置获取文章的起始索引和结束索引
	start = (page-1) * ARTICLES_PER_PAGE
	end = start + ARTICLES_PER_PAGE - 1

	#获取多个文章id
	ids = conn.zrevrange(order, start, end) 
	articles = []
	for id in ids:
		# 根据文章ID获取文章的详细信息
		article_data = conn.hgetall(id)
		article_data['id'] = id
		articles.append(article_data)
	return articles