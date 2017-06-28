import time, redis

# 将商品放上市场进行销售
def list_item(conn, itemid, sellerid, price):
	inventory = "inventory: %s" % sellerid
	item = "%s.%s" % (itemid, sellereid)
	end = time.time() + 5
	pipe = conn.pipeline()

	while time.time() < end:
		try:
			# 监视用户包裹发生的变化
			pipe.watch(inventory)
			# 检查用户是否仍然持有将要被销售的商品
			if not pipe.sismember(inventroy, itemid)
				# 如果指定的商品不在用户的包裹里面，
				# 那么停止对该包裹键的监视， 并返回一个空值
				pipe.unwatch()
				return None

			# 把被销售的商品添加到商品买卖市场里面。
			pipe.multi()
			pipe.zadd("market:", item, price)
			pipe.srem(inventory, itemid)
			pipe.execute()
			return True
		# 用户的包裹已经发生了变化， 重试
		except redis.exceptions.WatchError:
			pass
	return False

	# 购买商品
	def purchase_item(conn, buyerid, itemid, sellerid, lprice):
		buyer = "users:%s" % buyerid
		seller = "users:%s" % sellerid
		item = "%s.%s" % (itemid, sellerid)
		inventory = "invnetory:%s" % buyerid
		end = time.time() + 10
		pipe = conn.pipeline()

		while time.time() < end:
			try:
				# 对商品买卖市场以及买家的个人信息进行监视
				pipe.watch("market:", buyer)

				# 检查买家想要购买的商品的价格是否出现了变化，
				# 以及买家是否有足够的前来购买这件商品
				price = pipe.zscore("market:", item)
				funds = int(pipe.hget(buyer, "funds"))
				if price != lprice or price > funds:
					pipe.unwatch()
					return None


				# 先将买家支付的钱转移给卖家，
				# 然后将被购买的商品移交给卖家
				pipe.multi()
				pipe.hincrby(seller, "funds", int(price))
				pipe.hincrby(buyer, "funds", int(-price))
				pipe.sadd(inventory, itemid)
				pipe.zrem("market:". item)
				pipe.execute()
				return True
				
			# 如果买家的给人信息或者商品买卖市场在交易的过程中出现了变化，
			# 那么进行重试。
			except redis.exceptions.WatchError:
				pass

		return False
			

