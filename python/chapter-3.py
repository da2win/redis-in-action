import time, threading, redis

conn = redis.Redis(db=15)
def publisher(n):
	# 函数在刚开始执行时会先休眠, 让订阅者有足够的时间来连接服务器并监听消息
	time.sleep(1)
	for i in range(n):
		# 在发布消息之后进行短暂的休眠, 让消息可以一条接一条的出现
		conn.publish('channel1', i)
		time.sleep(1)

def run_pubsub():
	# 启动发送者线程, 并让它发送三条消息
	threading.Thread(target=publisher, args=(3,)).start()
	# 创建发布订阅对象, 并让它订阅给定的频道
	pubsub = conn.pubsub()
	pubsub.subscribe(['channel1'])
	count = 0

	# 通过遍历函数pubsub.listen()的执行结果来监听订阅消息
	for item in pubsub.listen():
		# 打印接收到的每条消息
		print(item)
		# 在接收到一条订阅反馈消息和三条发送者发送的消息之后,
		# 执行退订操作, 停止接收新消息
		count += 1
		if count == 4:
			pubsub.unsubscribe()
		# 在接收到一条订阅反馈消息和三条发布者发送的消息之后,
		# 就不再接收消息
		if count == 5:
			break

run_pubsub()