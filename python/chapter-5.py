import time
import threading
import redis

# 采用事务的版本@@@
conn = redis.Redis(db=15)
def notrans():
	# 创建一个事务型流水线对象
	pipeline = conn.pipeline()
	# 把针对 'trans:' 的计数器的自增操作放入队列
	pipeline.incr('trans:')
	# 等待100毫秒
	time.sleep(.1)
	# 把针对 'trans:' 的计数器的自减操作放入队列
	pipeline.incr('trans:', -1)
	# 执行被事务包裹的买两块, 并打印自增操作的执行结果
	print(pipeline.execute()[0])
if 1:
	# 启动3个现成来执行没有被事务包裹的自增、休眠、和自减操作.
	for i in range(3):
		threading.Thread(target=notrans).start()
	# 等待 500 毫秒, 让操作有足够的时间完成
	time.sleep(.5)
	
	# @rs
	# 1
	# 1
	# 1