import time
import threading
import redis

# 未采用事务的版本@@@
conn = redis.Redis(db=15)
def notrans():
	# 对'notrans:'计数器执行自增操作并打印操作的执行结果
	print(conn.incr('notrans:'))
	# 等待100毫秒
	time.sleep(.1)
	# 对 'notrans:' 计数器进行自减操作
	conn.incr('notrans:', -1)

if 1:
	# 启动3个现成来执行没有被事务包裹的自增、休眠、和自减操作.
	for i in range(3):
		threading.Thread(target=notrans).start()
	# 等待 500 毫秒, 让操作有足够的时间完成
	time.sleep(.5)
	# 因为没有使用事务, 所以3个现成执行的各个命令将互相交错, 
	# 使得计数器的值持续地增大