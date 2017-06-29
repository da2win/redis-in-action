import time


# 设置一个字典， 将大部分日志的安全级别映射为字符串
SERVERITY = {
	logging.DUBUG: 'debug',
	logging.INFO: 'info',
	logging.WARNING: 'warning',
	logging.ERROR: 'error',
	logging.CRITICAL: 'critical'
}

# 尝试将日志的安全级别转换为简单字符串
SERVERITY.update((name, name) for name in SERVERITY.values())

def log_recent(conn, name, message, serverity=logging.INFO, pipe=None):
	serverity = str(SERVERITY.get(serverity, serverity)).lower()

	# 创建负责存储消息的键
	destination = 'recent:%s:%s' % (name, serverity)

	# 将当前时间添加到消息里面，用于记录消息的发送时间
	message = time.asctime() + ' ' + message
	# 使用流水线来将通信往返次数降低为一次
	pipe = pipe or conn.pipeline()
	# 将消息添加到列表的最前面
	pipe.lpush(destination, message)
	# 对日志列表进行修剪，让它包含最新的100条消息
	pipe.ltrim(destination, 0, 99)
	# 执行两个命令
	pipe.execute()

def log_common(conn, name, message, serverity=logging.INFO, timeout=5):
	