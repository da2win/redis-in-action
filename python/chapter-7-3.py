import redis
import uuid
import unittest

def _zset_common(conn, method, scores, ttl=300, **kw):
    # 创建一个新的临时标识符。
    id = str(uuid.uuid4())                                 
    # 调用者可以通过传递参数来决定是否使用事务流水线。
    execute = kw.pop('_execute', True)                     
    # 设置事务流水线，保证每个单独的调用都有一致的结果。
    pipeline = conn.pipeline(True) if execute else conn    
    # 为输入的键添加 ‘idx:’ 前缀。
    for key in scores.keys():                             
        scores['idx:' + key] = scores.pop(key)             
    # 为将要被执行的操作设置好相应的参数。
    getattr(pipeline, method)('idx:' + id, scores, **kw)   
    # 为计算结果有序集合设置过期时间。
    pipeline.expire('idx:' + id, ttl)                      
    # 除非调用者明确指示要延迟执行操作，否则实际地执行计算操作。
    if execute:                                            
        pipeline.execute()                                  
    # 将计算结果的 ID 返回给调用者，以便做进一步的处理。
    return id    

#######################################################################
#######################################################################
#######################################################################

def zintersect(conn, items, ttl=30, **kw):
	return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)

# 对有序集合执行并集计算的辅助函数。
def zunion(conn, items, ttl=30, **kw):                                  
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)    

def add_job(conn, job_id, required_skills):
	# 把职位所需的技能全部添加到职位对应的集合里面
	conn.sadd('job:' + job_id, *required_skills)

def is_qualified(conn, job_id, candidate_skills):
	temp = str(uuid.uuid4())
	pipeline = conn.pipeline(True)
	# 把求职者拥有的技能全部添加到一个临时集合里面, 并设置过期时间
	pipeline.sadd(temp, *candidate_skills)
	# 找出职位所需技能当中, 求职者不具备的那些技能, 并把它们记录到结果集合里面
	pipeline.expire(temp, 5)
	# 如果求职者具备职位所需的全部技能, 那么返回True
	pipeline.sdiff('job:' + job_id, temp)
	return not pipeline.execute()[-1]

# 根据所需技能对职位进行索引的函数
def index_job(conn, job_id, skills):
    pipeline = conn.pipeline(True)
    for skill in skills:
        # 将职位 ID 添加到相应的技能集合里面。
        pipeline.sadd('idx:skill:' + skill, job_id)            
    # 将职位所需技能的数量添加到记录了所有职位所需技能数量的有序集合里面。
    pipeline.zadd('idx:jobs:req', job_id, len(set(skills)))    
    pipeline.execute()

# 找出求职者能够胜任的所有工作
def find_jobs(conn, candidate_skills):
    # 设置好用于计算职位得分的字典。
    skills = {}                                                
    for skill in set(candidate_skills):                        
        skills['skill:' + skill] = 1                          

    # 计算求职者对于每个职位的得分。
    job_scores = zunion(conn, skills)                          
    # 计算出求职者能够胜任以及不能够胜任的职位。
    final_result = zintersect(                                 
        conn, {job_scores:-1, 'jobs:req':1})                   
    print('idx:' + final_result)
    print(conn.zrangebyscore('idx:' + final_result, 0, 0))
    # 返回求职者能够胜任的那些职位。
    return conn.zrangebyscore('idx:' + final_result, 0, 0)   

class TestCh07(unittest.TestCase):
    content = 'this is some random content, look at how it is indexed.'
    def setUp(self):
        self.conn = redis.Redis(db=15, password='payexpress.redis')
    #     self.conn.flushdb()
    # def tearDown(self):
    #     self.conn.flushdb()

    def test_is_qualified_for_job(self):
        add_job(self.conn, 'test', ['q1', 'q2', 'q3'])
        self.assertTrue(is_qualified(self.conn, 'test', ['q1', 'q3', 'q2']))
        self.assertFalse(is_qualified(self.conn, 'test', ['q1', 'q2']))

    def test_index_and_find_jobs(self):
        index_job(self.conn, 'test1', ['q1', 'q2', 'q3'])
        index_job(self.conn, 'test2', ['q1', 'q3', 'q4'])
        index_job(self.conn, 'test3', ['q1', 'q3', 'q5'])

        self.assertEquals(find_jobs(self.conn, ['q1']), [])
        # self.assertEquals(find_jobs(self.conn, ['q1', 'q3', 'q4']), [b'test2'])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q3', 'q5']), [b'test3'])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q2', 'q3', 'q4', 'q5']), [b'test1', b'test2', b'test3'])


if __name__ == '__main__':
	unittest.main()
