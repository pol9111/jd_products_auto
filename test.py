# import redisbloomfilter
#
# print(dir(redisbloomfilter))
#
# bf = redisbloomfilter.BloomFilter(db=0, key='jd_bf')
#
# urls = [
#     'http://www.baidu.com',
#     'http://www.qq.com',
#     'http://www.163.com',
#     'http://www.jd.com',
#     'http://www.taobao.com',
#     'http://www.sina.com.cn',
#     'http://www.nba.com',
#     'http://www.google.com'
# ]
#
#
# for each in urls:
#     exists = bf.exists(each)
#     print('{} exists is {}'.format(each, exists))
#     if not exists:
#         bf.insert(each)
#         print('{} insert success.'.format(each))




import pymongo

client = pymongo.MongoClient('mongodb://Bridi:anNBU7MD@localhost:27017/') # 建立连接
db = client['zzz'] # 指定数据库
coll = db['qqq'] # 指定集合
coll.insert_one({'q':1})



import asyncio
import pprint
import motor.motor_asyncio


client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://Bridi:anNBU7MD@localhost:27017/')
db = client['test']
coll = db['test_collection']


async def do():
    document = {'key': 'value'}
    result = await db.test_collection.insert_one(document)
    print('result %s' % repr(result.inserted_id))
    document = await coll.find_one({'i': {'$lt': 1}})
    pprint.pprint(document)


loop = asyncio.get_event_loop()
loop.run_until_complete(do())
loop.close()



