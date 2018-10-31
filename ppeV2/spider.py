import asyncio
import random
import re
import aioredis
import motor.motor_asyncio
import redis
import redisbloomfilter
from pyppeteer import errors
from fake_useragent import UserAgent
from pyppeteer import launch
from lxml import etree


class jdSpider(object):

    def __init__(self, db, bf):
        self.db = db
        self.bf = bf

    async def adownloader(self, url, loop, key):
        redis_conn = await aioredis.create_redis_pool('redis://localhost', loop=loop, password='qwe123')
        coll = self.db[key]

        browser_options = {
            'headless': False, # 无界面模式
            # 'autoClose': False, # 自动关闭
            'args': [
                '--user-agent={}'.format(UserAgent().random), # 设置UA
                # '--proxy-server=http://127.0.0.1:8080', # 设置代理
                # '--no-sandbox',
                     ],
                           }
        browser = await launch(options=browser_options)
        page = await browser.newPage()
        await page.goto(url)

        # 持续点击下一页, 一直到不可点击
        has_next = await page.xpath('//a[@class="pn-next"]')
        while has_next:
            wait = random.randint(2, 4)

            # 加载后30个item
            js1 = "var q=document.documentElement.scrollTop=8500"
            await page.evaluate(js1)
            try_time = 0
            while try_time < 5:
                try:
                    wait_item = await page.waitForXPath('//ul[@class="gl-warp clearfix"]/li[57]', timeout=150)
                    if wait_item:
                        break
                except errors.TimeoutError:
                    # await asyncio.sleep(wait) # 请求太快后30个加载不出来
                    pos2 = random.randint(6000, 9000)
                    js2 = "var q=document.documentElement.scrollTop={}".format(pos2)
                    await page.evaluate(js2)
                    try_time += 1

            # 解析页面
            html = await page.content()
            await self.parse(html, redis_conn, key)

            # 判断是否需要存入mongo
            total_num = await redis_conn.llen(key)
            if total_num >= 1500:
                print('开始存入mongo..')
                await self.save_to_mongo(redis_conn, coll, total_num, key)

            # 点击下一页
            has_next = await page.xpath('//a[@class="pn-next"]')
            try:
                await page.click('#J_bottomPage > span.p-num > a.pn-next')
            except errors.ElementHandleError:
                await page.click('#J_bottomPage > span.p-num > a.pn-next')

        # 最后一次redis_to_mongo
        total_num = await redis_conn.llen(key)
        if total_num:
            print('开始存入mongo..')
            await self.save_to_mongo(redis_conn, coll, total_num, key)
        print('succeed')


    async def parse(self, html, redis_conn, key):
        doc = etree.HTML(html)
        el = doc.xpath('//ul[@class="gl-warp clearfix"]/li')
        extract = lambda elem: elem[0] if elem else ''
        for item in el:
            item_id = item.xpath('.//div[@class="p-name p-name-type-2"]/a/i/@id')
            title = item.xpath('.//div/div[@class="p-name p-name-type-2"]/a/em/text()[2]')
            price = item.xpath('.//div/div[@class="p-price"]/strong/i/text()')
            comments = item.xpath('.//strong/a/text()')
            shop = item.xpath('.//div/div[@class="p-shop"]/span/a/text()')

            item_id = extract(item_id)
            item_id = extract(re.findall(r'J_AD_(\d+)', item_id))
            # 检查item_id是否存在过滤表中
            exists = self.bf.exists(item_id)
            if not exists:
                self.bf.insert(item_id)
                data = {
                    'item_id': item_id,
                    'title': extract(title),
                    'price': extract(price),
                    'comments': extract(comments),
                    'shop': extract(shop),
                }
                print(data)
                await self.save_to_redis(data, redis_conn, key)


    async def save_to_redis(self, item, redis_conn, key):
        await redis_conn.lpush(key, str(item))


    async def save_to_mongo(self, redis_conn, coll, total_num, key):
        items = []
        for i in range(total_num):
            item = await redis_conn.rpop(key)
            items.append(eval(item.decode()))
        if items:
            await coll.insert_many(items)


def main():
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://Bridi:anNBU7MD@localhost:27017/')
    db = client['jd']
    bf = redisbloomfilter.BloomFilter(db=0, key='jd_bf', password='qwe123') # 初始化bloomfilter

    keys = ['蓝牙耳机', '固态硬盘', '机械键盘']
    # keys = ['macbook']
    base_url = 'https://search.jd.com/Search?keyword={}&enc=utf-8&wq={}'

    jdCrawler = jdSpider(db, bf)
    loop = asyncio.get_event_loop()
    tasks = []
    for key in keys:
        url = base_url.format(key, key)
        task = asyncio.ensure_future(jdCrawler.adownloader(url, loop, key))
        tasks.append(task)
    loop.run_until_complete(tasks)


if __name__ == '__main__':
    main()