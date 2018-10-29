import asyncio
import random
import aioredis
import motor.motor_asyncio
from pyppeteer import errors
from fake_useragent import UserAgent
from pyppeteer import launch
from lxml import etree
import time


async def adownloader(url, loop, db, key):
    redis_conn = await aioredis.create_redis_pool('redis://localhost', loop=loop)
    coll = db[key]

    browser_options = {
        'headless': False, # 无界面模式
        'autoClose': False, # 自动关闭
        'args': [
            '--user-agent={}'.format(UserAgent().random), # 设置UA
            # '--proxy-server=http://127.0.0.1:8080', # 设置代理
            '--no-sandbox',
                 ],
                       }
    browser = await launch(options=browser_options)
    page = await browser.newPage()
    await page.goto(url)

    has_next = await page.xpath('//a[@class="pn-next"]')
    while has_next:
        wait = random.randint(2, 4)

        js1 = "var q=document.documentElement.scrollTop=9000"
        await page.evaluate(js1)

        try_time = 0
        while try_time > 5:
            try:
                wait_item = await page.waitForXPath('//ul[@class="gl-warp clearfix"]/li[55]', timeout=300)
                if wait_item:
                    break
            except errors.TimeoutError:
                # time.sleep(3)
                await asyncio.sleep(wait)
                pos = random.randint(0, 9000)
                js3 = "var q=document.documentElement.scrollTop={}".format(pos)
                await page.evaluate(js3)
                try_time += 1

        html = await page.content()
        await parse(html, redis_conn)

        total_num = await redis_conn.llen('jd_items')
        if total_num >= 3000:
            print('开始存入mongo..')
            await save_to_mongo(redis_conn, coll, total_num)
            await asyncio.sleep(10)

        has_next = await page.xpath('//a[@class="pn-next"]')
        try:
            await page.click('#J_bottomPage > span.p-num > a.pn-next')
        except errors.ElementHandleError:
            await asyncio.sleep(wait)
            await page.click('#J_bottomPage > span.p-num > a.pn-next')

    total_num = await redis_conn.llen('jd_items')
    if total_num:
        print('开始存入mongo..')
        await save_to_mongo(redis_conn, coll, total_num)
    print('succeed')


async def parse(html, redis_conn):
    doc = etree.HTML(html)
    el = doc.xpath('//ul[@class="gl-warp clearfix"]/li')
    extract = lambda elem: elem[0] if elem else ''
    for item in el:
        title = item.xpath('.//div/div[@class="p-name p-name-type-2"]/a/em/text()[2]')
        price = item.xpath('.//div/div[@class="p-price"]/strong/i/text()')
        comments = item.xpath('.//strong/a/text()')
        shop = item.xpath('.//div/div[@class="p-shop"]/span/a/text()')
        data = {
            'title': extract(title),
            'price': extract(price),
            'comments': extract(comments),
            'shop': extract(shop),
        }
        print(data)
        await save_to_redis(data, redis_conn)


async def save_to_redis(item, redis_conn):
    await redis_conn.lpush('jd_items', str(item))


async def save_to_mongo(redis_conn, coll, total_num):
    items = []
    for i in range(total_num):
        item = await redis_conn.rpop('jd_items')
        items.append(eval(item.decode()))
    if items:
        await coll.insert_many(items)


def main():
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
    db = client['jd']

    # keys = ['ipad', 'macbook']
    keys = ['macbook']
    base_url = 'https://search.jd.com/Search?keyword={}'

    loop = asyncio.get_event_loop()
    tasks = []
    for key in keys:
        url = base_url.format(key)
        task = asyncio.ensure_future(adownloader(url, loop, db, key))
        tasks.append(task)
    loop.run_until_complete(asyncio.gather(*tasks))


if __name__ == '__main__':
    main()