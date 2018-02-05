import os
import sys
import uvloop
import asyncio
import uvloop
import aiohttp
import aioredis
import threading

console_url = os.getenv('CONSOLE_URL')
# 2017: 2017210001~2017215839
# 2013: no data
starts = [2014210001, 2016210001, 2015210001]
ends =   [2014214840, 2016214628, 2015214780]

async def spider_per_sid(userId, days, loop):
    print("%s start!" % userId)
    redis = await aioredis.create_redis('redis://localhost:6380', loop=loop)
    key = userId[:4]
    data = await redis.hget(key, userId)
    if data is not None:
        return
    async with aiohttp.ClientSession() as session:
        async with session.get(console_url % (userId, days)) as resp:
            try:
                resp_json = await resp.json()
                await redis.hset(key, userId, str(resp_json))
            except asyncio.TimeoutError:
                return
    redis.close()
    await redis.wait_closed()
    print(">>>> %s done!" % userId)

async def co_2014(loop):
    for userId in range(starts[0], ends[0]+1):
        await spider_per_sid(str(userId), 365, loop)
    
async def co_2015(loop):
    for userId in range(starts[2], ends[2]+1):
        await spider_per_sid(str(userId), 365, loop)

async def co_2016(loop):
    for userId in range(starts[1], ends[1]+1):
        await spider_per_sid(str(userId), 365, loop)

def run_loop(grade):
    print("------- %d thread start ---------" % grade)
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    if grade == 2014:
        loop.run_until_complete(co_2014(loop))
    if grade == 2015:
        loop.run_until_complete(co_2015(loop))
    if grade == 2016:
        loop.run_until_complete(co_2016(loop))

if __name__ == '__main__':
    starts = eval(sys.argv[1])
    ends = eval(sys.argv[2])
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    t1 = threading.Thread(target=run_loop, args=(2016,))
    t2 = threading.Thread(target=run_loop, args=(2015,))
    t3 = threading.Thread(target=run_loop, args=(2016,))
    # t1.start()
    # t2.start()
    t3.start()
