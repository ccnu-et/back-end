import os
import asyncio
import aioredis
from app import app
from data_analysis import *
from recommender import *

async def handle_data():
    rds = await aioredis.create_redis(os.getenv('CCNU_RDS') or
            'redis://127.0.0.1:6380', loop=loop)
    canteen, cos = await init_dataset()
    await meta_data(rds, canteen)
    await max_canteen(rds, canteen)
    max_windows = await max_window(rds, canteen)
    await deal_data(rds, canteen)
    await day_canteen(rds, canteen)
    await recommender(rds, canteen, cos, max_windows)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    print("---- start data analysis ----")
    loop.run_until_complete(handle_data())
    print("---- data analysis done  ----")
    app.run(host='0.0.0.0', port=5000, debug=True)
