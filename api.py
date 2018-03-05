import os
import json
import aioredis
from functools import wraps
from app import app, json

def get_resp(f):
    @wraps(f)
    async def decorator(*args, **kwargs):
        loop = args[0].app.loop
        rds = await aioredis.create_redis(os.getenv('CCNU_RDS') or
                'redis://127.0.0.1:6380', loop=loop)
        resp = await rds.get(f.__name__)
        return await f(*args, resp, **kwargs)
    return decorator

@app.route('/api/')
@get_resp
async def api_meta_data(request, resp):
    return json({"meta": { 'data_len': int(resp) }})

@app.route('/api/max_canteen/')
@get_resp
async def api_max_canteen(request, resp):
    return json(eval(resp))

@app.route('/api/max_window/')
@get_resp
async def api_max_window(request, resp):
    return json(eval(resp))

@app.route('/api/deal_data/')
@get_resp
async def api_deal_data(request, resp):
    return json(eval(resp))

@app.route('/api/day_canteen/')
@get_resp
async def api_day_canteen(request, resp):
    return json(eval(resp))

@app.route('/api/recommender/')
@get_resp
async def api_recommender(request, resp):
    sid = request.args.get('sid')
    data_dict = eval(resp)
    data = data_dict.get(int(sid))
    return json(data)
