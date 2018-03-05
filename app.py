from functools import wraps
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin

app = Sanic()
CORS(app)

def ccnu_loop(f):
    @wraps(f)
    async def decorator(*args, **kwargs):
        ccnu = args[0].app.config.CCNU
        cos = args[0].app.config.COS
        loop = args[0].app.loop
        canteen = ccnu.copy()
        return await f(*args, canteen, cos, loop, **kwargs)
    return decorator

from api import *
