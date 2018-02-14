import pandas as pd
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

app = Sanic()
CORS(app)
app.config.CCNU = pd.read_csv('ccnu_data.csv')
executor = ThreadPoolExecutor(max_workers=15)

def ccnu_loop(f):
    @wraps(f)
    async def decorator(*args, **kwargs):
        ccnu = args[0].app.config.CCNU
        loop = args[0].app.loop
        canteen = ccnu.copy()
        return await f(*args, canteen, loop, **kwargs)
    return decorator

@app.listener('before_server_start')
async def drop_bad_data(app, loop):
    # 删除错误数据
    ccnu = app.config.CCNU
    drop_indexes = ccnu[
        #--------------------------------------------------
        (   ccnu['orgName'].str.contains("超市"))         |
        (   ccnu['orgName'].str.contains("保卫处"))       |
        (   ccnu['orgName'].str.contains("后勤处"))       |
        (   ccnu['orgName'].str.contains("信息化办公室")) |
        (   ccnu['orgName']      ==      '华中师范大学')  |
        (   ccnu['transMoney'] <= 0                     ) |
        (   ccnu['transMoney'] >= 90                    )#|
        #--------------------------------------------------
    ].index
    ccnu = ccnu.drop(drop_indexes)
    canteen = ccnu[ ccnu['orgName'].str.contains("饮食中心") ]
    canteen['orgName'] = canteen['orgName'].apply(
        lambda x: x.split('/')[3] + '&' + x.split('/')[-1]
    )
    app.config.CCNU = canteen

@app.route('/api/')
@ccnu_loop
async def meta_data(request, canteen, loop):
    # meta data
    data_len = len(canteen)
    return json({"meta": { 'data_len': data_len }})

@app.route('/api/max_canteen/')
@ccnu_loop
async def max_canteen(request, canteen, loop):
    # 消费次数最多的食堂
    # + 饼图
    canteen_count = canteen.groupby('canteen').size().reset_index(name="value")
    canteen_count.columns = ['name', 'value']
    data_dict = eval(canteen_count.to_json(orient='index'))
    legend_dict = eval(canteen_count['name'].to_json())
    return json({
        'data': data_dict.values(),
        'legend': legend_dict.values()
    })

@app.route('/api/max_window/')
@ccnu_loop
async def max_window(request, canteen, loop):
    # 刷卡次数前6的食堂窗口
    breakfast = ['06:30:00', '10:30:00']
    lunch = ['10:40:00', '14:00:00']
    dinner = ['17:00:00', '21:30:00']

    orgsb_list = await loop.run_in_executor(
        executor, handle_org, canteen, breakfast
    )
    orgsl_list = await loop.run_in_executor(
        executor, handle_org, canteen, lunch,
    )
    orgsd_list = await loop.run_in_executor(
        executor, handle_org, canteen, dinner,
    )

    return json({
        'breakfast': {
            'xAxis': [item['orgName'] for item in orgsb_list],
            'data': [item['value'] for item in orgsb_list] 
        },
        'lunch': {
            'xAxis': [item['orgName'] for item in orgsl_list],
            'data': [item['value'] for item in orgsl_list] 
        },
        'dinner': {
            'xAxis': [item['orgName'] for item in orgsd_list],
            'data': [item['value'] for item in orgsd_list] 
        }
    })

@app.route('/api/deal_data/')
@ccnu_loop
async def deal_data(request, canteen, loop):
    # 华师各食堂消费水平
    avg_data = canteen["transMoney"].mean().round(2),
    avg = avg_data[0]
    low = []; high = []
    for name in ["学子餐厅", "东一餐厅新", "东二餐厅", "博雅园餐厅", "学子中西餐厅",
                 "桂香园餐厅新", "沁园春餐厅", "北区教工餐厅", "南湖校区餐厅"]:
        await loop.run_in_executor(executor,
                handle_trans, canteen, name, low, high, avg)
    return json({
        'avg' : avg_data[0], 'low': low, 'high': high
    })

@app.route('/api/day_canteen/')
@ccnu_loop
async def day_canteen(request, canteen, loop):
    # 华师主要食堂每日刷卡量对比
    canteen['dealDay'] = pd.to_datetime(canteen['dealDateTime'])
    canteen['day'] = canteen['dealDay'].apply(lambda x: x.dayofweek)
    canteens = ["东一餐厅(旧201508)", "东一餐厅新", "东二餐厅", "北区教工餐厅",
            "南湖校区餐厅", "博雅园餐厅", "学子中西餐厅", "学子餐厅",
            "桂香园餐厅新", "沁园春餐厅"]
    json_list = []
    for name in canteens:
        json_dict = await loop.run_in_executor(executor, handle_day, canteen, name)
        json_list.append(json_dict)
    return json(json_list)

# cpu intensive tasks
## running in thread pool
def handle_org(canteen, time):
    canteen_x = canteen[
        (canteen['dealDateTime'].str.split().str[1] >= time[0]) &
        (canteen['dealDateTime'].str.split().str[1] <= time[-1])
    ]
    orgsx = canteen_x.groupby('orgName').size().reset_index(name="value")
    orgsx_dict = eval(orgsx.sort_values(by=['value']).tail(6).to_json(orient='index'))
    return orgsx_dict.values()

def handle_trans(canteen, name, low, high, avg):
    avgd = canteen[ (canteen["canteen"]==name) ]["transMoney"].mean().round(2)
    maxd = canteen[ (canteen["canteen"]==name) ]["transMoney"].max()
    if avgd < avg:
        low.append([maxd, avgd, name])
    else:
        high.append([maxd, avgd, name])

def handle_day(canteen, name):
    # 'dealDateTime'
    day_count = canteen[ canteen['canteen']==name ].groupby('day').size().reset_index()
    data_dict = eval(day_count.to_json())
    data_list = list(data_dict.values())
    return {
        'name': name, 'type': 'line', 'stack': '刷卡量',
        'data': list(data_list)[-1].values()
    }
     
# main
if __name__ == '__main__':
    # from aoiklivereload import LiveReloader
    # reloader = LiveReloader()
    # reloader.start_watcher_thread()
    app.run(host='0.0.0.0', port=3000, debug=True)
