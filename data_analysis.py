import pandas as pd
from sanic import Sanic
from sanic.response import json
from sanic_cors import CORS, cross_origin

app = Sanic()
CORS(app)
app.config.CCNU = pd.read_csv('ccnu_data.csv')

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
        (   ccnu['orgName']      ==      '华中师范大学')# |
        #--------------------------------------------------
    ].index
    ccnu = ccnu.drop(drop_indexes)
    app.config.CCNU = ccnu

@app.route('/api/')
async def meta_data(request):
    # meta data
    ccnu = request.app.config.CCNU
    # print("-------------- 上半年 ----------------")
    # print(ccnu[ ccnu['dealDateTime'] <= '2017-06-15 00:00:00' ].info())
    # print("-------------- 下半年 ----------------")
    # print(ccnu[ ccnu['dealDateTime'] > '2017-06-15 00:00:00' ].info())
    return json({"meta": ccnu.info()})

@app.route('/api/max_canteen/')
async def max_canteen(request):
    # 消费次数最多的食堂
    # + 饼图
    ccnu = request.app.config.CCNU
    canteen = ccnu[ ccnu['orgName'].str.contains("饮食中心") ]
    canteen_count = canteen.groupby('canteen').size().reset_index(name="value")
    canteen_count.columns = ['name', 'value']
    data_dict = eval(canteen_count.to_json(orient='index'))
    legend_dict = eval(canteen_count['name'].to_json())
    return json({
        'data': data_dict.values(),
        'legend': legend_dict.values()
    })

@app.route('/api/max_window/')
async def max_window(request):
    # 刷卡次数前五的食堂窗口
    ccnu = request.app.config.CCNU
    breakfast = ['06:30:00', '10:30:00']
    lunch = ['10:40:00', '14:00:00']
    dinner = ['17:00:00', '21:30:00']
    canteen = ccnu[ ccnu['orgName'].str.contains("饮食中心") ]

    orgsb_list = await handle_org(breakfast, canteen)
    orgsl_list = await handle_org(lunch, canteen)
    orgsd_list = await handle_org(dinner, canteen)

    return json({
        'breakfast': orgsb_list,
        'lunch': orgsl_list,
        'dinner': orgsd_list
    })

# handle functions
async def handle_org(time, canteen):
    canteen_x = canteen[
        (canteen['dealDateTime'].str.split().str[1] >= time[0]) &
        (canteen['dealDateTime'].str.split().str[1] <= time[-1])
    ]
    orgsx = canteen_x.groupby('orgName').size().reset_index(name="value")
    orgsx_dict = eval(orgsx.sort_values(by=['value']).tail(6).to_json(orient='index'))
    orgsx_list = await handle_orgName(orgsx_dict.values())
    return orgsx_list

async def handle_orgName(org_list):
    # 处理窗口名称:食堂\窗口
    for item in org_list:
        orgName_list = item["orgName"].split('/')
        item["orgName"] = orgName_list[3] + orgName_list[-1]
    return org_list
    
# main
if __name__ == '__main__':
    from aoiklivereload import LiveReloader
    reloader = LiveReloader()
    reloader.start_watcher_thread()
    app.run(host='0.0.0.0', port=3000, debug=True)
