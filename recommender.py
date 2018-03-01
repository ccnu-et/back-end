import aiohttp
from app import app, ccnu_loop, executor, json, pd

@app.route('/api/recommender/', methods=['GET'])
@ccnu_loop
async def recommender(request, canteen, cos, loop):
    # 依据食堂窗口名称, 提取标签做推荐
    # 使用TF-IDF矩阵, 计算余弦相似度
    sid = request.args.get('sid')
    orgs = await loop.run_in_executor(
        executor, handle_orglike, sid, canteen
    )
    orgName = canteen.groupby('orgName').size().reset_index(name="value")
    recommends = await loop.run_in_executor(
        executor, handle_recommendations, orgName, cos, orgs
    )
    borgs, bindices = recommends['breakfast']
    lorgs, lindices = recommends['lunch']
    dorgs, dindices = recommends['dinner']
    borgs = borgs[0].tolist()
    lorgs = lorgs[0].tolist()
    dorgs = dorgs[0].tolist()
    """
    bsrcs = []; lsrcs = []; dsrcs = []
    for borg in borgs:
        try:
            bsrc = await get_src(borg)
        except:
            bsrc = "https://avatars0.githubusercontent.com/u/23288182?s=460&v=4"
        bsrcs.append(bsrc)
    for lorg in lorgs:
        try:
            lsrc = await get_src(lorg)
        except:
            lsrc = "https://avatars0.githubusercontent.com/u/23288182?s=460&v=4"
        lsrcs.append(lsrc)
    for dorg in dorgs:
        try:
            dsrc = await get_src(dorg)
        except:
            dsrc = "https://avatars0.githubusercontent.com/u/23288182?s=460&v=4"
        dsrcs.append(dsrc)
    print(len(bsrcs))
    """
    bsrcs = lsrcs = dsrcs = ["", "", ""]
    json_data = {
        'breakfast': {
            'head': "早餐你的最爱是 " + orgs[0] + ",推荐你尝尝以下窗口",
            'recommends': json_format(borgs, bindices, bsrcs)
        },
        'lunch': {
            'head': "中餐你的最爱是 " + orgs[1] + ",推荐你尝尝以下窗口",
            'recommends': json_format(lorgs, lindices, lsrcs)
        },
        'dinner': {
            'head': "晚餐你的最爱是 " + orgs[2] + ",推荐你尝尝以下窗口",
            'recommends': json_format(dorgs, dindices, dsrcs)
        }
    }
    return json(json_data)

async def get_src(org):
    import json
    imgapi = "https://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&ie=utf-8&oe=utf-8&word=%s"
    src = ""
    async with aiohttp.ClientSession() as session:
        async with session.get(imgapi % org.split('&')[1], timeout=3) as resp:
            data = await resp.text()
            json_data = json.loads(data)
            src = json_data['data'][0]['thumbURL']
    return src

# cpu-intensive function
def handle_recommendations(orgName, cos, orgs):
    # 食堂窗口推荐函数
    # 根据余弦相似度推荐相似度较高的窗口
    bindex = orgName[ (orgName['orgName'] == orgs[0]) ].index[0]  # breakfast
    lindex = orgName[ (orgName['orgName'] == orgs[1]) ].index[0]  # lunch
    dindex = orgName[ (orgName['orgName'] == orgs[2]) ].index[0]  # dinner
    bscores = sorted(
        list(enumerate(cos[bindex])), key=lambda x: x[1], reverse=True
    )[1:4]
    lscores = sorted(
        list(enumerate(cos[lindex])), key=lambda x: x[1], reverse=True
    )[1:4]
    dscores = sorted(
        list(enumerate(cos[dindex])), key=lambda x: x[1], reverse=True
    )[1:4]
    bindices = [i[0] for i in bscores]
    lindices = [i[0] for i in lscores]
    dindices = [i[0] for i in dscores]
    borgs = orgName['orgName'].iloc[bindices],
    lorgs = orgName['orgName'].iloc[lindices],
    dorgs = orgName['orgName'].iloc[dindices],
    return {
        "breakfast": (borgs, bindices),
        "lunch": (lorgs, lindices),
        "dinner": (dorgs, dindices)
    }

def handle_orglike(sid, canteen):
    breakfast = ['06:30:00', '10:30:00']
    lunch = ['10:40:00', '14:00:00']
    dinner = ['17:00:00', '21:30:00']
    sid_canteen = canteen[ (canteen['userId'] == int(sid)) ]  # 特定学生的消费记录
    breakfast_canteen = sid_canteen[
        (sid_canteen['dealDateTime'].str.split().str[1] >= breakfast[0]) &
        (sid_canteen['dealDateTime'].str.split().str[1] <= breakfast[-1])
    ]
    lunch_canteen = sid_canteen[
        (sid_canteen['dealDateTime'].str.split().str[1] >= lunch[0]) &
        (sid_canteen['dealDateTime'].str.split().str[1] <= lunch[-1])
    ]
    dinner_canteen = sid_canteen[
        (sid_canteen['dealDateTime'].str.split().str[1] >= dinner[0]) &
        (sid_canteen['dealDateTime'].str.split().str[1] <= dinner[-1])
    ]
    breakfast_orgs = breakfast_canteen.groupby('orgName').size().reset_index(name="value")
    lunch_orgs = lunch_canteen.groupby('orgName').size().reset_index(name="value")
    dinner_orgs = dinner_canteen.groupby('orgName').size().reset_index(name="value")
    re_breakfast = eval(breakfast_orgs.sort_values(by=['value']).tail(1).to_json())
    re_lunch = eval(lunch_orgs.sort_values(by=['value']).tail(1).to_json())
    re_dinner = eval(dinner_orgs.sort_values(by=['value']).tail(1).to_json())
    return list(re_breakfast['orgName'].values()) + \
           list(re_lunch['orgName'].values()) + \
           list(re_dinner['orgName'].values())

def json_format(orgNames, scores, srcs):
    z = zip(orgNames, scores, srcs)
    return [{'name': name, 'star': star, 'src': src} for name, star, src in z]
