import asyncio
import aiohttp
from data_analysis import ccnu_cache, executor

@ccnu_cache
async def recommender(rds, canteen, cos, max_windows):
    # 依据食堂窗口名称, 提取标签做推荐
    # 使用TF-IDF矩阵, 计算余弦相似度
    loop = asyncio.get_event_loop()
    data_dict = {}
    sids = list(canteen.userId.unique())
    # sids = [2016210324]
    for sid in sids:
        try:
            data = await sid_recommender(sid, canteen, cos, loop, max_windows)
        except Exception as e:
            print("<Error> " + str(e))
            continue
        data_dict[sid] = data
        print("<Info> " + str(sid))
    return data_dict

async def sid_recommender(sid, canteen, cos, loop, max_windows):
    orgs = await loop.run_in_executor(
        executor, handle_orglike, sid, canteen
    )
    orgName = canteen.groupby('orgName').size().reset_index(name="value")
    report, info = await loop.run_in_executor(
        executor, handle_report, sid, canteen
    )
    recommends = await loop.run_in_executor(
        executor, handle_recommendations, orgName, cos, orgs
    )
    borgs, bindices = recommends['breakfast']
    lorgs, lindices = recommends['lunch']
    dorgs, dindices = recommends['dinner']
    borgs = borgs[0].tolist()
    lorgs = lorgs[0].tolist()
    dorgs = dorgs[0].tolist()
    bsrcs = lsrcs = dsrcs = ["", "", ""]
    return {
        'info': info,
        'report': report,
        'breakfast': {
            'head': "<h2>早餐你的最爱是 " + orgs[0] + ",推荐你尝尝以下窗口</h2><br/>",
            'recommends': json_format(borgs, bindices, bsrcs,
                max_windows['breakfast']['xAxis'])
        },
        'lunch': {
            'head': "<h2>中餐你的最爱是 " + orgs[1] + ",推荐你尝尝以下窗口</h2>",
            'recommends': json_format(lorgs, lindices, lsrcs,
                max_windows['lunch']['xAxis'])
        },
        'dinner': {
            'head': "<h2>晚餐你的最爱是 " + orgs[2] + ",推荐你尝尝以下窗口</h2>",
            'recommends': json_format(dorgs, dindices, dsrcs,
                max_windows['dinner']['xAxis'])
        }
    }

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
    re_breakfast = list(eval(
        breakfast_orgs.sort_values(by=['value']).tail(1).to_json()
    )['orgName'].values()) or ['&']
    re_lunch = list(eval(
        lunch_orgs.sort_values(by=['value']).tail(1).to_json()
    )['orgName'].values()) or ['&']
    re_dinner = list(eval(
        dinner_orgs.sort_values(by=['value']).tail(1).to_json()
    )['orgName'].values()) or ['&']
    return re_breakfast + re_lunch + re_dinner

def handle_report(sid, canteen):
    # 个人食堂消费报告
    """
    --5. 各个食堂消费占比图
    """
    stu = canteen[ (canteen['userId'] == sid) ]
    maxval = stu['transMoney'].max()
    maxdate = stu[ (stu['transMoney'] == maxval) ]['dealDateTime'].to_string().split()
    stu['month'] = stu['dealDateTime'].apply(
        lambda x: x.split()[0].split('-')[1]
    )
    month = stu.groupby('month')
    month_max = month['transMoney'].sum().idxmax()
    month_max_val = round(month['transMoney'].sum().max())
    month_min_val = round(month['transMoney'].sum().min())
    if month_min_val == 0:
        month_min_val = 1
    month_val = round(month_max_val / month_min_val)
    page1 = "<h2>%s, %s你在食堂吃的最放肆的一顿一共花了%s元.</h2>\
           <h2>你最土豪的月份是%s月, 消费高达%s元, 竟是最低月份的%s倍.</h2>" % \
            (maxdate[1], maxdate[2], maxval, month_max, month_max_val, month_val)
    all_val = round(stu['transMoney'].sum())
    stus = canteen.groupby('userId')
    stus_l = stus['transMoney'].sum().to_string().split('\n')
    stus_l = sorted(stus_l[1:], key=lambda x: x.split()[1])
    slen = len(stus_l[-1])
    transMoney = '%.02f' % stu['transMoney'].sum()
    stu_mon = str(sid) + ' '*(slen-len(transMoney)-10) + transMoney
    ranking = stus_l.index(stu_mon)
    percent = str(round(ranking / len(stus_l) * 100)) + "%"
    page2 = "<h2>你在食堂共消费%s元, 超过了全校%s的人, 全校排名第%s名.</h2><br>" % \
            (all_val, percent, ranking)
    info = stu.userName.to_string().split('\n')[0].split()[1] + " " + str(sid)
    return page1 + page2, info

def json_format(orgNames, scores, srcs, max_windows_list):
    z = zip(orgNames, scores, srcs)
    def get_name(name, star):
        if star == 0:
            name=max_windows_list[0]
        return name
    def get_star(star):
        if star == 0:
            star=3
        return star
    return [{'name': get_name(name, star), 'star': get_star(star) , 'src': src} \
            for name, star, src in z]
