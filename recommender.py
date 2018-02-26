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
    return json(recommends)

# cpu-intensive function
def handle_recommendations(orgName, cos, orgs):
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
    srcs = [
        'https://avatars0.githubusercontent.com/u/5249050?s=460&v=4',
        'https://avatars0.githubusercontent.com/u/5249050?s=460&v=4',
        'https://avatars0.githubusercontent.com/u/5249050?s=460&v=4'
    ]
    return {
        'breakfast': {
            'like': orgs[0],
            'recommends': json_format(
                orgName['orgName'].iloc[bindices],
                bindices, srcs
            )
        },
        'lunch': {
            'like': orgs[1],
            'recommends': json_format(
                orgName['orgName'].iloc[lindices],
                lindices, srcs
            )
        },
        'dinner': {
            'like': orgs[2],
            'recommends': json_format(
                orgName['orgName'].iloc[dindices],
                dindices, srcs
            )
        },
    }

def handle_orglike(sid, canteen):
    breakfast = ['06:30:00', '10:30:00']
    lunch = ['10:40:00', '14:00:00']
    dinner = ['17:00:00', '21:30:00']
    # print(canteen.info())
    # canteen = canteen[ (canteen['userId'] == int(sid)) ]  # 特定学生的消费记录
    # print(canteen.head())
    breakfast_canteen = canteen[
        (canteen['dealDateTime'].str.split().str[1] >= breakfast[0]) &
        (canteen['dealDateTime'].str.split().str[1] <= breakfast[-1])
    ]
    lunch_canteen = canteen[
        (canteen['dealDateTime'].str.split().str[1] >= lunch[0]) &
        (canteen['dealDateTime'].str.split().str[1] <= lunch[-1])
    ]
    dinner_canteen = canteen[
        (canteen['dealDateTime'].str.split().str[1] >= dinner[0]) &
        (canteen['dealDateTime'].str.split().str[1] <= dinner[-1])
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
