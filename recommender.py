from app import app, ccnu_loop, executor, json, pd

@app.route('/api/recommender/', methods=['POST'])
@ccnu_loop
async def recommender(request, canteen, cos, loop):
    # 依据食堂窗口名称, 提取标签做推荐
    # 使用TF-IDF矩阵, 计算余弦相似度
    org = request.json['orgName']
    orgName = canteen.groupby('orgName').size().reset_index(name="value")
    index = orgName[ (orgName['orgName'] == org) ].index[0]
    recommends = await loop.run_in_executor(
        executor, handle_recommendations, orgName, cos, index
    )
    return json({ 'recommends': recommends })

# cpu-intensive function
def handle_recommendations(orgName, cos, index):
    sim_scores = sorted(
        list(enumerate(cos[index])), key=lambda x: x[1], reverse=True
    )[1:4]
    movie_indices = [i[0] for i in sim_scores]
    return orgName['orgName'].iloc[movie_indices]
