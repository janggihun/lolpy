import requests
import pandas as pd
import pymysql
import random
from tqdm import tqdm
import time

api_key = 'RGAPI-e674eb69-7d34-41d9-adfb-e43ad16950ca' #본인걸로 바꾸기 빅데이터과정 장기훈의 api입니다.


# mysql 사용자지정
def connect_mysql(db='mydb'):
    conn = pymysql.connect(host='localhost', port=3306,
                           user='icia', password='1234',
                           db=db, charset='utf8')
    return conn

#쿼리 실행시구문
def sql_execute(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

#딕셔너리만들때사용
def sql_execute_dict(conn, query):
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    return result

## 여기서부터 롤입니다.
def get_puuid(nickname, tag):
    url = f'https://asia.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{nickname}/{tag}?api_key={api_key}'
    res = requests.get(url).json()
    puuid = res['puuid']
    return puuid


def get_match_id(puuid,num):
    url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={api_key}'
    match_list = requests.get(url).json()
    return match_list


def get_matches_timelines(matchid):
    url1 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}?api_key={api_key}'
    url2 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{matchid}/timeline?api_key={api_key}'
    matches = requests.get(url1).json()
    timelines = requests.get(url2).json()
    return matches, timelines

# 위 3개를 가지고 티어를 넣어서 raw데이터를 얻기

def get_rawdata(tier):
    division_list = ['I','II','III','IV']
    lst = []
    page = random.randrange(1,20)
    print('get summonerId....')
    
    for division in tqdm(division_list):
        url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={api_key}'
        res = requests.get(url).json()
        lst += random.sample(res,3) # 테스트시 1 , 추후에 3으로 바꿔두기
    # lst라는 변수에서 summonerId만 리스트에 담기
    summoner_id_list = list(map(lambda x:x['summonerId'] ,lst))
    # summonerId가 담긴 리스트를 통해 puuId
    print('get puuId.....')
    puu_id_list = []
    for summoner_id in tqdm(summoner_id_list):
        url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}?api_key={api_key}'
        res = requests.get(url).json()
        puu_id = res['puuid']
        puu_id_list.append(puu_id)
    
    print('get match_id....')
    match_id_list = []
    #puuId를 통해 matchId를 가져오기 -> 3개씩 담기
    for puu_id in tqdm(puu_id_list):
        match_ids = get_match_id(puu_id,3)
        match_id_list.extend(match_ids)
    print('get matches & timeline....')
    df_create = []
    for match_id in tqdm(match_id_list):
        matches,timelines = get_matches_timelines(match_id)
        df_create.append([tier,match_id,matches,timelines]) #티어까지 넣어줘야함
    #matches,timeline을 불러서 이중리스트를 만들고 데이터프레임으로 만들어서 - [match_id,matches,timelines]
    df =pd.DataFrame(df_create,columns = ['tier','match_id','matches','timelines'])#티어까지 넣어줘야함
    return df
    
def get_match_timeline_df(df):
    # df를 한개로 만들기
    df_creater = []
    print('소환사 스텟 생성중.....')
    for i in tqdm(range(len(df))):       
        # matches 관련된 데이터 
        try:
            if df.iloc[i].matches['info']['gameDuration'] > 900:   # 게임시간이 15분이 안넘을경우에는 패스하기
                for j in range(10):
                    tmp = []
                    tmp.append(df.iloc[i].tier)
                    tmp.append(df.iloc[i].match_id)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['gameVersion'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId']) #1~10번까지 플레이어
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])# 흐웨이 추가완료
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champLevel']) #카사딘 레벨 알기위한 컬럼
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience']) #겜끝났을때 경험치
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition']) # 라인
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId']) # 100 ,200
                    
                    if j < 5: # 각각 어느캐릭터를 밴했는지 확인하기 위해서 0~4배열 확인               
                        tmp.append(df.iloc[i].matches['info']['teams'][0]['bans'][j]['championId'])
                    else:
                        tmp.append(df.iloc[i].matches['info']['teams'][1]['bans'][j-5]['championId'])


                    tmp.append(df.iloc[i].timelines['info']['frames'][1]['events'][0]['participantId']) #가장빨리 템을 산사람이 이기는거 확인                                    
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['gameEndedInEarlySurrender']) # 15분 서렌 맨탈확인용  
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['firstBloodKill'])  # 첫번째 킬 누가 했는지 확인       

                    tmp.append(df.iloc[i].matches['info']['participants'][j]['doubleKills']) 
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['tripleKills']) 
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['quadraKills']) 
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['pentaKills']) 



                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])




                    tmp.append(df.iloc[i].matches['info']['participants'][j]['challenges']['kda']) #kda값 가지고오기
                    
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions']) #상대챔프에게 가한 데미지
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken']) #총 탱킹량

            #timeline 관련된 데이터
                    try:  
                        tmp.append(df.iloc[i].timelines['info']['frames'][10]['participantFrames'][str(j+1)]['totalGold']) #10분 골드량
                    except:
                        tmp.append(0)
                    try:  
                        tmp.append(df.iloc[i].timelines['info']['frames'][15]['participantFrames'][str(j+1)]['totalGold']) #15분 골드량 // 맨탈 확인용
                    except:
                        tmp.append(0)
                    try:  
                        tmp.append(df.iloc[i].timelines['info']['frames'][20]['participantFrames'][str(j+1)]['totalGold']) #20분 골드량
                    except:
                        tmp.append(0)
                    try:  
                        tmp.append(df.iloc[i].timelines['info']['frames'][25]['participantFrames'][str(j+1)]['totalGold']) #25분 골드량
                    except:
                        tmp.append(0)    

 
                    df_creater.append(tmp)
                  
        except:
            print(i)
            continue
    columns = ['tier','gameId','gameDuration','gameVersion','summonerName','summonerLevel','participantId','championName','champLevel','champExperience',
    'teamPosition','teamId','ban', 'firstpurchased', 'win','gameEndedInEarlySurrender','firstBloodKill','doubleKills','tripleKills','quadraKills','pentaKills','kills','deaths','assists','kda','totalDamageDealtToChampions','totalDamageTaken','g_10','g_15','g_20','g_25']
    df = pd.DataFrame(df_creater,columns = columns).drop_duplicates()
    print('df 제작이 완료되었습니다. 현재 df의 수는 %d 입니다'%len(df))
    return df    
    #총 칼럼수 31~32 필요한 칼럼으로 전칼럼 변경완료


def insert_matches_timeline_mysql(row,conn):
    # lambda를 이용해서 progress_apply를 통해 insert할 구문 만들기
    query = (
             f'insert into lol_datas(tier, gameId, gameDuration, gameVersion, summonerName, summonerLevel, participantId,'
             f'championName, champLevel, champExperience, teamPosition, teamId,ban, firstpurchased, win, gameEndedInEarlySurrender, firstBloodKill,doubleKills,tripleKills,quadraKills,pentaKills, kills, deaths, assists,kda,'
             f'totalDamageDealtToChampions, totalDamageTaken,'
             f'g_10, g_15, g_20, g_25)'#우리조가 원하는 분당 총 골드량
             f'values(\'{row.tier}\',\'{row.gameId}\',{row.gameDuration}, \'{row.gameVersion}\', \'{row.summonerName}\','
             f'{row.summonerLevel}, {row.participantId},\'{row.championName}\',\'{row.champLevel}\' ,\'{row.champExperience}\','
             f'\'{row.teamPosition}\', {row.teamId},\'{row.ban}\', \'{row.firstpurchased}\', \'{row.win}\',\'{row.gameEndedInEarlySurrender}\',\'{row.firstBloodKill}\',\'{row.doubleKills}\', \'{row.tripleKills}\', \'{row.quadraKills}\', \'{row.pentaKills}\',  {row. kills}, {row.deaths}, {row.assists},{row.kda},'
             f'{row.totalDamageDealtToChampions},{row.totalDamageTaken},'
             f'{row.g_10},{row.g_15},{row.g_20},{row.g_25})'
           
             f'ON DUPLICATE KEY UPDATE '
             f'tier = \'{row.tier}\', gameId = \'{row.gameId}\', gameDuration = {row.gameDuration}, gameVersion = \'{row.gameVersion}\', summonerName= \'{row.summonerName}\','
             f'summonerLevel = {row.summonerLevel},participantId = {row.participantId},championName = \'{row.championName}\', champLevel = \'{row.champLevel}\','
             f'champExperience = {row.champExperience}, teamPosition = \'{row.teamPosition}\', teamId = {row.teamId} ,ban = \'{row.ban}\', firstpurchased = \'{row.firstpurchased}\', win = \'{row.win}\', gameEndedInEarlySurrender = \'{row.gameEndedInEarlySurrender}\','
             f'firstBloodKill = \'{row. firstBloodKill}\',doubleKills = \'{row. doubleKills}\',tripleKills = \'{row. tripleKills}\',quadraKills = \'{row. quadraKills}\',pentaKills = \'{row. pentaKills}\', kills = {row. kills}, deaths = {row.deaths}, assists = {row.assists}, kda = {row.kda},     totalDamageDealtToChampions = {row.totalDamageDealtToChampions},'
             f'totalDamageTaken = {row.totalDamageTaken},'
             f'g_10 = {row.g_10},g_15 = {row.g_15},g_20 = {row.g_20},g_25 = {row.g_25}'
            )
    sql_execute(conn,query)
    return query

def deletecheck() :#만든 전처리 과정용

    column_list = ['gameId', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel', 'participantId','championName', 'champLevel', 'champExperience', 
                'teamPosition', 'teamId','ban', 'firstpurchased', 'win', 'gameEndedInEarlySurrender', 'firstBloodKill','doubleKills','tripleKills','quadraKills',
                'pentaKills', 'kills', 'deaths', 'assists','kda','totalDamageDealtToChampions', 'totalDamageTaken']


    conn = connect_mysql()

    # 전체 데이터 중에서 빈칸을 전부다 삭제
    for i in range(len(column_list)):
        sql = (
                f"select * from lol_datas where {column_list[i]} = '' and {column_list[i]} != 0 "
            )
        print(sql)
        result = sql_execute(conn,sql)

        print("빈칸이 있는 데이터의 수는 총 {}개 입니다.".format(len(result)))
        
        if result != '' :
            for j in range(len(result)):
                
                sql1 = f"delete from lol_datas where gameId = {result[j].gameId}"
                print(result[j].gameId)
                result = sql_execute(conn,sql1)
                print("삭제 완료된 데이터의 수는 총 {}개 입니다.".format(len(j)))


    # Gameid가 10개가 아닌 게임번호를 삭제
    sql3 = (
            f"select gameId, count(gameId)  from lol_datas group by gameId having count(gameId) != 10 "    
            )

    result = sql_execute(conn,sql3)

    print("칼럼이 10개단위가 아닌 데이터의 수는 총 {}개 입니다.".format(len(result)))
    for i in range(len(result)):
        print(result[i][0])
        sql2 = (
            f"delete from lol_datas where gameId = \'{result[i][0]}\'"
            )
        sql_execute(conn,sql2)

    conn.commit()

    print("삭제 완료되었습니다.")
    conn.close()









    