import requests
import pandas as pd
import pymysql
import random
from tqdm import tqdm
import time

api_key = 'RGAPI-e674eb69-7d34-41d9-adfb-e43ad16950ca' #본인걸로 바꾸기
seoul_api_key = '494e45546661707037397647657252'


def get_df(url):
    url_re = url.replace('(인증키)', seoul_api_key).replace('xml', 'json').replace('/5/', '/1000/')
    res = requests.get(url_re).json()
    key = list(res.keys())[0]
    df = pd.DataFrame(res[key]['row'])
    return df

def connect_mysql(db='mydb'):
    conn = pymysql.connect(host='localhost', port=3306,
                           user='root', password='1234',
                           db=db, charset='utf8')
    return conn


def sql_execute(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result


def sql_execute_dict(conn, query):
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    return result


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

def get_rawdata(tier):
    division_list = ['I','II','III','IV']
    lst = []
    page = random.randrange(1,20)
    print('get summonerId....')
    
    for division in tqdm(division_list):
        url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={api_key}'
        res = requests.get(url).json()
        lst += random.sample(res,1) # 테스트시 1 , 추후에 3으로 바꿔두기
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
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champLevel']) #카사딘 레벨 알기위한 컬럼
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])

                    if j<5:
                        tmp.append(df.iloc[i].matches['info']['teams'][0]['bans'][j+1]['championId'])
                    else :
                        tmp.append(df.iloc[i].matches['info']['teams'][1]['bans'][j+1]['championId'])


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
                    
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions']) #총 때린 데미지
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken']) #총 맞은 데미지

            #timeline 관련된 데이터
                    try:  
                        tmp.append(df.iloc[i].timelines['info']['frames'][10]['participantFrames'][str(j+1)]['totalGold']) #10분 골드량
                    except:
                        tmp.append(0)
                    try:  
                        tmp.append(df.iloc[i].timelines['info']['frames'][15]['participantFrames'][str(j+1)]['totalGold']) #15분 골드량
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