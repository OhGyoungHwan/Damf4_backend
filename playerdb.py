import requests
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import asyncio
import time
from functools import partial
import pymongo
import numpy as np
import re
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.compose import ColumnTransformer
from dotenv import load_dotenv
import os

load_dotenv()

path = "D:/dampi/public/players"
file_list = os.listdir(path)

path2 = "D:/dampi/public/playersAction"
file_list2 = os.listdir(path2)

kor_to_rng = {
    "속력": "sprintspeed",
    "가속력": "acceleration",
    "골 결정력": "finishing",
    "슛 파워": "shotpower",
    "중거리 슛": "longshots",
    "위치 선정": "positioning",
    "발리슛": "volleys",
    "페널티 킥": "penalties",
    "짧은 패스": "shortpassing",
    "시야": "vision",
    "크로스": "crossing",
    "긴 패스": "longpassing",
    "프리킥": "freekickaccuracy",
    "커브": "curve",
    "드리블": "dribbling",
    "볼 컨트롤": "ballcontrol",
    "민첩성": "agility",
    "밸런스": "balance",
    "반응 속도": "reactions",
    "대인 수비": "marking",
    "태클": "standingtackle",
    "가로채기": "interceptions",
    "헤더": "headingaccuracy",
    "슬라이딩 태클": "slidingtackle",
    "몸싸움": "strength",
    "스태미너": "stamina",
    "적극성": "aggression",
    "점프": "jumping",
    "침착성": "composure",
    "GK 다이빙": "gkdiving",
    "GK 핸들링": "gkhandling",
    "GK 킥": "gkkicking",
    "GK 반응속도": "gkreflexes",
    "GK 위치 선정": "gkpositioning",
    "키": "height",
    "몸무게": "weight",
    "약발": "subfoot",
}


def find_img(spid):
    if "p" + str(spid) + ".webp" in file_list2:
        return "playersAction/p" + str(spid) + ".webp"

    elif "p" + str(spid % 1000000) + ".webp" in file_list:
        return "players/p" + str(spid) + ".webp"

    else:
        return "players/not_found.webp"


def get_recommendations(spid, df_cosine_sim, playerdf):
    # sim_scores 사전 제작
    sim_scores = dict(zip(df_cosine_sim.index, list(df_cosine_sim.loc[spid])))
    sim_scores = dict(
        sorted(sim_scores.items(), key=lambda item: item[1], reverse=True)
    )

    # 닮은 선수 100추출
    sim_scores_list = list(sim_scores.keys())
    sim_scores_list = sim_scores_list[0:100]

    # 닮은 선수에서 본인제외(즉 타시즌 카드 제외)
    poplist = []
    for item in sim_scores_list:
        if (spid % 1000000) == (item % 1000000):
            poplist.append(item)
    for item in poplist:
        sim_scores_list.remove(item)

    # 닮은 선수 60명추출해 추가
    return_sim_scores_list = sim_scores_list[:60]
    playerdf.at[spid, "simplayers"] = return_sim_scores_list


def simplayer(playerdf, weighted_columns, unweighted_columns):
    # 데이터 전처리
    transformer_StandardScaler = ColumnTransformer(
        [
            ("StandardScaler", StandardScaler(), weighted_columns + unweighted_columns),
        ]
    )
    transformerdf_MinMaxScaler = ColumnTransformer(
        [
            ("MinMaxScaler", MinMaxScaler(), weighted_columns + unweighted_columns),
        ]
    )

    # 가중치 속성 2배 => 가중치 속성이 consin에서 큰 비중을 차지하게 된다.
    fitdf = transformer_StandardScaler.fit_transform(playerdf)
    df_StandardScaler = pd.DataFrame(
        fitdf,
        columns=weighted_columns + unweighted_columns,
    )
    fitdf = transformerdf_MinMaxScaler.fit_transform(df_StandardScaler)
    df_MinMaxScaler = pd.DataFrame(
        fitdf,
        columns=weighted_columns + unweighted_columns,
    )

    df_MinMaxScaler[weighted_columns] = df_MinMaxScaler[weighted_columns].mul(2)

    # cosine알고리즘 적용
    cosine_sim = cosine_similarity(df_MinMaxScaler)

    # cosine_sim 데이터프레임 전환
    df_cosine_sim = pd.DataFrame(
        cosine_sim, index=playerdf["_id"], columns=playerdf["_id"]
    )

    # 실제 데이터프레임에 닮은 선수 속성 추가
    for spid in playerdf["_id"]:
        get_recommendations(spid, df_cosine_sim, playerdf)


async def findplayer(spid, concatlist):
    loop = asyncio.get_event_loop()
    request = partial(
        requests.post,
        "https://fifaonline4.nexon.com/datacenter/PlayerPreView",
        data={"spid": spid},
    )
    res = await loop.run_in_executor(None, request)
    soup = bs(res.text, "html.parser")

    temp = {}
    temp["_id"] = spid
    temp["pid"] = spid % 1000000
    temp["imgsrc"] = find_img(spid)

    elements = soup.select_one("div.name")
    temp["name"] = elements.text

    elements = soup.select_one("div.card_back > img")["src"]
    temp["season"] = elements.replace(
        "https://ssl.nexon.com/s2/game/fo4/obt/externalAssets/card/", ""
    ).replace(".png", "")

    elements = soup.select("span.position > span")
    datapostion = [elements[i].text for i in range(0, len(elements), 2)]
    datapostionovr = [elements[i].text for i in range(1, len(elements), 2)]
    json_postion = dict(zip(datapostion, datapostionovr))

    for key in json_postion:
        if json_postion[key] != None:
            temp[key] = int(json_postion[key])

    temp["ovr"] = int(max(json_postion.values()))

    elements = soup.select_one("div.pay")
    temp["pay"] = int(elements.text)

    elements = soup.select_one("span.height")
    temp["height"] = int(elements.text.replace("cm", ""))

    elements = soup.select_one("span.weight")
    temp["weight"] = int(elements.text.replace("kg", ""))

    elements = soup.select_one("span.physical")
    temp["physical"] = elements.text

    elements = soup.select_one("span.skill > span")
    temp["skillmove"] = elements.text

    elements = soup.select_one("span.foot > strong")
    temp["mainfoot"] = elements.text

    elements = soup.select_one("span.foot")
    temp["subfoot"] = int(
        re.sub(
            r"[^0-9]",
            "",
            str(elements.text.replace(temp["mainfoot"], "").replace(" – ", "")),
        )
    )

    elements = soup.select_one("div.nation > span.txt")
    temp["nation"] = elements.text.replace(", 국가대표", "")

    elements = soup.select("div.skill_wrap > span")
    team = [elem.text for elem in elements]
    temp["trait"] = list(set(team))

    elements = soup.select("div.content_bottom > ul > li.ab > div")
    dataname = [elements[i].text for i in range(0, len(elements), 2)]
    datanumber = [elements[i].text for i in range(1, len(elements), 2)]
    json_stats = dict(zip(dataname, datanumber))
    for key in json_stats:
        temp[kor_to_rng[key]] = int(json_stats[key])

    elements = soup.select("div.data_table > ul > li > div.club")
    club = [elem.text for elem in elements]
    temp["club"] = list(set(club))
    concatlist.append(temp)


async def main():
    concatlist = []
    # 피파온라인4 선수데이터 비동기 방식 스크래핑
    futures = [
        asyncio.ensure_future(findplayer(keyword["id"], concatlist)) for keyword in data
    ]
    await asyncio.gather(*futures)

    # 선수사전리스트에서 데이터프레임 변환 및 결측값 처리, GK제외 5급여 이하 선수 처리
    playerdf = pd.DataFrame(concatlist).fillna(0)
    playerdf.set_index("_id", drop=False, inplace=True)
    playerdf["simplayers"] = [[] for r in range(len(playerdf))]

    playerdf_noGK = playerdf.loc[playerdf["GK"] == 0]
    playerdf_noGK = playerdf_noGK.loc[(playerdf_noGK["pay"] > 5)]
    playerdf_GK = playerdf.loc[playerdf["GK"] != 0]

    # 닮은 선수 찾기
    simplayer(
        playerdf_noGK,
        [
            kor_to_rng["속력"],
            kor_to_rng["가속력"],
            kor_to_rng["민첩성"],
            kor_to_rng["밸런스"],
            kor_to_rng["스태미너"],
            kor_to_rng["몸싸움"],
            kor_to_rng["침착성"],
            kor_to_rng["가로채기"],
            kor_to_rng["시야"],
            kor_to_rng["커브"],
            kor_to_rng["슛 파워"],
            kor_to_rng["골 결정력"],
            kor_to_rng["크로스"],
            kor_to_rng["중거리 슛"],
            kor_to_rng["헤더"],
            kor_to_rng["키"],
            kor_to_rng["약발"],
        ],
        [
            kor_to_rng["드리블"],
            kor_to_rng["점프"],
            kor_to_rng["발리슛"],
            kor_to_rng["적극성"],
            kor_to_rng["위치 선정"],
            kor_to_rng["반응 속도"],
            kor_to_rng["볼 컨트롤"],
            kor_to_rng["긴 패스"],
            kor_to_rng["짧은 패스"],
            kor_to_rng["대인 수비"],
            kor_to_rng["태클"],
            kor_to_rng["몸무게"],
        ],
    )

    simplayer(
        playerdf_GK,
        [
            kor_to_rng["키"],
            kor_to_rng["점프"],
        ],
        [
            kor_to_rng["GK 킥"],
            kor_to_rng["GK 다이빙"],
            kor_to_rng["GK 위치 선정"],
            kor_to_rng["반응 속도"],
            kor_to_rng["민첩성"],
            kor_to_rng["GK 반응속도"],
            kor_to_rng["GK 핸들링"],
        ],
    )

    playerdf = pd.concat([playerdf_GK, playerdf_noGK])

    print(playerdf.to_dict("records"))
    for player in playerdf.to_dict("records"):
        coll.update_one({"_id": player["_id"]}, {"$set": player}, upsert=True)
        print(player["_id"], "-> done")


if __name__ == "__main__":
    conn = pymongo.MongoClient(os.environ.get("DATABASE_URL"))
    db = conn.get_database("fifa4sim")
    coll = db.get_collection("players")
    print("===DB 연결완료===")

    with open("playerlist.json", encoding="UTF-8") as f:
        data = json.load(f)

    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    end = time.time()
    print(f"time taken: {end-start}")
