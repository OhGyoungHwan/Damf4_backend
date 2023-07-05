import requests
import pymongo
from bs4 import BeautifulSoup as bs
import asyncio
import pandas as pd
import time
from functools import partial
import re
import json
import datetime as dt
from dotenv import load_dotenv
import os

load_dotenv()

seaseonlist = [
    "22UCL",
    "21UCL",
    "23TOTN",
    "ICON",
    "BWC",
    "WC22",
    "2012KH",
    "HG",
    "2012KH",
    "LN",
    "ICONTM",
    "TKL",
    "20UCL",
    "BTB",
    "22KLB",
]

seaseonlist_newest = [
    "23TOTS",
    "ICONTM",
    "HG",
    "RTN",
    "23HR",
    "22UCL",
    "23TOTY",
    "23TOTN",
    "22KLB",
    "22NG",
    "RMCF",
    "WC22",
    "BWC",
    "SPL",
    "LN",
]

postionlist = [
    "ST",
    "CF",
    "LW",
    "RW",
    "CM",
    "CAM",
    "CDM",
    "LM",
    "RM",
    "CB",
    "LB",
    "RB",
    "LWB",
    "RWB",
    "GK",
]

postiondict = {
    "ST": 25,
    "CF": 21,
    "LW": 27,
    "RW": 23,
    "CM": 14,
    "CAM": 18,
    "CDM": 10,
    "LM": 16,
    "RM": 12,
    "CB": 5,
    "LB": 7,
    "RB": 3,
    "LWB": 8,
    "RWB": 2,
    "GK": 0,
    None: None,
}

all_recommend_list = []
all_tactic_list = []


async def get_recommend(order, categories, strClass=None, n4Position=None):
    global all_recommend_list
    tag = ""
    url = ""
    if strClass is None:
        tag = n4Position
        url = "https://fifaonline4.nexon.com/Datacenter/BestUsePositionPlayer"
    else:
        tag = strClass
        url = "https://fifaonline4.nexon.com/Datacenter/BestUseClassPlayer"
    # 현재시간
    now_time = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y.%m.%d")
    # get요청 params조정
    params = {
        "strDate": now_time,
        "strClass": strClass,
        "n4Position": postiondict[n4Position],
        "n4StartRanking": 1,
        "n4EndRanking": 10000,
    }
    # request
    loop = asyncio.get_event_loop()
    request = partial(
        requests.get,
        url,
        params,
    )
    res = await loop.run_in_executor(None, request)
    soup = bs(res.text, "html.parser")
    # request 활용 DB선수 데이터 찾기
    elements = soup.select("a")
    numbers = []
    for i in range(0, len(elements)):
        numbers.append(
            coll2.find_one(
                {"_id": int(re.sub(r"[^0-9]", "", str(elements[i]))[:-2])},
                {
                    "_id": 1,
                    "season": 1,
                    "name": 1,
                    "role": "",
                    "imgsrc": 1,
                },
            )
        )
    all_recommend_list.append(
        {
            "players": numbers,
            "_id": tag + categories,
            "tag": tag,
            "categories": categories,
            "order": order,
        }
    )


def get_tactic(order, tacticjson):
    global all_tactic_list
    temp_dict = {}
    temp_dict["_id"] = tacticjson["squadName"] + tacticjson["pad"]
    temp_dict["tag"] = tacticjson["squadName"]
    temp_dict["categories"] = tacticjson["pad"]
    temp_dict["order"] = order
    temp_dict["players"] = []

    path = "C:/Users/rudgh/Desktop/players"
    file_list = os.listdir(path)

    path2 = "C:/Users/rudgh/Desktop/playersAction"
    file_list2 = os.listdir(path2)

    for playerkey in range(0, 11):
        temp_dict2 = {}
        if "p" + str(tacticjson["players"][playerkey]["spid"]) + ".webp" in file_list2:
            temp_dict2["imgsrc"] = (
                "playersAction/p"
                + str(tacticjson["players"][playerkey]["spid"])
                + ".webp"
            )
        elif (
            "p" + str(tacticjson["players"][playerkey]
                      ["spid"] % 1000000) + ".webp"
            in file_list
        ):
            temp_dict2["imgsrc"] = (
                "players/p"
                + str(tacticjson["players"][playerkey]["spid"] % 1000000)
                + ".webp"
            )
        else:
            temp_dict2["imgsrc"] = "players/not_found.webp"

        temp_dict2["role"] = tacticjson["players"][playerkey]["role"]
        temp_dict2["id"] = tacticjson["players"][playerkey]["spid"]
        temp_dict2["name"] = tacticjson["players"][playerkey]["name"]
        temp_dict2["season"] = tacticjson["players"][playerkey]["season"]
        temp_dict["players"].append(temp_dict2)

    all_tactic_list.append(temp_dict)


async def main():
    with open("tactic.json", encoding="UTF-8") as f:
        tacticList = json.load(f)
    print("===json로드 완료===")

    futures = [
        asyncio.ensure_future(
            get_recommend(order=i, n4Position=postionlist[i], categories="포지션")
        )
        for i in range(len(postionlist))
    ]
    futures += [
        asyncio.ensure_future(
            get_recommend(order=i, strClass=seaseonlist[i], categories="시즌")
        )
        for i in range(len(seaseonlist))
    ]

    futures += [
        asyncio.ensure_future(
            get_recommend(
                order=i,
                strClass=seaseonlist_newest[i],
                categories="신규 시즌",
            )
        )
        for i in range(len(seaseonlist_newest))
    ]

    await asyncio.gather(*futures)

    for i in range(len(tacticList)):
        get_tactic(i, tacticList[i])

    recommenddf = pd.DataFrame(all_recommend_list)

    for recommend in recommenddf.to_dict("records"):
        coll.update_one({"_id": recommend["_id"]}, {
                        "$set": recommend}, upsert=True)
        print(recommend["_id"], "-> done")

    tacticdf = pd.DataFrame(all_tactic_list)

    for tactic in tacticdf.to_dict("records"):
        coll.update_one({"_id": tactic["_id"]}, {"$set": tactic}, upsert=True)
        print(tactic["_id"], "-> done")


if __name__ == "__main__":
    conn = pymongo.MongoClient(os.environ.get("DATABASE_URL"))
    db = conn.get_database("fifa4sim")
    coll = db.get_collection("recommend")
    coll2 = db.get_collection("players")
    print("===DB 연결완료===")

    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    end = time.time()
    print(f"time taken: {end-start}")
