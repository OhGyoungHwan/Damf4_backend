import requests

from bs4 import BeautifulSoup as bs

import pandas as pd

import pymongo

from dotenv import load_dotenv
import os

load_dotenv()

kor_to_rng = {
    "속력": "sprintspeed",
    "가속력": "acceleration",
    "골 결정력": "finishing",
    "슛 파워": "shotpower",
    "중거리 슛": "longshots",
    "위치 선정": "positioning",
    "발리 슛": "volleys",
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
    "전체 능력치": "all",
}


df = pd.DataFrame()
concatlist = []


def scrapteamcolor(option1, option2):
    res = requests.get(
        "https://fifaonline4.nexon.com/datacenter/teamcolor?strTeamColorCategory=%2C"
        + option1
        + "%2C&strTeamColorType=%2C"
        + option2
        + "%2C&strTeamColorName="
    )
    soup = bs(res.text, "html.parser")

    elements = soup.select("div.teamcolor_item")
    for i in elements:
        temp_dict = {}
        temp_dict["category"] = option1
        temp_dict["type"] = option2
        soup2 = bs(str(i), "html.parser")

        temp = soup2.select_one("a.btn_detail_link")["onclick"]
        temp_dict["_id"] = int(
            str(temp)
            .replace("DataCenter.GetTeamColorDetail(", "")
            .replace("); return false;", "")
        )

        temp = soup2.select_one("div.name")
        temp_dict["name"] = temp.text

        temp = soup2.select("div.desc > span")
        temp_dict2 = {}
        for i in temp:
            temp_str_list = i.text.split(" +")
            temp_dict2[kor_to_rng[temp_str_list[0]]] = int(temp_str_list[1])
        if option1 == "affiliation":
            if soup2.select_one("div.level").text == "3단계":
                temp_dict2["all"] = 3
            else:
                temp_dict2["all"] = 4
        temp_dict["ability"] = temp_dict2
        concatlist.append(temp_dict)


if __name__ == "__main__":
    conn = pymongo.MongoClient(os.environ.get("DATABASE_URL"))
    db = conn.get_database("fifa4sim")
    coll = db.get_collection("teamcolors")
    print("===DB 연결완료===")

    for option1 in ["feature", "affiliation"]:
        if option1 == "feature":
            scrapteamcolor(option1, "relation")
        else:
            for option2 in ["club", "nation", "special"]:
                scrapteamcolor(option1, option2)
    tempdf = pd.DataFrame(concatlist)
    coll.insert_many(tempdf.to_dict("records"))
