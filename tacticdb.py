import pymongo

import json

import pandas as pd

import os

temp_list = []


def maketactic_db(tacticjson, seasonjson):
    temp_dict = {}
    temp_dict["_id"] = tacticjson["squadName"]
    temp_dict["formation"] = tacticjson["formation"]
    temp_dict["pad"] = tacticjson["pad"]
    temp_dict["players"] = []

    path = "D:/dampi/public/players"
    file_list = os.listdir(path)

    path2 = "D:/dampi/public/playersAction"
    file_list2 = os.listdir(path2)

    for playerkey in range(0, 11):
        temp_dict2 = {}
        if "p"+str(tacticjson["players"][playerkey]["spid"])+".webp" in file_list2:
            temp_dict2["imgsrc"] = "playersAction/p" + \
                str(tacticjson["players"][playerkey]["spid"])+".webp"
        elif "p"+str(tacticjson["players"][playerkey]["spid"] % 1000000)+".webp" in file_list:
            temp_dict2["imgsrc"] = "players/p" + \
                str(tacticjson["players"][playerkey]["spid"] % 1000000)+".webp"
        else:
            temp_dict2["imgsrc"] = "players/not_found.webp"

        temp_dict2["role"] = tacticjson["players"][playerkey]["role"]
        temp_dict2["id"] = tacticjson["players"][playerkey]["spid"]
        temp_dict2["name"] = tacticjson["players"][playerkey]["name"]
        temp_dict2["season"] = tacticjson["players"][playerkey]["season"]
        temp_dict["players"].append(temp_dict2)
    temp_list.append(temp_dict)


if __name__ == "__main__":
    conn = pymongo.MongoClient("mongodb://localhost:27017")
    db = conn.get_database("fifa4sim")
    coll = db.get_collection("tactics")
    print("===DB 연결완료===")

    with open("tactic.json", encoding="UTF-8") as f:
        tacticjson = json.load(f)
    print("===json로드 완료===")

    with open("season.json", encoding="UTF-8") as f:
        seasonjson = json.load(f)
    print("===json로드 완료===")

    for tempjson in tacticjson:
        maketactic_db(tempjson, seasonjson)

    tempdf = pd.DataFrame(temp_list)
    coll.insert_many(tempdf.to_dict("records"))
