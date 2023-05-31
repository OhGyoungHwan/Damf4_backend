import requests

from bs4 import BeautifulSoup as bs

import pandas as pd

import pymongo

import re

import time

import asyncio

from functools import partial

df = pd.DataFrame()
temp_list = []


async def seemore(ovr, team, addlist=None):
    if addlist is None:
        addlist = []
    temp_dict = {}
    params = {
        "teamcolorid": team["_id"],
        "n4OvrMax": ovr,
        "n4SalaryMin": 6,
    }
    loop = asyncio.get_event_loop()
    request = partial(
        requests.post,
        "https://fifaonline4.nexon.com/datacenter/PlayerList",
        params=params,
    )
    res = await loop.run_in_executor(None, request)

    soup = bs(res.text, "html.parser")
    elements = soup.select("a.btn_preview")
    addlist += list(
        map(lambda x: int(re.findall(
            r"[0-9,]+", str(x))[0]) % 1000000, elements)
    )

    elements = soup.select("div.info_middle")
    temp_ovr_list = list(
        map(lambda x: int(re.findall(r"[0-9,]+", str(x))[1]), elements)
    )

    if len(temp_ovr_list) >= 200:
        print("more")
        await seemore(temp_ovr_list[-1], team, addlist)
        return

    temp_dict["_id"] = team["_id"]
    temp_dict["pids"] = list(set(addlist))
    temp_dict["category"] = team["category"]
    temp_dict["type"] = team["type"]
    temp_dict["name"] = team["name"]
    temp_dict["ability"] = team["ability"]

    temp_list.append(temp_dict)

    print("done:", ovr, ":", team["_id"])


async def maketeamxpid_db():
    futures = []
    futures += [
        asyncio.ensure_future(seemore(200, teamdict))
        for teamdict in list(coll.find())
    ]
    await asyncio.gather(*futures)
    tempdf = pd.DataFrame(temp_list)
    coll2.insert_many(tempdf.to_dict("records"))


if __name__ == "__main__":
    conn = pymongo.MongoClient("mongodb://localhost:27017")
    db = conn.get_database("fifa4sim")
    coll = db.get_collection("teamcolors")
    coll2 = db.get_collection("teamxpid")

    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(maketeamxpid_db())
    end = time.time()
    print(f"time taken: {end-start}")
