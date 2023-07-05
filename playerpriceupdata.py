import requests
import re
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import asyncio
from functools import partial
import pymongo
import json
from dotenv import load_dotenv
import os

load_dotenv()


def addprice(element):
    numbers = re.findall(r"[0-9,]+", element)
    for idx in range(2, 22, 2):
        coll.update_one(
            {"_id": int(numbers[0])},
            {
                "$set": {
                    "bp{}".format(numbers[idx]): int(numbers[idx + 1].replace(",", ""))
                }
            },
        )


async def requestsseson(season, pay, skillmove):
    loop = asyncio.get_event_loop()
    request = partial(
        requests.post,
        "https://fifaonline4.nexon.com/datacenter/PlayerList",
        data={
            "strSeason": ",{},".format(season),
            "n1SkillMove": skillmove,
            "n4SalaryMin": pay,
            "n4SalaryMax": pay,
        },
    )
    res = await loop.run_in_executor(None, request)
    soup = bs(res.text, "html.parser")
    try:
        elements = soup.select("div.td_ar_bp")
        for i in range(0, len(elements)):
            addprice(str(elements[i]))
    except Exception as e:
        print("===", season, pay, skillmove, "=== error", e)
        return
    print("===", season, pay, skillmove, "=== done")


async def main():
    with open("season.json", encoding="UTF-8") as f:
        season_json = json.load(f)
    print("===Json 연결완료===")
    futures = []
    for pay in range(6, 30):
        for skillmove in range(1, 6):
            futures += [
                asyncio.ensure_future(requestsseson(season["seasonId"], pay, skillmove))
                for season in season_json
            ]
    await asyncio.gather(*futures)


if __name__ == "__main__":
    conn = pymongo.MongoClient(os.environ.get("DATABASE_URL"))
    db = conn.get_database("fifa4sim")
    coll = db.get_collection("players")
    print("===DB 연결완료===")

    for i in range(1, 11):
        coll.update_many(
            {str(i) + "강": {"$exists": True}}, {"$unset": {str(i) + "강": True}}
        )

    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    end = time.time()
    print(f"time taken: {end-start}")
