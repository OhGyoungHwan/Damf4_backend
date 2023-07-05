import requests
import re
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import asyncio
from functools import partial
import pymongo
import json
import os
from PIL import Image

import os
from dotenv import load_dotenv

load_dotenv()

path = "D:/players"
file_list = os.listdir(path)

path2 = "D:/playersAction"
file_list2 = os.listdir(path2)

conn = pymongo.MongoClient(os.environ.get("DATABASE_URL"))
db = conn.get_database("fifa4sim")
coll = db.get_collection("players")
print("===DB 연결완료===")
for doc in coll.find():
    if "p" + str(doc["_id"]) + ".webp" in file_list2:
        coll.update_one(
            {"_id": doc["_id"]},
            {"$set": {"imgsrc": "playersAction/p" +
                      str(doc["_id"]) + ".webp"}},
        )
    elif "p" + str(doc["pid"]) + ".webp" in file_list:
        coll.update_one(
            {"_id": doc["_id"]},
            {"$set": {"imgsrc": "players/p" + str(doc["pid"]) + ".webp"}},
        )
    else:
        coll.update_one(
            {"_id": doc["_id"]},
            {"$set": {"imgsrc": "players/not_found.webp"}},
        )
