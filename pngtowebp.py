from PIL import Image

import os

path = "D:/dampi/public/playersAction"
file_list = os.listdir(path)

for i in file_list:
    im = Image.open(path + "/" + i).convert("RGBA")
    im.save((path + "/" + i).replace("png", "webp"), "webp")
