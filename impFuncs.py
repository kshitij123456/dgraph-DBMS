import math
import numpy as np
import json
import uuid

with open("dataset.json") as f:
    data = json.load(f)


def phonenumber():
    return str(math.floor(1000000000 + np.random.rand(1) * 9000000000))


def userid():
    return str(uuid.uuid4().hex)


def ageFunc():
    return (math.floor(np.random.rand(1) * 111) + 10)


def name():

    firstNames = data["firstNames"]
    lastNames = data["lastNames"]

    fname = firstNames[math.floor(np.random.rand(1) * len(firstNames))]
    lname = lastNames[math.floor(np.random.rand(1) * len(lastNames))]

    return {"fname": fname, "lname": lname}
