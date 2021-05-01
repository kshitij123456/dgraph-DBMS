from impFuncs import *
import json
from pprint import pprint


def data_generator():
    age = ageFunc()
    names = name()
    user_id = userid()
    phNo = phonenumber()

    fname = names["fname"]
    lname = names["lname"]

    return {
        "id": user_id,
        "firstname": fname,
        "lastname": lname,
        "age": age,
        "phonenumber": phNo,
    }
