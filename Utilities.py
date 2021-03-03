from datetime import datetime
import uuid
from random import randint


def getCurrentTime():
    # datetime object containing current date and time
    now = datetime.now()
    # FORMAT: dd/mm/YY H:M:S
    return now.strftime("%d/%m/%Y %H:%M:%S")


def getBatchID():
    # logID = uuid.uuid1()
    # RANDOM -> DAY + MONTH + 4 DIGIT RANDOM NUMBER
    now = datetime.now()
    logID = str(now.strftime("%d%m")) + str(randint(1000, 9999))
    return str(logID)
