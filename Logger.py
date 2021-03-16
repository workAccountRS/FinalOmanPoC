import os
from datetime import datetime
import inspect
import logging


def customLogger(logLevel, name=""):
    # Gets the name of the class / method from where this method is called
    loggerName = inspect.stack()[1][3]
    logger = logging.getLogger(loggerName)
    # By default, log all messages
    logger.setLevel(logging.DEBUG)
    print(inspect.stack()[1][3])
    if str(loggerName).__contains__('<'):
        loggerName = "TEST"



    now = (str(datetime.now().time()).replace(":", "_"))[:5]
    directory = os.path.abspath('.')
    path = directory  + "\\"
    fileHandler = logging.FileHandler(path + "{0}_{1}_{2}.log".format(loggerName, now, name), mode='w')
    fileHandler.setLevel(logLevel)
    print(str(path)  + "{0}_{1}_{2}.log".format(loggerName, now, name))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')

    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    logging.addLevelName(51, 'GENERAL')

    return logger
