import glob
import os

from ExcelHandler import ExcelHandler
import Utilities
import time
from DataBase import DB
import ExcelToPDF
import pandas as pd

directory = os.path.abspath('.')
path = directory + "\\Input\\*.xlsx"
list_of_files = glob.glob(path)
print(list_of_files)
for file in list_of_files:
    if str(file).__contains__("~"):  # TO SKIP TEMP EXCEL FILES
        continue

    # TO CALCULATE EXECUTION TIME
    start_time = time.time()

    # TASK 1 READ THE EXCEL FILE:
    InputFileName = file
    excelHandler = ExcelHandler(fileName=InputFileName)
    pdfFileName = excelHandler.getCellFromSheet(sheet='Cover page', cell='B5')
    tablePostFix = '_{0}'.format(pdfFileName)

    db = DB(landing_db='landing_db' + tablePostFix, relational_db='relational_db' + tablePostFix,
            s2t_mapping='s2t_mapping' + tablePostFix, ref_dictionary='ref_dictionary' + tablePostFix)

    db.printRelationalDB()


excelHandler.saveSpreadSheet(fileName=InputFileName)
