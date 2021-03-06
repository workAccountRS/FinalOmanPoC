import glob
import os

from ExcelHandler import ExcelHandler
import Utilities
import time
from DataBase import DB
import ExcelToPDF

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

    # COLUMNS TO CREATE DYNAMIC TABLES
    s2tColumns = excelHandler.getRowDataFromSheet(sheet='S2T Mapping', row=1)
    relationalColumns = excelHandler.getRowDataFromSheet(sheet='Relational DB', row=2)
    refDictionaryColumns = excelHandler.getRowDataFromSheet(sheet='Ref_Dictionary', row=1)
    landingDBColumns = excelHandler.getRowDataFromSheet(sheet='Landing DB', row=1)

    isFirstRun = not (db.getNumberOfRecords(tableName=db.s2t_mapping) > 0)

    lastRow = excelHandler.getMaxRow(sheet='Landing DB') + 1

    BatchID = Utilities.getBatchID()
    currentTime = '08/03/2021 20:02:20'

    skipedRows = []
    errors = []

    # LOOP THROUGH THE MAP
    for rowNumber in range(2, excelHandler.getMaxRow(sheet='S2T Mapping') + 1):
        currentRowData = excelHandler.getRowDataFromSheet(sheet='S2T Mapping', row=rowNumber)
        sheet_source = currentRowData[0]
        cell_source = currentRowData[1]
        sheet_target = currentRowData[2]
        cell_target = currentRowData[3]
        cell_type = currentRowData[4]
        desc_ar = currentRowData[5]
        dataType = currentRowData[6]
        is_mandatory = currentRowData[7]
        Ref_Dictionary = currentRowData[8]

        if sheet_target == 'NA':
            skipedRows.append(rowNumber)
            continue

        source_data = excelHandler.getCellFromSheet(sheet=sheet_source, cell=cell_source)
        target_data = excelHandler.getCellFromSheet(sheet=sheet_target, cell=cell_target)

        # TASK 2 FILL THE LANDING DB:
        # Sheet_Source | Cell_Source | Cell_Content	| Time_Stamp | Batch_ID

        # WRITING ON A EXCEL FILE (Write Test)
        if True:
            # WRITE FROM SOURCE TO TARGET
            excelHandler.writeCell(sheet='Target Table', cell=str(cell_target), value=source_data)

            # WRITE TO LANDING DB
            # GET MAX COLUMN

            # LOOP FROM A TO LAST COL
            excelHandler.writeCell(sheet='Landing DB', cell=str('A' + str(lastRow)), value=sheet_source)
            excelHandler.writeCell(sheet='Landing DB', cell=str('B' + str(lastRow)), value=cell_source)
            excelHandler.writeCell(sheet='Landing DB', cell=str('C' + str(lastRow)), value=source_data)
            excelHandler.writeCell(sheet='Landing DB', cell=str('D' + str(lastRow)), value=currentTime)
            excelHandler.writeCell(sheet='Landing DB', cell=str('E' + str(lastRow)), value=str(BatchID))

        # except Exception as e:
        #     print("ERROR IN ROW#" + str(rowNumber) + " -- " + str(e))
        #     errors.append(['ROW NUMBER:' + str(rowNumber), 'CELL SOURCE:' + cell_source, 'CELL TARGET:' + cell_target , 'ERROR: ' + str(e)]  )

        # NEXT ROW TO WRITE
        lastRow += 1

    for rowNumber in range(4, excelHandler.getMaxRow(sheet='Relational DB') + 1):
        # TIME AND BATCH ID

        cell = excelHandler.getCellCoordinate(sheet='Relational DB')
        excelHandler.writeCell(sheet='Relational DB', cell=str(str(cell[0]) + str(rowNumber)), value=currentTime)
        excelHandler.writeCell(sheet='Relational DB', cell=str(str(cell[1]) + str(rowNumber)), value=str(BatchID))

        currentRowData = excelHandler.getRowDataFromSheet(sheet='Relational DB', row=rowNumber)

    excelHandler.saveSpreadSheet(fileName=InputFileName)

    # ExcelToPDF.excelToPDF(pdfFileName=pdfFileName, fileName=InputFileName)

    # db.printDescription()
    # db.printLandingDB()
    # db.printS2t()
    # db.printRelationalDB()

    # ========================== RESULTS

    print("ERRORS IN ROWS: ", errors)
    print("SKIPPED ROWS: ", skipedRows)

    # TO CALCULATE EXECUTION TIME
    print("--- Took %s seconds to process ---" % (time.time() - start_time))

    # ########################################################################################################################

    # TODO: IMPORTS FIRST
    from Preprocess import Preprocess
    from Reports import Reports
    import tableChecks

    import pandas as pd

    pd.set_option('display.max_rows', None)

    currentTime = '11/03/2021 12:51:36'

    formattedTime = str(currentTime).replace('/', '-').replace(':', '').replace(' ', '_')
    outputFile = 'Validation_{0}_{1}.xlsx'.format(pdfFileName, formattedTime)
    excelHandler.creatWorkBook(outputFile)
    excelHandlerForOutput = ExcelHandler(fileName=outputFile)

    # GET TABLES FROM DB INTO PANDAS DATAFRAME
    df_curr, df_old = db.relationalDF(selctedTable=db.relational_db, time_stamp=currentTime)
    ref_dict = db.getTableToDF(selctedTable=db.ref_dictionary)

    # PREPROCESS ALL DF (STRIP FROM EXTRA WHITE SPACE AND REMOVE ARABIC SPECIAL CHARACTERS)
    print('____________________________INITAIL PREP____________________________')
    prep = Preprocess()
    df_curr = prep.initialPrep(df_curr)
    df_old = prep.initialPrep(df_old)
    ref_dict = prep.initialPrep(ref_dict)

    # PREP DATES AND NUMBER VALUES
    print('____________________________date and obs prep____________________________')
    relational_data = prep.prepDatesAndValues(df_curr)
    reports = Reports(relational_data)

    # PRED DISCREPANCIES CHECK
    print('____________________________PredDisc____________________________')
    PredDisc = prep.getPredDiscrepancies(df_curr, df_old)
    excelHandlerForOutput.saveDFtoExcel('predecessor discrepancies', PredDisc)

    # PRED DISCREPANCIES CHECK
    print('____________________________PredDisc____________________________')
    PredDisc = prep.getPredDiscrepancies(df_curr, df_old)
    excelHandlerForOutput.saveDFtoExcel('predecessor discrepancies', PredDisc)

    # GET GOOD AND BAD ROWS AND OUTPUT TO EXCEL
    print('____________________________pass fail____________________________')
    tableRules = tableChecks.Table(relational_data, ref_dict)
    df_pass, df_fail = tableRules.getPassFail()
    excelHandlerForOutput.saveDFtoExcel('fail', df_fail)
    excelHandlerForOutput.saveDFtoExcel('pass', df_pass)

    # GET MIN MAX
    print('____________________________min max____________________________')
    min_max = reports.minmax(ref_dict)
    excelHandlerForOutput.saveDFtoExcel('min_max', min_max)

    # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
    print('____________________________changes____________________________')
    diff, freq = reports.changes()
    excelHandlerForOutput.saveDFtoExcel('frequency', freq)
    excelHandlerForOutput.saveDFtoExcel('changes', diff)

    # GET TOTALS REPORT
    print('____________________________Total____________________________')
    total = reports.totals_new(ref_dict)
    excelHandlerForOutput.saveDFtoExcel('total', total)
    excelHandlerForOutput.closeWriter()

    db.closeConnection()
