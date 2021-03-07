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

    db.createDynamicTable(tableName=db.s2t_mapping, columns=s2tColumns)
    db.createDynamicTable(tableName=db.relational_db, columns=relationalColumns)
    db.createDynamicTable(tableName=db.landing_db, columns=landingDBColumns)
    db.createDynamicTable(tableName=db.ref_dictionary, columns=refDictionaryColumns)

    isFirstRun = not (db.getNumberOfRecords(tableName=db.s2t_mapping) > 0)


    lastRow = excelHandler.getMaxRow(sheet='Landing DB') + 1

    BatchID = Utilities.getBatchID()
    currentTime = Utilities.getCurrentTime()

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

        if  isFirstRun:
            db.insertDynamicTable(tableName=db.s2t_mapping, columns=s2tColumns, values=currentRowData)

        source_data = excelHandler.getCellFromSheet(sheet=sheet_source, cell=cell_source)
        target_data = excelHandler.getCellFromSheet(sheet=sheet_target, cell=cell_target)

        print("============= ROW NUMBER: ", rowNumber)
        print("UNIQUE ID: ", str(BatchID), " DATE AND TIME =", currentTime)
        print("SOURCE DATA: ", source_data)
        print("============================")

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

            # db.insertIntoLandingDB(sheetSource=sheet_source, cellSource=cell_source, cellContent=source_data,
            #                        TimeStamp=currentTime, BatchID=BatchID)

            db.insertDynamicTable(tableName=db.landing_db, columns=landingDBColumns, values= [sheet_source,cell_source, source_data, currentTime , str(BatchID) ])

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

        db.insertDynamicTable(tableName=db.relational_db , columns=relationalColumns , values=currentRowData)

    if isFirstRun:
        for rowNumber in range(2, excelHandler.getMaxRow(sheet='Ref_Dictionary') + 1):
            currentRowData = excelHandler.getRowDataFromSheet(sheet='Ref_Dictionary', row=rowNumber)
            db.insertDynamicTable(tableName=db.ref_dictionary , columns=refDictionaryColumns , values=currentRowData)


    excelHandler.saveSpreadSheet(fileName=InputFileName)

    ExcelToPDF.excelToPDF(pdfFileName=pdfFileName, fileName=InputFileName)

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

    formattedTime = str(currentTime).replace('/', '-').replace(':', '').replace(' ', '_')
    outputFile = 'Validation_{0}_{1}.xlsx'.format(pdfFileName, formattedTime)
    excelHandler.creatWorkBook(outputFile)
    excelHandlerForOutput = ExcelHandler(fileName=outputFile)

    # GET TABLES FROM DB INTO PANDAS DATAFRAME
    df_input, df_old = db.relationalDF(selctedTable=db.relational_db, time_stamp=currentTime)
    ref_dict = db.getTableToDF(selctedTable=db.ref_dictionary)
    print(df_input)

    # PREPROCESS DATA
    a = Preprocess(df_input)
    input = a.getPrepTable()

    tableRules = tableChecks.Table(input, ref_dict)
    df_pass, df_fail = tableRules.getPassFail()
    print(df_fail.columns)

    # OUTPUT GOOD AND BAD ROWS
    excelHandlerForOutput.saveDFtoExcel('fail', df_fail)
    excelHandlerForOutput.saveDFtoExcel('pass', df_pass)

    # GET MIN MAX
    b = Reports(input,'CL_AGE_GROUP_EN_V1')
    min_max = b.minmax()
    excelHandlerForOutput.saveDFtoExcel('min_max', min_max)

    # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
    diff = b.changes()
    excelHandlerForOutput.saveDFtoExcel('changes', diff)

    # FREQ CHECK
    freq = b.frequency()
    excelHandlerForOutput.saveDFtoExcel('frequency', freq)

    # GET TOTALS REPORT
    total = b.totals_new()
    excelHandlerForOutput.saveDFtoExcel('total', total)

    # PRED DISCREPANCIES CHECK
    PredDisc = b.getPredDiscrepancies(df_old)
    excelHandlerForOutput.saveDFtoExcel('predecessor discrepancies', PredDisc)

    excelHandlerForOutput.closeWriter()
    db.closeConnection()