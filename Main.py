from ExcelHandler import ExcelHandler
import Utilities
import time
from DataBase import DB
import ExcelToPDF
import FilesHandling

tamara = False
SavePDF = True

start_time = time.time()

for InputFileName in FilesHandling.getListOfFiles():
    if str(InputFileName).__contains__("~"):  # TO SKIP TEMP EXCEL FILES (Opened Files)
        continue

    current_time = time.time()  # TO CALCULATE EXECUTION TIME

    excelHandler = ExcelHandler(fileName=InputFileName)

    sheetsToPdfList = excelHandler.sheetsAliveList(['Target Table', 'Target Graph', 'Target PT', 'Text'])
    pdfFileName = excelHandler.getCellFromSheet(sheet='Cover page', cell='B5')
    freq = excelHandler.getCellFromSheet(sheet='Cover page', cell='B8').strip().lower()
    tablePostFix = '_{0}'.format(pdfFileName)

    db = DB(landing_db='landing_db' + tablePostFix, relational_db='relational_db' + tablePostFix,
            s2t_mapping='s2t_mapping' + tablePostFix, ref_dictionary='ref_dictionary' + tablePostFix)

    # COLUMNS TO CREATE DYNAMIC TABLES
    s2tColumns = excelHandler.getEntireRowFromSheet(sheet='S2T Mapping', row=1)
    relationalColumns = excelHandler.getEntireRowFromSheet(sheet='Relational DB', row=2)
    relationalColumns.append('Serial_Data_Load')
    refDictionaryColumns = excelHandler.getEntireRowFromSheet(sheet='Ref_Dictionary', row=1)
    landingDBColumns = excelHandler.getEntireRowFromSheet(sheet='Landing DB', row=1)

    isNewTable = db.createDynamicTable(tableName=db.s2t_mapping, columns=s2tColumns)

    if isNewTable:
        db.createDynamicTable(tableName=db.relational_db, columns=relationalColumns)
        db.createDynamicTable(tableName=db.landing_db, columns=landingDBColumns)
        db.createDynamicTable(tableName=db.ref_dictionary, columns=refDictionaryColumns)

    isFirstRun = not (db.getNumberOfRecords(tableName=db.s2t_mapping) > 0)
    timeCount = db.getDistincTime() + 1

    lastRow = excelHandler.getMaxRow(sheet='Landing DB') + 1

    BatchID = Utilities.getBatchID()
    currentTime = Utilities.getCurrentTime()

    print("UNIQUE ID: ", str(BatchID), " DATE AND TIME =", currentTime)

    skippedRows = []
    errors = []

    listOfTuplesS2T = []
    listOfTuplesRelational = []
    listOfTuplesRef = []
    listOfTuplesLanding = []

    # LOOP THROUGH THE MAP
    for rowNumber in range(2, excelHandler.getMaxRow(sheet='S2T Mapping') + 1):
        currentRowData = excelHandler.getEntireRowFromSheet(sheet='S2T Mapping', row=rowNumber)
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
            skippedRows.append(rowNumber)
            continue

        listOfTuplesS2T.append(tuple(currentRowData))

        # if isFirstRun:
        #     db.insertDynamicTable(tableName=db.s2t_mapping, columns=s2tColumns, values=currentRowData)

        source_data = excelHandler.getCellFromSheet(sheet=str(sheet_source), cell=cell_source)
        target_data = excelHandler.getCellFromSheet(sheet=str(sheet_target), cell=cell_target)

        # PRINT EACH ROW
        # print("============= ROW NUMBER: ", rowNumber)
        # print("SOURCE DATA: ", source_data)
        # print("============================")

        try:
            # WRITE FROM SOURCE TO TARGET
            excelHandler.writeCell(sheet='Target Table', cell=str(cell_target), value=source_data)

            excelHandler.writeCell(sheet='Landing DB', cell=str('A' + str(lastRow)), value=sheet_source)
            excelHandler.writeCell(sheet='Landing DB', cell=str('B' + str(lastRow)), value=cell_source)
            excelHandler.writeCell(sheet='Landing DB', cell=str('C' + str(lastRow)), value=source_data)
            excelHandler.writeCell(sheet='Landing DB', cell=str('D' + str(lastRow)), value=currentTime)
            excelHandler.writeCell(sheet='Landing DB', cell=str('E' + str(lastRow)), value=str(BatchID))

            listOfTuplesLanding.append(
                tuple([str(sheet_source), str(cell_source), str(source_data), str(currentTime), str(BatchID)]))
        except Exception as e:
            print("ERROR IN ROW#" + str(rowNumber) + " -- " + str(e))
            errors.append(['ROW NUMBER:' + str(rowNumber), 'CELL SOURCE:' + cell_source, 'CELL TARGET:' + cell_target,
                           'ERROR: ' + str(e)])

        # NEXT ROW TO WRITE
        lastRow += 1

    currentTimeCell = excelHandler.getCellCoordinates(sheet='Relational DB',
                                                      cellName='Time_Stamp')  # THIS WILL GET THE EXACT CELL, SO INSTEAD I TOOK ONLY THE COLUMN WITHOUT THE ROW
    BatchIDCell = excelHandler.getCellCoordinates(sheet='Relational DB',
                                                  cellName='Batch_ID')  # THIS WILL GET THE EXACT CELL, SO INSTEAD I TOOK ONLY THE COLUMN WITHOUT THE ROW

    for rowNumber in range(4, excelHandler.getMaxRow(sheet='Relational DB') + 1):


        if currentTimeCell and BatchIDCell:
            excelHandler.writeCell(sheet='Relational DB', cell=str(currentTimeCell[:-1])  +str(rowNumber), value=currentTime)
            excelHandler.writeCell(sheet='Relational DB', cell=str(BatchIDCell[:-1]) + str(rowNumber), value=str(BatchID))
        else:
            print('CELL NOT FOUND')

        currentRowData = excelHandler.getEntireRowFromSheet(sheet='Relational DB', row=rowNumber)
        currentRowData = ['' if i is None else str(i) for i in currentRowData]
        currentRowData.append(timeCount)
        listOfTuplesRelational.append(tuple(currentRowData))

    if isFirstRun:
        for rowNumber in range(2, excelHandler.getMaxRow(sheet='Ref_Dictionary') + 1):
            currentRowData = excelHandler.getEntireRowFromSheet(sheet='Ref_Dictionary', row=rowNumber)
            listOfTuplesRef.append(tuple(currentRowData))

    excelHandler.saveSpreadSheet(fileName=InputFileName)

    if SavePDF:
        pdfFileName = excelHandler.getCellFromSheet(sheet='Cover page', cell='B5')
        ExcelToPDF.excelToPDF(pdfFileName=pdfFileName, fileName=InputFileName, sheetsListToConvert=sheetsToPdfList)

    print("ERRORS IN ROWS: ", errors)
    print("SKIPPED ROWS: ", skippedRows)

    if isFirstRun:
        db.insertDynamicTableFast(db.s2t_mapping, columns=s2tColumns, values=listOfTuplesS2T)
        db.insertDynamicTableFast(db.ref_dictionary, columns=refDictionaryColumns, values=listOfTuplesRef)

    db.insertDynamicTableFast(db.landing_db, columns=landingDBColumns, values=listOfTuplesLanding)
    db.insertDynamicTableFast(db.relational_db, columns=relationalColumns, values=listOfTuplesRelational)

    # TO CALCULATE EXECUTION TIME
    print("--- Took %s seconds to process {0}---".format(InputFileName) % (time.time() - current_time))

    if tamara:
        # ########################################################################################################################

        # TODO: IMPORTS FIRST
        from Preprocess import Preprocess
        from Reports import Reports
        import tableChecks

        import pandas as pd

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', 100)

        formattedTime = str(currentTime).replace('/', '-').replace(':', '').replace(' ', '_')
        outputFile = 'Validation_{0}_{1}.xlsx'.format(pdfFileName, formattedTime)
        excelHandler.creatWorkBook(outputFile)
        excelHandlerForOutput = ExcelHandler(fileName=outputFile)

        try:

            # GET TABLES FROM DB INTO PANDAS DATAFRAME
            df_curr, df_old = db.relationalDF(selctedTable=db.relational_db, time_stamp=currentTime)
            ref_dict = db.getTableToDF(selctedTable=db.ref_dictionary)

            # PREPROCESS ALL DF (STRIP FROM EXTRA WHITE SPACE AND REMOVE ARABIC SPECIAL CHARACTERS)
            print('____________________________INITAIL PREP____________________________')
            prep = Preprocess()
            df_curr = prep.initialPrep(df_curr)
            df_old = prep.initialPrep(df_old)
            ref_dict = prep.initialPrep(ref_dict)

            # PRED DISCREPANCIES CHECK
            print('____________________________PredDisc____________________________')
            PredDisc = prep.getPredDiscrepancies(df_curr, df_old)
            excelHandlerForOutput.saveDFtoExcel('predecessor discrepancies', PredDisc)

            # PREP DATES AND NUMBER VALUES
            print('____________________________date and obs prep____________________________')
            relational_data = prep.prepDatesAndValues(df_curr)
            reports = Reports(relational_data)
            print(relational_data[
                      ['OBS_VALUE_P', 'PUBLICATION_DATE_EN_P', 'PUBLICATION_DATE_AR_P',
                       'TIME_PERIOD_DATE_P']].to_string())

            # GET GOOD AND BAD ROWS AND OUTPUT TO EXCEL
            print('____________________________pass fail____________________________')
            optionalColumns = ['NOTE1_AR_P', 'NOTE2_AR_P', 'NOTE3_AR_P', 'NOTE1_EN_P', 'NOTE2_EN_P', 'NOTE3_EN_P',
                               'UNIT_EN_P', 'UNIT_AR_P', 'MULTIPLIER_EN_P', 'MULTIPLIER_AR_P']

            tableRules = tableChecks.Table(relational_data, ref_dict, optionalColumns)
            df_pass, df_fail = tableRules.getPassFail()
            excelHandlerForOutput.saveDFtoExcel('fail', df_fail)
            excelHandlerForOutput.saveDFtoExcel('pass', df_pass)
            print('rows passed', len(df_pass), 'out of', len(relational_data))
            print('rows failed', len(df_fail), 'out of', len(relational_data))

            # GET MISSING LOOKUPS
            print('____________________________missing CL____________________________')
            missingCL = reports.CLCoverage(ref_dict)
            excelHandlerForOutput.saveDFtoExcel('missing lookups', missingCL)
            print('missingCL')

            # GET MIN MAX
            print('____________________________min max____________________________')
            min_max = reports.minmax(ref_dict, freq)
            excelHandlerForOutput.saveDFtoExcel('min_max', min_max)

            # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
            print('_______________________changes and frequency____________________________')
            diff, freq = reports.changes(freq)
            excelHandlerForOutput.saveDFtoExcel('frequency', freq)
            excelHandlerForOutput.saveDFtoExcel('changes', diff)

            # GET TOTALS REPORT
            print('____________________________Total____________________________')
            total = reports.totals_new(ref_dict)
            excelHandlerForOutput.saveDFtoExcel('total', total)
            excelHandlerForOutput.closeWriter()

            print("--- Took %s seconds to process ---" % (time.time() - start_time))

        except Exception as e:
            # TODO delete file
            print('Reporting failed')
            print(e)
            excelHandlerForOutput.closeWriter()

    db.closeConnection()

    print("--- Took %s seconds to process all files---" % (time.time() - start_time))
