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
    if str(file).__contains__("~"):
        continue

    # TO CALCULATE EXECUTION TIME
    start_time = time.time()

    InputFileName = file
    tablePostFix = 'test324231423432424324234'

    # TASK 1 READ THE EXCEL FILE:

    excelHandler = ExcelHandler(fileName=InputFileName)

    db = DB(landing_db = 'landing_db' + tablePostFix , relational_db = 'relational_db' + tablePostFix, s2t_mapping = 's2t_mapping' + tablePostFix, ref_dictionary = 'ref_dictionary' + tablePostFix )
    s2tColumns = excelHandler.getRowDataFromSheet(sheet='S2T Mapping' , row=1)
    relationalColumns = excelHandler.getRowDataFromSheet(sheet='Relational DB' , row=2)
    refDictionaryColumns = excelHandler.getRowDataFromSheet(sheet='Ref_Dictionary' , row=1)
    landingDBColumns = excelHandler.getRowDataFromSheet(sheet='Landing DB' , row=1)

    db.createDynamicTable(tableName=db.s2t_mapping, columns=s2tColumns)
    db.createDynamicTable(tableName=db.relational_db, columns=relationalColumns)
    db.createDynamicTable(tableName=db.landing_db, columns=relationalColumns)
    db.createDynamicTable(tableName=db.ref_dictionary, columns=refDictionaryColumns)


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

        db.insertIntoS2t_mapping(sheetSource=sheet_source, cellSource=cell_source, SHEET_TARGET=sheet_target,
                                 CELL_TARGET=cell_target, CELL_TYPE=cell_type, DESC_AR=desc_ar, DATA_TYPE=dataType,
                                 IS_MANDATORY=is_mandatory, REF_DICTIONARY=Ref_Dictionary)

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

        # except Exception as e:
        #     print("ERROR IN ROW#" + str(rowNumber) + " -- " + str(e))
        #     errors.append(['ROW NUMBER:' + str(rowNumber), 'CELL SOURCE:' + cell_source, 'CELL TARGET:' + cell_target , 'ERROR: ' + str(e)]  )

        # NEXT ROW TO WRITE
        lastRow += 1

    for rowNumber in range(4, excelHandler.getMaxRow(sheet='Relational DB') + 1):
        # TIME AND BATCH ID
        excelHandler.writeCell(sheet='Relational DB', cell=str('Y' + str(rowNumber)), value=currentTime)
        excelHandler.writeCell(sheet='Relational DB', cell=str('Z' + str(rowNumber)), value=str(BatchID))

        currentRowData = excelHandler.getRowDataFromSheet(sheet='Relational DB', row=rowNumber)
        db.insertIntoRelationalDB(currentRowData[0],
                                  currentRowData[1],
                                  currentRowData[2],
                                  currentRowData[3],
                                  currentRowData[4],
                                  currentRowData[5],
                                  currentRowData[6],
                                  currentRowData[7],
                                  currentRowData[9],
                                  currentRowData[10],
                                  currentRowData[11],
                                  currentRowData[12],
                                  currentRowData[13],
                                  currentRowData[14],
                                  currentRowData[15],
                                  currentRowData[16],
                                  currentRowData[17],
                                  currentRowData[18],
                                  currentRowData[19],
                                  currentRowData[20],
                                  currentRowData[21],
                                  currentRowData[22],
                                  currentRowData[23],
                                  currentRowData[24],
                                  currentRowData[25],
                                  currentRowData[8],
                                  )

    for rowNumber in range(2, excelHandler.getMaxRow(sheet='Ref_Dictionary') + 1):
        currentRowData = excelHandler.getRowDataFromSheet(sheet='Ref_Dictionary', row=rowNumber)
        db.insertIntoRef_dictionary(DESCRIPTION=currentRowData[0], ID=currentRowData[1], CL_ID=currentRowData[2])

    excelHandler.saveSpreadSheet(fileName=InputFileName)

    pdfFileName = excelHandler.getCellFromSheet(sheet='Cover page', cell='B5')
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

    ########################################################################################################################

    # TODO: IMPORTS FIRST
    from Preprocess import Preprocess
    from Reports import Reports
    import tableChecks

    outputFile = 'Validation_{0}.xlsx'.format(pdfFileName)
    excelHandler.creatWorkBook(outputFile)
    excelHandlerForOutput = ExcelHandler(fileName=outputFile)


    # GET TABLES FROM DB INTO PANDAS DATAFRAME
    df_input = db.tamaraPandas(selctedTable=db.relational_db)
    ref_dict = db.tamaraPandas(selctedTable=db.ref_dictionary)

    tableRules = tableChecks.Table(df_input, ref_dict)
    df_pass,df_fail = tableRules.getPassFail()

    # OUTPUT GOOD AND BAD ROWS
    excelHandlerForOutput.saveDFtoExcel('fail', df_fail)
    excelHandlerForOutput.saveDFtoExcel('pass', df_pass)

    # PREPROCESS DATA
    a = Preprocess(df_input)
    input = a.table1()

    # GET MIN MAX
    b = Reports(input)
    min_max = b.minmax()
    excelHandlerForOutput.saveDFtoExcel('min_max', min_max)

    # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
    diff = b.changes()
    excelHandlerForOutput.saveDFtoExcel('changes', diff)

    # FREQ CHECK
    freq = b.frequency()
    excelHandlerForOutput.saveDFtoExcel('frequency', freq)

    # GET TOTALS REPORT
    total = b.totals()
    excelHandlerForOutput.saveDFtoExcel('total', total)

    excelHandlerForOutput.closeWriter()
    db.closeConnection()