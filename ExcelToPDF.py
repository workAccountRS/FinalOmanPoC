import win32com.client
import os
from datetime import datetime
from random import randint


def excelToPDF(pdfFileName='', fileName='', sheetsListToConvert=['Target Table', 'dsf' , 'Target Graph', 'Target PT', 'Text']):
    #  [4, 6, 7, 8, 9]
    try:
        o = win32com.client.Dispatch("Excel.Application")

        o.Visible = False

        directory = os.path.abspath('.')

        wb = o.Workbooks.Open("{0}".format(fileName))

        ws_index_list = sheetsListToConvert

        print('===========================SAVING PDF')
        for item in ws_index_list:
            path_to_pdf = directory + '/{0}_{1}.pdf'.format(item, pdfFileName)
            try:
                if type(item) == type(''):
                    wb.sheets(item).Select()
                elif type(item) == type(1):
                    wb.WorkSheets(item).Select()
                wb.ActiveSheet.ExportAsFixedFormat(0, path_to_pdf)
                print('SAVING: {0}'.format(path_to_pdf))
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)
    finally:
        try:
            wb.Close()
        except Exception as e:
            print(e)
