import glob
import os


def getListOfFiles():
    directory = os.path.abspath('.')
    path = directory + "\\Input\\*.xlsx"
    list_of_files = glob.glob(path)
    print(list_of_files)
    return list_of_files
