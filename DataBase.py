from random import Random, randrange
import pandas as pd
import cx_Oracle
import config


# TODO: INSERT NULL
# TODO: DYNAMIC
# TODO GET TABLE

class DB:
    connection = None

    def __init__(self, landing_db='landing_db', relational_db='relational_db', s2t_mapping='s2t_mapping',
                 ref_dictionary='ref_dictionary'):
        self.landing_db = landing_db
        self.relational_db = relational_db
        self.s2t_mapping = s2t_mapping
        self.ref_dictionary = ref_dictionary
        print('DB...')
        try:

            self.connection = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                                             config.password,
                                                                             config.dsn,
                                                                             config.port,
                                                                             config.SERVICE_NAME))

            cx_Oracle.connect

            print('VERSION::', self.connection.version)


        except cx_Oracle.Error as error:
            print('ERROR', error)
            # release the connection
            if self.connection:
                self.connection.close()

    def printDescription(self):
        print('-------------------landing_db------------------------')

        sql = """SELECT * FROM {0}""".format(self.landing_db)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        for each in cursor.description:
            print(each[0:2])
        for each in cursor.execute(sql):
            print(each)

        print('-------------------landing_db------------------------')

        print('---------------------relational_db---------------------')

        sql = """SELECT * FROM {0}""".format(self.relational_db)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        c = 0
        for each in cursor.description:
            print(each[0:2])

        print('---------------------relational_db---------------------')

        print('---------------------s2t_mapping---------------------')

        sql = """SELECT * FROM {0}""".format(self.s2t_mapping)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        c = 0
        for each in cursor.description:
            print(each[0:2])

        print('---------------------s2t_mapping---------------------')

        print('---------------------ref_dictionary---------------------')

        sql = """SELECT * FROM {0}""".format(self.ref_dictionary)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        c = 0
        for each in cursor.description:
            print(each[0:2])

        print('---------------------ref_dictionary---------------------')

    def createDynamicTable(self, tableName, columns):
        columnsSQL = """"""
        for column in columns:
            columnsSQL += """{0} VARCHAR2(4000 CHAR) {1}
                          """.format(column, ',' if not column == columns[-1] else '')

        sql = """
              CREATE TABLE EPUBLICATION.{0}
          ( {1}) """.format(tableName, columnsSQL)
        print(':::::', sql)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            print(' A NEW TABLE WITH THE FOLLOWING NAME:', str(tableName),
                  ' HAS BEEN CREATED')
        except Exception as e:
            if str(e).__contains__('name is already used by an existing object'):
                print('TABLE NAME ALREADY EXISTS...')
                return
            else:
                print(e)
                return

    def insertDynamicTable(self, tableName, columns, values):
        columnsSQL = """"""
        valuesSQL = """"""
        counter = 0
        for column, value in zip(columns, values):
            counter += 1
            isLastValue = counter == len(columns)
            columnsSQL += """{0} {1}
                          """.format(column, ',' if not isLastValue else '')
            isLastValue = counter == len(values)
            value = 'NULL' if value is None else value
            valuesSQL += """'{0}' {1}
                                     """.format(value, ',' if not isLastValue else '')
        valuesSQL = valuesSQL.replace("'NULL'", "NULL")
        sql = """INSERT INTO {0} ({1})
                values ({2})""".format(tableName, columnsSQL, valuesSQL)
        print(':::::', sql)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
            return

    def insertIntoLandingDB(self, sheetSource='', cellSource='', cellContent='', TimeStamp='', BatchID=''):

        sql = """INSERT INTO {5} (Sheet_Source,Cell_Source,Cell_Content,Time_Stamp,Batch_ID)
        values ('{0}','{1}','{2}','{3}','{4}' )""".format(sheetSource, cellSource, cellContent,
                                                          TimeStamp, BatchID, self.landing_db)

        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)

    def printRelationalDB(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))

        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.relational_db)
        cursor.execute(SQL)
        for record in cursor:
            print('relational_db', record)

    def printLandingDB(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))
        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.landing_db)
        cursor.execute(SQL)
        for record in cursor:
            print('landing_db', record)


    def getNumberOfRecords(self,tableName):

        sql = "SELECT * FROM {0}".format(tableName)
        print(':::::', sql)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            counter = 0
            for record in cursor:
                counter+=1
            return counter
        except Exception as e:
            print(e)
            return

    def printS2t(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))
        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.s2t_mapping)
        cursor.execute(SQL)
        for record in cursor:
            print('s2t_mapping', record)

    def printRefDictionary(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))
        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.ref_dictionary)
        cursor.execute(SQL)
        for record in cursor:
            print('ref_dictionary', record)

    def getTableToDF(self, selctedTable):
        SQL = """SELECT * FROM {0}""".format(selctedTable)
        df_input = pd.read_sql(SQL, con=self.connection)
        return df_input

    def relationalDF(self, selctedTable, time_stamp):
        SQL = """select * from {0} where time_stamp in (select *  from ( select distinct time_stamp from 
        {0} order by time_stamp desc ) where ROWNUM <3)""".format(selctedTable)
        df_all = pd.read_sql(SQL, con=self.connection)
        df_new = df_all[df_all.TIME_STAMP == time_stamp].reset_index(drop=True)
        df_old = df_all[df_all.TIME_STAMP != time_stamp].reset_index(drop=True)
        print('TEST' , str(df_old) , str(df_new))
        return df_new, df_old

    def closeConnection(self):
        # release the connection
        if self.connection:
            self.connection.commit()
            self.connection.close()
