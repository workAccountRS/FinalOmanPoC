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

    def createDynamicTable(self, tableName , columns):
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
            self.connection.commit()
            print(' A NEW TABLE WITH THE FOLLOWING NAME:', str(tableName),
                  ' HAS BEEN CREATED')
        except Exception as e:
            if str(e).__contains__('name is already used by an existing object'):
                print('TABLE NAME ALREADY EXISTS...')
                return
            else:
                print(e)
                return


    def insertIntoRef_dictionary(self, DESCRIPTION='', ID='', CL_ID=''):
        sql = """INSERT INTO {3} (DESCRIPTION,ID,CL_ID)
        values ('{0}','{1}','{2}')""".format(DESCRIPTION, ID, CL_ID, self.ref_dictionary)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()

    def insertIntoS2t_mapping(self, sheetSource='', cellSource='', SHEET_TARGET='', CELL_TARGET='', CELL_TYPE='',
                              DESC_AR='', DATA_TYPE='', IS_MANDATORY='', REF_DICTIONARY=''):
        sql = """INSERT INTO {9} (Sheet_Source,Cell_Source,SHEET_TARGET,CELL_TARGET,CELL_TYPE, DESC_AR , DATA_TYPE , IS_MANDATORY , REF_DICTIONARY)
        values ('{0}','{1}','{2}','{3}','{4}', '{5}' ,'{6}','{7}','{8}')""".format(sheetSource, cellSource,
                                                                                   SHEET_TARGET, CELL_TARGET, CELL_TYPE,
                                                                                   DESC_AR, DATA_TYPE, IS_MANDATORY,
                                                                                   REF_DICTIONARY, self.s2t_mapping)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()

    def insertIntoLandingDB(self, sheetSource='', cellSource='', cellContent='', TimeStamp='', BatchID=''):

        sql = """INSERT INTO {5} (Sheet_Source,Cell_Source,Cell_Content,Time_Stamp,Batch_ID)
        values ('{0}','{1}','{2}','{3}','{4}' )""".format(sheetSource, cellSource, cellContent,
                                                          TimeStamp, BatchID, self.landing_db)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()

    def insertIntoRelationalDB(self,
                               PUBLICATION_NAME_AR,
                               PUBLICATION_NAME_EN,
                               PUBLICATION_DATE_AR,
                               PUBLICATION_DATE_EN,
                               TABLE_ID,
                               REP_NAME_AR,
                               REP_NAME_EN,
                               TEM_ID,
                               CL_AGE_GROUP_EN_V1,
                               CL_AGE_GROUP_AR_V1,
                               CL_SEX_AR_V1,
                               CL_SEX_EN_V2,
                               OBS_VALUE,
                               TIME_PERIOD_Y,
                               TIME_PERIOD_M,
                               NOTE1_AR,
                               NOTE1_EN,
                               NOTE2_AR,
                               NOTE2_EN,
                               NOTE3_AR,
                               NOTE3_EN,
                               SOURCE_AR,
                               SOURCE_EN,
                               TIME_STAMP, Batch_ID, FREQUENCY):

        sql = """insert into {0} ( PUBLICATION_NAME_AR, PUBLICATION_NAME_EN, PUBLICATION_DATE_AR, 
        PUBLICATION_DATE_EN, TABLE_ID, REP_NAME_AR, REP_NAME_EN, TEM_ID, CL_AGE_GROUP_AR_V1, CL_AGE_GROUP_EN_V1, CL_SEX_AR_V1, CL_SEX_EN_V2, 
        OBS_VALUE, TIME_PERIOD_Y, TIME_PERIOD_M, NOTE1_AR, NOTE1_EN, NOTE2_AR, NOTE2_EN, NOTE3_AR, NOTE3_EN, SOURCE_AR, SOURCE_EN, TIME_STAMP, BATCH_ID, FREQUENCY) values (
        :1 , :2 , :3 , :4 , :5 , :6 , :7 , :8 ,:9 ,:10 ,:11 ,:12 ,:13 ,:14 ,:15 ,:16 ,:17 ,:18 ,:19 ,:20 ,:21 ,:22 ,:23 ,:24 ,:25 , :26)""".format(
            self.relational_db)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql, [PUBLICATION_NAME_AR,
                             PUBLICATION_NAME_EN,
                             PUBLICATION_DATE_AR,
                             PUBLICATION_DATE_EN,
                             TABLE_ID,
                             REP_NAME_AR,
                             REP_NAME_EN,
                             TEM_ID,
                             CL_AGE_GROUP_AR_V1,
                             CL_AGE_GROUP_EN_V1,
                             CL_SEX_AR_V1,
                             CL_SEX_EN_V2,
                             OBS_VALUE,
                             TIME_PERIOD_Y,
                             TIME_PERIOD_M,
                             NOTE1_AR,
                             NOTE1_EN,
                             NOTE2_AR,
                             NOTE2_EN,
                             NOTE3_AR,
                             NOTE3_EN,
                             SOURCE_AR,
                             SOURCE_EN,
                             TIME_STAMP, Batch_ID, FREQUENCY, ])
        self.connection.commit()

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

    def tamaraPandas(self, selctedTable):
        SQL = """SELECT * FROM {0}""".format(selctedTable)
        df_input = pd.read_sql(SQL, con=self.connection)
        return df_input

    def closeConnection(self):
        # release the connection
        if self.connection:
            self.connection.close()

