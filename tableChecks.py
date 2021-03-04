from ValidationRules import ValidationRules
import pandas as pd

rules = ValidationRules()


class Table:
    def __init__(self, table, lookups):
        self.table = table
        self.lookups = lookups

        self.CL_list = []
        self.commonColumns = []

        for item in self.table.columns:
            if str(item).upper().__contains__('CL'):
                self.CL_list.append(item)
            elif item in ['TIME_STAMP','BATCH_ID']:
                continue
            else:
                self.commonColumns.append(item)

    def table_rules(self,row):
        output = {'isnull': [],
                  'wrong type': [],
                  'wrong language': [],
                  'out of range lookup':[]}

        input = row
        lookups = self.lookups

        # CHECK IF LOOKUPS ARE IN RANGE
        for item in self.CL_list:
            if str(item).upper() in [*lookups['CL_ID']]:
                if not rules.check_dict(input[item], item, lookups):
                    output['out of range lookup'].append(item)

        # CHECK COMMON COLUMNS
        for item in self.commonColumns:
            # DO NOT CHECK FOR NULL
            if str(item).upper() in ['NOTE1_AR','NOTE2_AR','NOTE3_AR','NOTE1_EN','NOTE2_EN','NOTE3_EN']:
                if rules.notnull(input[item]):
                    if str(item).upper().__contains__('EN'):
                        if not rules.lang(input[item].lower(), 'en'):
                            output['wrong language'].append(item)

                    elif str(item).upper().__contains__('AR'):
                        if not rules.lang(input[item].lower(), 'ar'):
                            output['wrong language'].append(item)
                else:
                    continue

            # CHECK FOR NULL
            else:
                # CHECK DATA TYPE
                dataType = 'text'
                if str(item).upper() == 'TIME_PERIOD_Y':
                    dataType = 'number'
                if not rules.data_type(input[item], dataType):
                    output['wrong type'].append(item)

                # CHECK IF DATA IS NULL
                if not rules.notnull(input[item]):
                    output['isnull'].append(item)

                # CHECK LANGUAGE
                if str(item).upper().__contains__('EN'):
                    if not rules.lang(input[item].lower(), 'en'):
                        output['wrong language'].append(item)

                elif str(item).upper().__contains__('AR'):
                    if not rules.lang(input[item], 'ar'):
                        output['wrong language'].append(item)

        return output

    # CHECK PASS FAIL
    def getPassFail(self):
        dataframe = self.table

        df_fail = pd.DataFrame(columns=[*dataframe.columns] + ['isnull', 'wrong type', 'wrong language', 'out of range lookup'])
        df_pass = pd.DataFrame(columns=dataframe.columns)

        for column, row in dataframe.iterrows():
            error_dict = self.table_rules(row)
            if len([1 for x in error_dict if len(error_dict[x]) != 0]) != 0:
                inrow = [*row.values] + [*error_dict.values()]
                df_fail = df_fail.append(pd.DataFrame([inrow], columns=df_fail.columns), ignore_index=True)
            else:
                df_pass = df_pass.append(pd.DataFrame([[*row.values]], columns=df_pass.columns), ignore_index=True)
        return df_pass, df_fail