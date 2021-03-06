from ValidationRules import ValidationRules
import pandas as pd

rules = ValidationRules()


class Table:
    def __init__(self, table, lookups):
        self.table = table
        self.lookups = lookups

        self.freq = self.table['FREQUENCY'][self.table.FREQUENCY.first_valid_index()].upper()
        self.columns = self.table.columns
        self.preprocessed = ['PERIOD_Y', 'PERIOD_DATE', 'PUB_DATE_AR', 'PUB_DATE_EN', 'Obs_toNumber']

        # MONTHLY PERIOD WILL ONLY BE CHECKED IF FREQUENCY IS NOT YEARLY
        if not self.freq.upper().__contains__('YEAR'):
            self.preprocessed.append('PERIOD_M')

        self.optionalColumns = ['NOTE1_AR', 'NOTE2_AR', 'NOTE3_AR', 'NOTE1_EN', 'NOTE2_EN', 'NOTE3_EN',
                                'UNIT_EN', 'UNIT_AR', 'MULTIPLIER_EN', 'MULTIPLIER_AR']

    def table_rules(self, row):
        output = {'isnull': [],
                  'wrong type': [],
                  'wrong language': [],
                  'out of range lookup': [],
                  'invalid input': []}

        input = row
        lookups = self.lookups

        for item in self.columns:
            if item.upper().endswith('_P'):
                if item.upper() in ['TIME_PERIOD_DATE_P', 'OBS_VALUE_P']:
                    continue
                else:
                    if input[item] is None:
                        org = item[:-2].upper()
                        if input[org] is None:
                            continue
                        else:
                            output['invalid input'].append(org)

            else:
                # CHECK NULL AND TEXT TYPE
                if rules.notnull(input[item]):
                    # CHECK DATA TYPE
                    dataType = 'text'
                    if str(item).upper() == 'TIME_PERIOD_Y':
                        dataType = 'number'

                    if not rules.data_type(input[item], dataType):
                        output['wrong type'].append(item)

                    if item.upper().__contains__('CL'):
                        if str(item).upper() in [*lookups['CL_ID']]:
                            if not rules.check_dict(input[item], item, lookups):
                                output['out of range lookup'].append(item)

                    elif str(item).upper().__contains__('EN'):
                        if not rules.lang(input[item], 'en'):
                            output['wrong language'].append(item)

                    elif str(item).upper().__contains__('AR'):
                        if not rules.lang(input[item], 'ar'):
                            output['wrong language'].append(item)

                else:
                    if item not in self.optionalColumns:
                        output['isnull'].append(item)

        return output

    # CHECK PASS FAIL
    def getPassFail(self):
        dataframe = self.table

        df_fail = pd.DataFrame(columns=[*dataframe.columns] +
                                       ['isnull', 'wrong type', 'wrong language', 'out of range lookup', 'invalid input'])
        df_pass = pd.DataFrame(columns=dataframe.columns)

        for column, row in dataframe.iterrows():
            error_dict = self.table_rules(row)
            if len([1 for x in error_dict if len(error_dict[x]) != 0]) != 0:
                inrow = [*row.values] + [*error_dict.values()]
                df_fail = df_fail.append(pd.DataFrame([inrow], columns=df_fail.columns), ignore_index=True)
            else:
                df_pass = df_pass.append(pd.DataFrame([[*row.values]], columns=df_pass.columns), ignore_index=True)
        return df_pass, df_fail
