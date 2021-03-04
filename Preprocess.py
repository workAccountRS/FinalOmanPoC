import pandas as pd

class Preprocess:
    def __init__(self, table):
        self.table = table

    def table1(self):
        input = self.table.assign(Obs_toNumber=pd.to_numeric(self.table['OBS_VALUE'], errors='coerce'))
        if not self.table['FREQUENCY'][self.table.FREQUENCY.first_valid_index()].upper().__contains__('YEAR'):
            input = input.assign(MONTH=[i.split('-')[1].strip() for i in input['TIME_PERIOD_M']])
            input = input.assign(DATE=pd.to_datetime(input['MONTH'] + '-' + input['TIME_PERIOD_Y'].astype(str)))
        else:
            input = input.assign(DATE=pd.to_datetime(input['TIME_PERIOD_Y'].astype(str)))

        for i in [*input.columns]:
            try:
                if i in ['PUBLICATION_DATE_EN']:
                    input[i] = pd.to_datetime(input[i])
                elif i in ['TIME_PERIOD_Y']:
                    input[i] = pd.to_numeric(input[i])
                elif i in ['TIME_PERIOD_M']:
                    if not self.table['FREQUENCY'][self.table.FREQUENCY.first_valid_index()].upper().__contains__('YEAR'):
                        input[i] = [i.split('-')[1].strip() for i in input[i]]
                else:
                    input[i] = input[i].str.strip()
            except:
                continue
        for t in input.itertuples():
            if t.Obs_toNumber != t.Obs_toNumber:
                input.at[t.Index, 'Obs_toNumber'] = pd.to_numeric(t.OBS_VALUE[:-1])

        return input
