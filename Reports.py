import pandas as pd


class Reports:
    def __init__(self,table):
        self.table = table

        self.CL_list = []
        self.commonColumns = []
    
    def minmax(self):
        temp_list = [self.table.Obs_toNumber.idxmin(), self.table.Obs_toNumber.idxmax()]
        min_max = self.table.iloc[temp_list].sort_values(by='Obs_toNumber')
        return min_max

    def changes(self):
        # GET DIFFERENCE AND PERCENTAGE DIFFERENCE

        diff = self.table[['CL_AGE_GROUP_EN_V1', 'CL_SEX_EN_V2', 'DATE', 'Obs_toNumber']].sort_values(
                                        by=['CL_AGE_GROUP_EN_V1', 'CL_SEX_EN_V2', 'DATE']).reset_index()
        diff = diff.assign(difference=None, perc_diff=None)
        for i in range(len(diff) - 1):
            if diff['DATE'][i + 1].month - diff['DATE'][i].month == 1:
                diff.at[i + 1, 'difference'] = diff['Obs_toNumber'][i + 1] - diff['Obs_toNumber'][i]
                if diff['Obs_toNumber'][i] == 0:
                    continue
                else:
                    diff.at[i + 1, 'perc_diff'] = (diff['Obs_toNumber'][i + 1] - diff['Obs_toNumber'][i]) / diff['Obs_toNumber'][i] * 100
            else:
                continue

        diff = diff.sort_values(by=['DATE', 'CL_SEX_EN_V2', 'CL_AGE_GROUP_EN_V1']).reset_index()
        return diff

    def frequency(self):
        freq = self.table.drop_duplicates(['DATE'])[['DATE']].sort_values(by='DATE').reset_index(drop=True)
        freq = freq.assign(FREQ=None)
        for i in range(len(freq) - 1):
            freq.at[i + 1, 'FREQ'] = freq['DATE'][i + 1].month - freq['DATE'][i].month
        return freq

    def totals(self):
        reported_totals = self.table[self.table['CL_AGE_GROUP_EN_V1'] == 'Total'].groupby(
            ['TIME_PERIOD_Y', 'TIME_PERIOD_M', 'CL_SEX_EN_V2']).agg({"Obs_toNumber": "sum"})

        actual_totals = self.table[self.table['CL_AGE_GROUP_EN_V1'] != 'Total'].groupby(
            ['TIME_PERIOD_Y', 'TIME_PERIOD_M', 'CL_SEX_EN_V2']).agg({"Obs_toNumber": "sum"})

        totals = reported_totals.merge(actual_totals, on=['TIME_PERIOD_Y', 'TIME_PERIOD_M', 'CL_SEX_EN_V2'], how='right'
                                       ).merge(reported_totals - actual_totals,
                                               on=['TIME_PERIOD_Y', 'TIME_PERIOD_M', 'CL_SEX_EN_V2'], how='left')

        totals.columns = ['Reported Total', 'Actual Total', 'Reported-Actual']
        return totals