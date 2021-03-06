import pandas as pd


class Reports:
    def __init__(self, table, sum_by):
        self.table = table
        self.columns = self.table.columns
        self.sum_by = sum_by
        self.lookups = []
        self.values = 'OBS_VALUE_P'
        self.date = 'TIME_PERIOD_DATE_P'
        self.value_id = 'MEASUREID'
        self.value_name = 'MEASURE_NAME_EN'

        for i in self.columns:
            if i.upper().__contains__('CL'):
                self.lookups.append(i)
        self.group_list = self.lookups + [self.values, self.date]

    def minmax(self):
        temp_list = [self.table.OBS_VALUE_P.idxmin(), self.table.OBS_VALUE_P.idxmax()]
        min_max = self.table.iloc[temp_list].sort_values(by=self.values)
        return min_max

    def changes(self):
        # GET DIFFERENCE AND PERCENTAGE DIFFERENCE

        diff = self.table[self.group_list].sort_values(by=self.group_list).reset_index(drop=True)
        diff = diff.assign(DIFFERENCE=None, PERCENT_DIFFERENCE=None)
        for i in range(len(diff) - 1):
            if diff[self.date][i + 1].month - diff[self.date][i].month == 1:
                diff.at[i + 1, 'DIFFERENCE'] = diff[self.values][i + 1] - diff[self.values][i]
                if diff[self.values][i] == 0:
                    continue
                else:
                    diff.at[i + 1, 'PERCENT_DIFFERENCE'] = (diff[self.values][i + 1] - diff[self.values][i]) \
                                                           / diff[self.values][i] * 100
            else:
                continue

        order_list = [self.date] + self.lookups
        diff = diff.sort_values(by=order_list).reset_index(drop=True)
        return diff

    def frequency(self):
        freq = self.table.drop_duplicates([self.date])[[self.date]].sort_values(by=self.date).reset_index(drop=True)
        freq = freq.assign(FREQUENCY=None)
        for i in range(len(freq) - 1):
            freq.at[i + 1, 'FREQUENCY'] = freq[self.date][i + 1].month - freq[self.date][i].month
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

    def totals_new(self):
        group_list = self.group_list.remove(self.sum_by)

        reported_totals = self.table[self.table[self.value_name].upper() == 'TOTAL']#.groupby([self.value_id]).agg({self.values: "sum"})

        actual_totals = self.table[self.table[self.value_name].upper() != 'TOTAL'].groupby([self.value_id])\
                                                                                  .agg({self.values: "sum"})

        totals = reported_totals.merge(actual_totals, on=group_list, how='right'
                                       ).merge(reported_totals - actual_totals, on=group_list, how='left')

        totals.columns = ['Reported Total', 'Actual Total', 'Reported-Actual']
        return totals

    def getPredDiscrepancies(self, old_table):
        joint_table = self.table[[*old_table.columns]].append(old_table, ignore_index = True)
        PredDisc = joint_table.drop_duplicates(subset=joint_table.columns.difference(['TIME_STAMP','BATCH_ID']), keep=False)
        return PredDisc
