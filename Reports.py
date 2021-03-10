import pandas as pd


class Reports:
    def __init__(self, table):
        self.table = table
        self.columns = [*self.table.columns]
        self.freq = 0  # monthly = 0
        self.lookups = []
        self.lookups_en = []
        self.time_period = ['TIME_PERIOD_Y_P','TIME_PERIOD_M']
        self.values = 'OBS_VALUE_P'
        self.date = 'TIME_PERIOD_DATE_P'

        freq_val = self.table['FREQUENCY'][self.table.FREQUENCY.first_valid_index()].upper()
        if freq_val.__contains__('YEAR') or freq_val.__contains__('ANNUAL'):
            self.freq = 1  # yearly = 1

        if self.freq == 1:
            self.columns.remove('TIME_PERIOD_M')
            self.time_period.remove('TIME_PERIOD_M')

        for i in self.columns:
            if i.upper().__contains__('CL'):
                self.lookups.append(i)
                if not i.upper().__contains__('_AR_'):
                    self.lookups_en.append(i)

        self.group_list = self.lookups_en + [self.values, self.date]

    def minmax(self):
        temp_list = [self.table.OBS_VALUE_P.idxmin(), self.table.OBS_VALUE_P.idxmax()]
        min_max = self.table.iloc[temp_list].sort_values(by=self.values)
        return_cols = self.time_period + self.lookups + ['OBS_VALUE', 'MIN/MAX']
        min_max.insert(len(return_cols) - 1, 'MIN/MAX', ['Minimum', 'Maximum'], True)

        return min_max[return_cols]

    def changes(self):
        # IF ONLY ONE PERIOD AVAILABLE RETURN MESSAGE
        if len(set(self.table[self.date])) == 1:
            diff = pd.DataFrame({'MESSAGE': ['Only one time period changes not applicable']})
            freq = pd.DataFrame({'MESSAGE': ['Only one time period frequency not applicable']})
            return diff, freq

        # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
        order_list = self.lookups_en + [self.date]
        diff = self.table[self.group_list].sort_values(by=order_list).reset_index(drop=True)
        diff = diff.assign(FREQUENCY=None, DIFFERENCE=None, PERCENT_DIFFERENCE=None)

        for i in range(len(diff) - 1):
            if self.freq == 1:
                frq = diff[self.date][i + 1].year - diff[self.date][i].year
            else:
                frq = (diff[self.date][i + 1].year - diff[self.date][i].year) * 12 \
                      + (diff[self.date][i + 1].month - diff[self.date][i].month)

            if frq == 1:
                diff.at[i + 1, 'FREQUENCY'] = frq
                diff.at[i + 1, 'DIFFERENCE'] = diff[self.values][i + 1] - diff[self.values][i]
                if diff[self.values][i] == 0:
                    # DO NOT DIVIDE BY ZERO
                    continue
                else:
                    diff.at[i + 1, 'PERCENT_DIFFERENCE'] = (diff[self.values][i + 1] - diff[self.values][i]) \
                                                           / diff[self.values][i] * 100
            else:
                continue

        order_list = [self.date] + self.lookups_en
        diff = diff.sort_values(by=order_list).reset_index(drop=True)
        freq = diff.drop_duplicates(subset=[self.date, 'FREQUENCY'])

        return diff, freq

    def totals_new(self, ref_dict):
        print(ref_dict['P_ID_P'])
        if ref_dict['P_ID'].isnull().all():
            return pd.DataFrame({'MESSAGE': ['Totals not applicable']})

        full_table = self.table
        ref_dict_en = ref_dict['CL_ID_P'].apply(lambda x: not str(x).__contains__('ar'))
        ref_dict_en = ref_dict[ref_dict_en]

        lookups = [i for i in self.lookups_en if i.upper().endswith('_P')]

        group_by = []
        for i in lookups:
            temp = pd.DataFrame({'DESCRIPTION_P': self.table[i]})
            temp = temp.assign(CL_ID_P=i[:-2].lower())
            temp = temp.merge(ref_dict_en[['CL_ID_P', 'DESCRIPTION_P', 'ID_P', 'P_ID_P']],
                              on=['CL_ID_P', 'DESCRIPTION_P'], how='left')
            full_table[i + '_ID'] = temp['ID_P']
            full_table[i + '_ParentID'] = temp['P_ID_P']
            group_by.append(i + '_ID')

        actual_totals = pd.DataFrame(columns=group_by+['OBS_VALUE_P'])
        group_fixed = self.time_period +['MEASURE_ID_P']
        for i in group_by:
            group_by_list = group_by[:]
            group_by_list.remove(i)
            group_by_list.append(i.replace('_ID', '_ParentID'))
            print(group_by_list)
            temp2 = full_table.groupby(group_by_list+group_fixed).agg({'OBS_VALUE_P': "sum"}).reset_index()
            temp2.columns = [i.replace('_ParentID', '_ID') for i in temp2.columns]
            actual_totals = actual_totals.append(temp2, ignore_index=True)
            print(actual_totals)

        on = [i for i in actual_totals.columns if not i.__contains__('OBS')]

        actual_totals = actual_totals.drop_duplicates()

        totals = actual_totals.merge(full_table,on=on, how='left', suffixes=('_L', '_R'))

        out_cols = self.lookups + self.time_period + ['OBS_VALUE_P_L','OBS_VALUE_P_R']
        totals = totals[out_cols]

        totals = totals.rename(columns = {'OBS_VALUE_P_L': 'ACTUAL TOTALS', 'OBS_VALUE_P_R': 'REPORTED TOTALS'}, inplace = False)

        return totals

    def getPredDiscrepancies_test(self, curr_table, old_table):
        if old_table.empty:
            PredDisc = pd.DataFrame({'MESSAGE': ['No previously inserted data for this table']})
        else:
            columns_P = [i for i in old_table.columns if i.upper().endswith('_P')]
            print(columns_P)
            columns_output = [i for i in old_table.columns if not i.upper().endswith('_P')]
            print(columns_output)

            joint_table = curr_table.append(old_table, ignore_index=True)
            PredDisc = joint_table.drop_duplicates(subset=columns_P, keep=False)
            PredDisc = PredDisc[columns_output]

            if PredDisc.empty:
                PredDisc = pd.DataFrame({'MESSAGE': ['No predecessor discrepancies']})

        return PredDisc
