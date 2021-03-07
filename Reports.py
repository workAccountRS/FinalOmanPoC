import pandas as pd


class Reports:
    def __init__(self, table):
        self.table = table
        self.columns = [*self.table.columns]
        self.freq = 0 # monthly = 0
        self.lookups = []
        self.lookups_en = []
        self.time_period = []
        self.values = 'OBS_VALUE_P'
        self.date = 'TIME_PERIOD_DATE_P'

        freq_val = self.table['FREQUENCY'][self.table.FREQUENCY.first_valid_index()].upper()
        if freq_val.__contains__('YEAR') or freq_val.__contains__('ANNUAL'):
            self.freq = 1 #yearly = 1

        if self.freq == 1:
            self.columns.remove('TIME_PERIOD_M')

        for i in self.columns:
            if i.upper().__contains__('TIME_PERIOD') and not i.upper().endswith('_P'):
                self.time_period.append(i)

            elif i.upper().__contains__('CL'):
                self.lookups.append(i)
                if i.upper().__contains__('EN'):
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
        print('HEEEEEEEEERRRRRRRRRRRRRRRRREEEEEEEEEEEEEEEEE')
        print(diff.head())
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
        freq = diff.drop_duplicates(subset=[self.date,'FREQUENCY'])

        print(freq)

        return diff, freq

    def frequency(self):
        freq = self.table.drop_duplicates([self.date])[[self.date]].sort_values(by=self.date).reset_index(drop=True)

        if self.freq == 1:
            freq = freq.assign(Years=None)
            for i in range(len(freq) - 1):
                freq.at[i + 1, 'Years'] = freq[self.date][i + 1].year - freq[self.date][i].year
        else:
            freq = freq.assign(Months=None)
            for i in range(len(freq) - 1):
                freq.at[i + 1, 'Months'] = (freq[self.date][i + 1].year - freq[self.date][i].year) *12 \
                                           + (freq[self.date][i + 1].month - freq[self.date][i].month)
        return freq

    def getSumBy(self):
        sum_by = []
        for i in self.lookups_en:
            exists = 'TOTAL' in set(self.table[i].str.upper())
            if exists:
                sum_by.append(i)
        return sum_by

    def totals_new(self):
        sum_by_list = self.getSumBy()
        totals = None
        for sum_by in sum_by_list:
            group = self.lookups_en + [self.values, self.date]
            group.remove(sum_by)
            group.remove(self.values)

            reported_totals = self.table[self.table[sum_by].str.upper() == 'TOTAL'] \
                .groupby(group).agg({self.values: "sum"})

            actual_totals = self.table[self.table[sum_by].str.upper() != 'TOTAL'].groupby(group) \
                .agg({self.values: "sum"})

            sub_totals = reported_totals.merge(actual_totals, on=group, how='right'
                                               ).merge(reported_totals - actual_totals, on=group, how='left')

            if totals is None:
                totals = sub_totals
            else:
                totals = totals.append(sub_totals, ignore_index=True)

        totals.columns = ['Reported Total', 'Actual Total', 'Reported-Actual']
        return totals

    def getPredDiscrepancies(self, old_table):
        if old_table.empty:
            PredDisc = pd.DataFrame({'MESSAGE': ['No previously inserted data for this table']})
        else:
            joint_table = self.table[[*old_table.columns]].append(old_table, ignore_index=True)

            df_obj = joint_table.select_dtypes(['object'])
            joint_table[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

            PredDisc = joint_table.drop_duplicates(subset=joint_table.columns.difference(['TIME_STAMP', 'BATCH_ID']),
                                                   keep=False)

            if PredDisc.empty:
                PredDisc = pd.DataFrame({'MESSAGE': ['No predecessor discrepancies']})

        return PredDisc
