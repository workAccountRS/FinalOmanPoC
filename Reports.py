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

    def CLCoverage(self, ref_dict):
        prepCLs = [i for i in self.lookups if i.endswith('_P')]
        print(prepCLs)
        lookupDF = self.table[prepCLs]
        print(lookupDF)

        missing = {'Missing CL_ID':[],'Missing Description':[]}
        for i in prepCLs:
            dist_list = lookupDF[i].dropna().unique()
            lookup_list = ref_dict[ref_dict['CL_ID_P']==i[:-2].lower()]['DESCRIPTION_P'].tolist()

            for j in lookup_list:
                if j not in dist_list:
                    missing['Missing CL_ID'].append(i)
                    missing['Missing Description'].append(j)

        missing = pd.DataFrame(missing)
        return missing

    def minmax(self, ref_dict, freq):
        """
        Min max function returns a dict having the minimum and maximum values of each measure for the latest
        statistical period (TIME_PERIOD_Y,TIME_PERIOD_M). The values being considered are the OBS_VAL values
        after being casted to float values. Nulls are ignored.
        The function excludes total values (filtered using parent child information in ref_dictionary, any parent value
        is excluded from calculation)
        """
        # ignore frequency that is not = to statistical period specified in cover page
        childrenDF = self.table[self.table['FREQUENCY_P'] == freq]
        childrenDF = childrenDF[childrenDF['TIME_PERIOD_DATE_P'] == childrenDF['TIME_PERIOD_DATE_P'].max()].reset_index()
        if len(childrenDF['MEASURE_ID_P'].unique()) == len(childrenDF):
            print('Only one value per measure, minimum maximum check not applicable')
            return pd.DataFrame({'MESSAGE': ['Only one value per measure, minimum maximum check not applicable']})

        # if parents specified (totals applicable) exclude parent values
        if not ref_dict['P_ID'].isnull().all():
            # get all english lookups
            en_dict = ref_dict.dropna(subset=['CL_ID_P'], axis=0)
            en_dict = en_dict[~en_dict['CL_ID_P'].str.contains('ar')]
            # get a dict with only the parent lookups
            parentDF = en_dict.dropna(subset=['P_ID_P'])[['CL_ID_P', 'P_ID_P']].drop_duplicates()
            parentDF = parentDF.merge(en_dict[['CL_ID_P', 'ID_P', 'DESCRIPTION_P']], how='left',
                                      left_on=['CL_ID_P', 'P_ID_P'], right_on=['CL_ID_P', 'ID_P'])

            mask = pd.DataFrame()
            for i in en_dict['CL_ID_P'].str.upper().unique():
                col_name = i+'_P'
                desc_list = parentDF[parentDF['CL_ID_P'].str.upper() == i]['DESCRIPTION_P'].tolist()
                mask[col_name] = childrenDF[col_name].isin(desc_list)
            full_mask = ~mask.any(axis=1)

            childrenDF = childrenDF.assign(mask=full_mask)
            childrenDF = childrenDF[childrenDF['mask'] == True].drop(columns='mask').reset_index(drop=True)

        gblist=['MEASURE_ID_P']

        grouped = childrenDF.groupby(gblist)

        min = childrenDF.loc[grouped["OBS_VALUE_P"].idxmin()]
        min['MIN/MAX'] = 'MINIMUM'

        max = childrenDF.loc[grouped["OBS_VALUE_P"].idxmax()]
        max['MIN/MAX'] = 'MAXIMUM'

        min_max = min.append(max)

        #design output
        return_CL = [i for i in self.lookups if not i.__contains__('_P')]

        return_cols = return_CL+['TIME_PERIOD_Y_P','TIME_PERIOD_M','MEASURE_NAME_AR','MEASURE_NAME_EN','UNIT_AR','UNIT_EN','OBS_VALUE', 'MIN/MAX']

        return min_max[return_cols]


    def changes(self,freq):

        group_list = self.lookups + [self.values,'MEASURE_ID_P', self.date] +self.time_period
        freq_table = self.table[self.table['FREQUENCY_P'] == freq]
        # IF ONLY ONE PERIOD AVAILABLE RETURN MESSAGE
        if len(set(freq_table[self.date])) == 1:
            diff = pd.DataFrame({'MESSAGE': ['Only one time period changes not applicable']})
            freq = pd.DataFrame({'MESSAGE': ['Only one time period frequency not applicable']})
            print('Only one time period changes and frequency check not applicable')
            return diff, freq
        # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
        order_list = self.lookups + ['MEASURE_ID_P', self.date]
        diff = freq_table[group_list].sort_values(by=order_list).dropna(subset=['TIME_PERIOD_DATE_P']).reset_index(drop=True)
        diff = diff.assign(FREQUENCY=None, DIFFERENCE=None, PERCENT_DIFFERENCE=None)
        for i in range(len(diff) - 1):
            if self.freq == 1:
                frq = diff[self.date][i + 1].year - diff[self.date][i].year
            else:
                frq = (diff[self.date][i + 1].year - diff[self.date][i].year) * 12 \
                      + (diff[self.date][i + 1].month - diff[self.date][i].month)

            if frq > 0:
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

        # design output
        measureDict = self.table[['MEASURE_ID_P','MEASURE_NAME_AR','MEASURE_NAME_EN','UNIT_AR','UNIT_EN']].drop_duplicates()
        measure_col =[*measureDict.columns]
        measure_col.remove('MEASURE_ID_P')
        diff = diff.merge(measureDict, how='left')

        order_list = [self.date,'MEASURE_ID_P']
        diff = diff.sort_values(by=order_list).reset_index(drop=True)
        diff_out = diff[self.lookups + self.time_period + measure_col + [self.values,'FREQUENCY','DIFFERENCE','PERCENT_DIFFERENCE']]
        freq = diff.sort_values(by=self.date).drop_duplicates(subset=[self.date,'FREQUENCY'])
        freq = freq[self.time_period+['FREQUENCY']]

        return diff_out, freq

    def totals_new(self, ref_dict):
        if ref_dict['P_ID'].isnull().all():
            print('Totals not applicable')
            return pd.DataFrame({'MESSAGE': ['Totals not applicable']})

        full_table = self.table
        ref_dict_en = ref_dict.dropna(subset=['CL_ID_P'], axis=0)
        ref_dict_en = ref_dict_en[~ref_dict_en['CL_ID_P'].str.contains('ar')]

        lookups = [i for i in self.lookups_en if i.upper().endswith('_P')]

        groupby_fixed = []
        sum_by = []
        for i in lookups:
            # GET LOOKUP i DESCRIPTION_P FROM RELATIONAL TABLE
            temp = pd.DataFrame({'DESCRIPTION_P': self.table[i]})
            # TEMP IS {'DESCRIPTION_P': self.table[i],'CL_ID_P' : i-'_P' }
            temp = temp.assign(CL_ID_P=i[:-2].lower())
            temp = temp.merge(ref_dict_en[['CL_ID_P', 'DESCRIPTION_P', 'ID_P', 'P_ID_P']],
                              on=['CL_ID_P', 'DESCRIPTION_P'], how='left')
            if temp['P_ID_P'].isnull().all():
                groupby_fixed.append(i)
            else:
                full_table[i + '_ID'] = temp['ID_P']
                full_table[i + '_ParentID'] = temp['P_ID_P']
                sum_by.append(i + '_ID')

        actual_totals = pd.DataFrame(columns=groupby_fixed+sum_by+['OBS_VALUE_P'])
        groupby_fixed = groupby_fixed + self.time_period + ['MEASURE_ID_P']
        for i in sum_by:
            sum_by_list = sum_by[:]
            sum_by_list.remove(i)
            sum_by_list.append(i.replace('_ID', '_ParentID'))
            temp2 = full_table.groupby(sum_by_list+groupby_fixed).agg({'OBS_VALUE_P': "sum"}).reset_index()
            temp2.columns = [i.replace('_ParentID', '_ID') for i in temp2.columns]
            actual_totals = actual_totals.append(temp2, ignore_index=True)

        on = [i for i in actual_totals.columns if not i.__contains__('OBS')]
        actual_totals = actual_totals.drop_duplicates()
        totals = actual_totals.merge(full_table,on=on, how='left', suffixes=('_L', '_R'))
        totals['CALCULATED - TOTALS'] = totals['OBS_VALUE_P_L'] - totals['OBS_VALUE_P_R']
        totals['CALCULATED - TOTALS'] = totals['CALCULATED - TOTALS'].round(0)

        # design output
        measureDict = self.table[['MEASURE_ID_P','MEASURE_NAME_AR','MEASURE_NAME_EN','UNIT_AR','UNIT_EN']].drop_duplicates()
        measure_col =[*measureDict.columns]
        measure_col.remove('MEASURE_ID_P')
        totals = totals.merge(measureDict, how='left')
        out_cols = self.lookups + self.time_period + measure_col + ['OBS_VALUE_P_L','OBS_VALUE_P_R','CALCULATED - TOTALS']
        totals = totals[out_cols]
        totals = totals.rename(columns={'OBS_VALUE_P_L': 'CALCULATED TOTALS', 'OBS_VALUE_P_R': 'REPORTED TOTALS'}, inplace = False)

        return totals