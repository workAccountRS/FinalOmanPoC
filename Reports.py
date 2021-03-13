import pandas as pd
import re


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
        lookupDF = self.table[prepCLs]

        missing = {'Missing CL_ID':[],'Missing Description':[]}
        for i in prepCLs:
            dist_list = lookupDF[i].dropna().unique()
            print('list',dist_list)
            lookup_list = ref_dict[ref_dict['CL_ID_P']==i[:-2].lower()]['DESCRIPTION_P'].tolist()
            print('check', lookup_list)

            for j in dist_list:
                if j not in lookup_list:
                    missing['Missing CL_ID'].append(i)
                    missing['Missing Description'].append(j)

        missing = pd.DataFrame(missing)
        return missing

    def minmax(self, ref_dict):
        """
        Min max function returns a dict having the minimum and maximum values of each measure for the latest
        statistical period (TIME_PERIOD_Y,TIME_PERIOD_M). The values being considered are the OBS_VAL values
        after being casted to float values. Nulls are ignored.
        The function excludes total values (filtered using parent child information in ref_dictionary, any parent value
        is excluded from calculation)
        """
        # no totals to be consider in the statistical sheet (no parent lookup values)
        if ref_dict['P_ID'].isnull().all():
            childrenDF = self.table

        # else: exclude parent values
        else:
            # get all english lookups
            en_dict = ref_dict.dropna(subset=['CL_ID_P'], axis=0)
            en_dict = en_dict[~en_dict['CL_ID_P'].str.contains('ar')]
            # get a dict with only the parent lookups
            parentDF = en_dict.dropna(subset=['P_ID_P'])[['CL_ID_P', 'P_ID_P']].drop_duplicates()
            parentDF = parentDF.merge(en_dict[['CL_ID_P', 'ID_P', 'DESCRIPTION_P']], how='left',
                                      left_on=['CL_ID_P', 'P_ID_P'], right_on=['CL_ID_P', 'ID_P'])
            print(parentDF)
            mask = pd.DataFrame()

            for i in en_dict['CL_ID_P'].str.upper().unique():
                print(i)
                col_name = i+'_P'
                desc_list = parentDF[parentDF['CL_ID_P'].str.upper() == i]['DESCRIPTION_P'].tolist()
                mask[col_name] = self.table[col_name].isin(desc_list)
            print(mask)
            full_mask = ~mask.any(axis=1)
            print(full_mask)

            childrenDF = self.table.assign(mask=full_mask)
            childrenDF = childrenDF[childrenDF['mask'] == True].drop(columns='mask').reset_index(drop=True)
            print(childrenDF)

        # CHECK WHICH FREQUENCY IS NEEDED AND GET MAX PERIOD


        # GROUP BY TO GET OUTPUT
        # temp2 = childrenDF.groupby(
        #                            ).agg({'OBS_VALUE_P': "sum"}).reset_index()

        temp_list = [childrenDF.OBS_VALUE_P.idxmin(), childrenDF.OBS_VALUE_P.idxmax()] # Get index of min and max value
        print(temp_list)
        min_max = childrenDF.iloc[temp_list].sort_values(by=self.values) # get rows having min max index

        # design output
        return_cols = self.time_period + self.lookups + ['OBS_VALUE', 'MIN/MAX']
        min_max.insert(len(return_cols) - 1, 'MIN/MAX', ['Minimum', 'Maximum'], True) #Add column with min max tag

        return min_max[return_cols]


    # def minmax(self, ref_dict):
    #     if ref_dict['P_ID'].isnull().all():
    #         childrenDF = self.table
    #
    #     else:
    #         en_dict = ref_dict.dropna(subset=['CL_ID_P'], axis=0)
    #         # create a dictionary having parents 'CL_ID_P','ID_P','DESCRIPTION_P'
    #         en_dict = en_dict[~en_dict['CL_ID_P'].str.contains('ar')]
    #         print(en_dict)
    #         parentDF = en_dict.dropna(subset=['P_ID_P'])[['CL_ID_P', 'P_ID_P']].drop_duplicates()
    #         parentDF = parentDF.merge(en_dict[['CL_ID_P', 'ID_P', 'DESCRIPTION_P']], how='left',
    #                                   left_on=['CL_ID_P', 'P_ID_P'], right_on=['CL_ID_P', 'ID_P'])
    #         print(parentDF)
    #         mask = pd.DataFrame()
    #
    #         for i in en_dict['CL_ID_P'].str.upper().unique():
    #             print(i)
    #             col_name = i+'_P'
    #             desc_list = parentDF[parentDF['CL_ID_P'].str.upper() == i]['DESCRIPTION_P'].tolist()
    #             mask[col_name] = self.table[col_name].isin(desc_list)
    #         print(mask)
    #         full_mask = ~mask.any(axis=1)
    #         print(full_mask)
    #
    #         childrenDF = self.table.assign(mask=full_mask)
    #         childrenDF = childrenDF[childrenDF['mask'] == True].drop(columns='mask').reset_index(drop=True)
    #         print(childrenDF)
    #
    #     temp_list = [childrenDF.OBS_VALUE_P.idxmin(), childrenDF.OBS_VALUE_P.idxmax()] # Get index of min and max value
    #     print(temp_list)
    #     min_max = childrenDF.iloc[temp_list].sort_values(by=self.values) # get rows having min max index
    #
    #     # design output
    #     return_cols = self.time_period + self.lookups + ['OBS_VALUE', 'MIN/MAX']
    #     min_max.insert(len(return_cols) - 1, 'MIN/MAX', ['Minimum', 'Maximum'], True) #Add column with min max tag
    #
    #     return min_max[return_cols]

    def changes(self):
        group_list = self.lookups_en + [self.values, self.date,'MEASURE_ID_P']
        # IF ONLY ONE PERIOD AVAILABLE RETURN MESSAGE
        if len(set(self.table[self.date])) == 1:
            diff = pd.DataFrame({'MESSAGE': ['Only one time period changes not applicable']})
            freq = pd.DataFrame({'MESSAGE': ['Only one time period frequency not applicable']})
            return diff, freq

        # GET DIFFERENCE AND PERCENTAGE DIFFERENCE
        order_list = self.lookups_en + ['MEASURE_ID_P', self.date]
        diff = self.table[group_list].sort_values(by=order_list).reset_index(drop=True)
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
        freq = diff[[self.date, 'FREQUENCY']].drop_duplicates(subset=[self.date, 'FREQUENCY'])

        return diff, freq

    def totals_new(self, ref_dict):
        if ref_dict['P_ID'].isnull().all():
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
            print(sum_by_list + groupby_fixed)
            temp2 = full_table.groupby(sum_by_list+groupby_fixed).agg({'OBS_VALUE_P': "sum"}).reset_index()
            print(temp2)
            temp2.columns = [i.replace('_ParentID', '_ID') for i in temp2.columns]
            actual_totals = actual_totals.append(temp2, ignore_index=True)

        on = [i for i in actual_totals.columns if not i.__contains__('OBS')]

        actual_totals = actual_totals.drop_duplicates()

        totals = actual_totals.merge(full_table,on=on, how='left', suffixes=('_L', '_R'))

        totals['CALCULATED - TOTALS'] = totals['OBS_VALUE_P_L'] - totals['OBS_VALUE_P_R']

        out_cols = self.lookups + self.time_period + ['OBS_VALUE_P_L','OBS_VALUE_P_R','CALCULATED - TOTALS']
        totals = totals[out_cols]

        totals = totals.rename(columns={'OBS_VALUE_P_L': 'CALCULATED TOTALS', 'OBS_VALUE_P_R': 'REPORTED TOTALS'}, inplace = False)

        return totals

    def getPredDiscrepancies_test(self, curr_table, old_table):
        if old_table.empty:
            PredDisc = pd.DataFrame({'MESSAGE': ['No previously inserted data for this table']})
        else:
            columns_P = [i for i in old_table.columns if i.upper().endswith('_P')]
            columns_output = [i for i in old_table.columns if not i.upper().endswith('_P')]

            joint_table = curr_table.append(old_table, ignore_index=True)
            PredDisc = joint_table.drop_duplicates(subset=columns_P, keep=False)
            PredDisc = PredDisc[columns_output]

            if PredDisc.empty:
                PredDisc = pd.DataFrame({'MESSAGE': ['No predecessor discrepancies']})

        return PredDisc
