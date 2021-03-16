import re
import pandas as pd
from ValidationRules import ValidationRules

check = ValidationRules()


class Preprocess:

    # THIS FUNCTION WILL RETURN ORG_DF+PREPROC_DF HAVING PREPROCESSED TEXT FOR ALL COLUMNS
    # PREPROCESSED COLUMNS ARE DEFINED BY APPENDING '_P' TO ORIGINAL NAME
    def initialPrep(self, df):
        df_P = df.dropna(how='all')  # drop rows having all None values
        df_P = df_P.apply(lambda x: None if x is 'None' else x.astype(str).str.strip())  # Strip L and R space
        df_P = df_P.apply(lambda x: None if x is 'None' else x.astype(str).str.lower())  # lower case
        df_P = df_P.apply(lambda x: None if x is 'None' else x.astype(str).str.replace("\s\s+",
                                                                                       " "))  # Make whitespace into one space
        df_P = df_P.apply(
            lambda x: None if x is 'None' else x.astype(str).str.replace('[ًٌٍَُِّْٰٓ]+', ""))  # Remove تشكيل
        df_P = df_P.apply(
            lambda x: None if x is 'None' else x.astype(str).str.replace('[أآإ]+', 'ا'))  # Remove arabic special char
        df_P.columns = [str(col) + '_P' for col in df_P.columns]  # Append _P to preprocessed columns
        df_P = df_P.replace({'none': None})
        df_out = pd.concat([df, df_P], axis=1)  # Join original columns to preprocessed columns
        try:
            df_out = df_out.drop(['TIME_STAMP_P', 'BATCH_ID_P','SERIAL_DATA_LOAD_P'], axis=1)
        except:
            pass
        return df_out

    def matchPattern(self, text, pattern):
        try:
            match = re.search(pattern, text, re.IGNORECASE).group()
        except:
            match = None

        return match

    def getMonth(self, column, isDate):

        month_dict = {'اب': 'aug',
                      'اذار': 'mar',
                      'اغسطس': 'aug',
                      'اكتوبر': 'oct',
                      'ايار': 'may',
                      'ايلول': 'sep',
                      'ابريل': 'apr',
                      'تشرين الاول': 'oct',
                      'تشرين الثاني': 'nov',
                      'تموز': 'jul',
                      'حزيران': 'jun',
                      'ديسمبر': 'dec',
                      'سبتمبر': 'sep',
                      'شباط': 'feb',
                      'فبراير': 'feb',
                      'كانون الاول': 'dec',
                      'كانون الثاني': 'jan',
                      'مارس': 'mar',
                      'مايو': 'may',
                      'نوفمبر': 'nov',
                      'نيسان': 'apr',
                      'يناير': 'jan',
                      'يوليو': 'jul',
                      'يونيو': 'jun'}

        pattern = '|'.join([*month_dict.keys()]) + '|'.join(set(month_dict.values()))
        if isDate == 0:
            allmonths = [None if str(i).__contains__('-') else self.matchPattern(i, pattern) for i in column]
        else:
            allmonths = [self.matchPattern(i, pattern) for i in column]

        output = []
        for i in allmonths:
            if check.lang(i, 'ar'):
                output.append(month_dict[i])
            else:
                output.append(i)
        return output

    def getYear(self, column):
        output = []
        for i in column:
            try:
                extractYear = re.search(r'\d{4}', i)
                y = extractYear.group(0)
                output.append(int(y))
            except:
                output.append(None)
        return output

    def getDate(self, table, month_title, year_title):
        isDate = 0
        if month_title == year_title:
            isDate = 1

        month_list = self.getMonth(table[month_title], isDate)
        year_list = self.getYear(table[year_title])
        freq = table['FREQUENCY_P'].tolist()
        date = []

        for i in range(len(month_list)):
            # Year should be provided in all cases
            if pd.isna(year_list[i]):
                date.append(None)

            # frequency is relevant to time series
            elif str(freq[i]).__contains__('month') and isDate == 0:
                if pd.isna(month_list[i]):
                    date.append(None)
                else:
                    date.append(month_list[i] + '-' + str(year_list[i]))

            # frequency is irrelevant or frequency is not monthly
            else:
                if pd.isna(month_list[i]):
                    date.append(str(year_list[i]))
                else:
                    date.append(month_list[i] + '-' + str(year_list[i]))

        output = pd.to_datetime(date, errors='coerce')
        return output

    def getNumeric(self, column):
        output = []
        for i in column:
            if i == '-':
                output.append(None)
            else:
                try:
                    n = re.sub('[^0-9-+.]+', '', i)
                    if len(n) == 0:
                        output.append(None)
                    else:
                        output.append(float(n))
                except:
                    output.append(None)

        return output

    def prepDatesAndValues(self, df):

        # convert obs_value_p to numeric values
        df['OBS_VALUE_P'] = pd.to_numeric(self.getNumeric(df['OBS_VALUE_P']))

        # get TIME_PERIOD_DATE_P based on TIME_PERIOD_Y_P and TIME_PERIOD_Y_P
        df = df.assign(TIME_PERIOD_DATE_P=self.getDate(df, 'TIME_PERIOD_M_P', 'TIME_PERIOD_Y_P'))
        df['TIME_PERIOD_M_P'] = self.getMonth(df['TIME_PERIOD_M_P'], 0)
        df['TIME_PERIOD_Y_P'] = self.getYear(df['TIME_PERIOD_Y_P'])

        # get PUBLICATION_DATE_AR_P and PUBLICATION_DATE_EN_P
        df = df.assign(PUBLICATION_DATE_AR_P=
                       self.getDate(df, 'PUBLICATION_DATE_AR_P', 'PUBLICATION_DATE_AR_P'))
        df = df.assign(PUBLICATION_DATE_EN_P=pd.to_datetime(df['PUBLICATION_DATE_EN_P'],errors='coerce'))
        return df

    def getPredDiscrepancies(self, curr_table, old_table):

        if old_table.empty:
            PredDisc = pd.DataFrame({'MESSAGE': ['No previously inserted data for this table']})
        else:
            columns_P = [i for i in old_table.columns if i.upper().endswith('_P')]
            columns_output = [i for i in old_table.columns if not i.upper().endswith('_P')]

            newYears = curr_table['TIME_PERIOD_Y_P'].unique().tolist()
            oldYears = old_table['TIME_PERIOD_Y_P'].unique().tolist()
            years = set(newYears).intersection(oldYears)
            print('common years', years)

            df_cur = curr_table[curr_table['TIME_PERIOD_Y_P'].isin(years)]
            df_old = old_table[old_table['TIME_PERIOD_Y_P'].isin(years)]

            joint_table = df_cur.append(df_old, ignore_index=True)

            #for debugging
            for i in columns_P:
                print('column', i)
                PredDisc = joint_table.drop_duplicates(subset=i, keep=False)
                print(PredDisc[i])
                # PredDisc = PredDisc[columns_output]

            PredDisc = joint_table.drop_duplicates(subset=columns_P, keep=False)
            PredDisc = PredDisc[columns_output]

            if PredDisc.empty:
                PredDisc = pd.DataFrame({'MESSAGE': ['No predecessor discrepancies']})

        return PredDisc
