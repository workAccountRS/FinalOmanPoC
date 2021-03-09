import re
import pandas as pd
from ValidationRules import ValidationRules

check = ValidationRules()


class Preprocess:

    # THIS FUNCTION WILL RETURN ORG_DF+PREPROC_DF HAVING PREPROCESSED TEXT FOR ALL COLUMNS
    # PREPROCESSED COLUMNS ARE DEFINED BY APPENDING '_P' TO ORIGINAL NAME
    def initialPrep(self,df):
        df_P = df.apply(lambda x: None if x is 'None' else x.astype(str).str.strip())  # Strip L and R space
        df_P = df_P.apply(lambda x: None if x is 'None' else x.astype(str).str.lower())  # Strip L and R space
        df_P = df_P.apply(lambda x: None if x is 'None' else x.astype(str).str.replace("\s\s+", " "))  # Make whitespace into one space
        df_P = df_P.apply(lambda x: None if x is 'None' else x.astype(str).str.replace('[ًٌٍَُِّْٰٓ]+', ""))  # Remove تشكيل
        df_P = df_P.apply(lambda x:  None if x is 'None' else x.astype(str).str.replace('[أآإ]+', 'ا'))  # Remove arabic special char
        df_P.columns = [str(col) + '_P' for col in df_P.columns]  # Append _P to preprocessed columns
        df_P = df_P.replace({'none': None})
        df_out = pd.concat([df, df_P], axis=1) # Join original columns to preprocessed columns
        try:
            df_out = df_out.drop(['TIME_STAMP_P','BATCH_ID_P'],axis=1)
        except:
            pass
        return df_out

    def matchPattern(self, text, pattern):
        try:
            match = re.search(pattern, text, re.IGNORECASE).group()
        except:
            match = None

        return match

    def getMonth(self, column):

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
                output.append(y)
            except:
                output.append(None)
        return output

    def getDate(self, month_list=None, year_list=[]):
        if month_list is not None:
            date = []
            for i in range(len(month_list)):
                try:
                    date.append(month_list[i] + '-' + str(year_list[i]))
                except:
                    date.append(None)

            output = pd.to_datetime(date, errors='coerce')

        else:
            output = pd.to_datetime(year_list, format='%Y', errors='coerce')

        return output

    def getNumeric(self, column):
        output = []
        try:
            for i in column:
                n = re.sub('[^0-9-+.]+', '', i)
                if len(n) == 0:
                    output.append(None)
                else:
                    output.append(float(n))
        except:
            output.append(None)

        return output

    def prepDatesAndValues(self, df):

        # freq_val = 1 if yearly otherwise 0
        freq_val = df['FREQUENCY'][df.FREQUENCY.first_valid_index()].upper()
        freq_val = 1 if freq_val.__contains__('YEAR') or freq_val.__contains__('ANNUAL') else 0

        # convert obs_value_p to numeric values
        df['OBS_VALUE_P'] = pd.to_numeric(self.getNumeric(df['OBS_VALUE_P']))

        # get TIME_PERIOD_DATE_P based on TIME_PERIOD_Y_P and TIME_PERIOD_Y_P
        if freq_val == 1:
            df['TIME_PERIOD_Y_P'] = pd.to_numeric(self.getYear(df['TIME_PERIOD_Y_P']))
            df = df.assign(TIME_PERIOD_DATE_P=self.getDate(year_list=df['TIME_PERIOD_Y_P']))
        else:
            df['TIME_PERIOD_M_P'] = self.getMonth(df['TIME_PERIOD_M_P'])
            df['TIME_PERIOD_Y_P'] = pd.to_numeric(self.getYear(df['TIME_PERIOD_Y_P']))
            df = df.assign(TIME_PERIOD_DATE_P=self.getDate(df['TIME_PERIOD_M_P'], df['TIME_PERIOD_Y_P']))

        # get PUBLICATION_DATE_AR_P and PUBLICATION_DATE_EN_P
        df = df.assign(PUBLICATION_DATE_AR_P=
                             self.getDate(self.getMonth(df['PUBLICATION_DATE_AR_P']),
                                          self.getYear(df['PUBLICATION_DATE_AR_P'])))
        df = df.assign(PUBLICATION_DATE_EN_P=
                             self.getDate(self.getMonth(df['PUBLICATION_DATE_EN_P']),
                                          self.getYear(df['PUBLICATION_DATE_EN_P'])))

        return df

    def getPredDiscrepancies(self, curr_table, old_table):
        if old_table.empty:
            PredDisc = pd.DataFrame({'MESSAGE': ['No previously inserted data for this table']})
        else:
            columns_P = [i for i in old_table.columns if i.upper().endswith('_P')]
            print(columns_P)
            columns_output = [i for i in old_table.columns if not i.upper().endswith('_P')]
            print(columns_output)

            joint_table = curr_table.append(old_table, ignore_index=True)
            PredDisc = joint_table.drop_duplicates(subset=columns_P, keep=False)
            #PredDisc = PredDisc[columns_output]

            if PredDisc.empty:
                PredDisc = pd.DataFrame({'MESSAGE': ['No predecessor discrepancies']})

        return PredDisc
