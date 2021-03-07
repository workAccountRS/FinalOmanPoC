import re
import pandas as pd
from ValidationRules import ValidationRules

check = ValidationRules()


class Preprocess:
    def __init__(self, table):
        self.table = table
        self.columns = self.table.columns
        self.freq = self.table['FREQUENCY'][self.table.FREQUENCY.first_valid_index()].upper()
        self.dates = ['PUBLICATION_DATE_AR', 'PUBLICATION_DATE_EN', 'TIME_PERIOD_Y', 'TIME_PERIOD_M']
        self.value = ['OBS_VALUE']

    def preptext(self, text):
        if text is None:
            output = None
        else:
            output = re.sub('[^A-Za-z0-9ء-ئا-ي ]+', '', re.sub('[أآإ]+', 'ا', str(text).lower()))

        return output

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
        allmonths = [self.matchPattern(self.preptext(i), pattern) for i in column]

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
                y = re.sub('[^0-9.]+', '', i)
                if len(y) == 4:
                    output.append(int(y))
                else:
                    output.append(None)
            except:
                output.append(None)

        print(output)
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

    def getObsVal(self, column):
        output = []
        try:
            for i in column:
                n = re.sub('[^0-9.]+', '', i)
                if len(n) == 0:
                    output.append(None)
                else:
                    output.append(float(n))
        except:
            output.append(None)

        return output

    def getPrepTable(self):
        for i in self.columns:
            self.table[i] = self.table[i].str.strip()

        input = self.table.assign(OBS_VALUE_P=self.getObsVal(self.table['OBS_VALUE']))
        if not self.freq.__contains__('YEAR'):
            input = input.assign(TIME_PERIOD_M_P=self.getMonth(input['TIME_PERIOD_M']))
            input = input.assign(TIME_PERIOD_Y_P=self.getYear(input['TIME_PERIOD_Y']))
            input = input.assign(TIME_PERIOD_DATE_P=self.getDate(input['TIME_PERIOD_M_P'], input['TIME_PERIOD_Y_P']))
        else:
            input = input.assign(TIME_PERIOD_Y_P=self.getYear(input['TIME_PERIOD_Y']))
            input = input.assign(TIME_PERIOD_DATE_P=self.getDate(input['TIME_PERIOD_Y_P']))

        input = input.assign(PUBLICATION_DATE_AR_P=
                             self.getDate(self.getMonth(input['PUBLICATION_DATE_AR']),
                                          self.getYear(input['PUBLICATION_DATE_AR'])))
        input = input.assign(PUBLICATION_DATE_EN_P=
                             self.getDate(self.getMonth(input['PUBLICATION_DATE_EN']),
                                          self.getYear(input['PUBLICATION_DATE_EN'])))

        return input
