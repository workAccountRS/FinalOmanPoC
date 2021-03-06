import datetime


# This will return true false booleans based on check

class ValidationRules:

    def notnull(self, input):
        return input is not None

    def data_type(self, input, data_type="text", date_format=None):
        if data_type == "text":
            return isinstance(input, str)

        elif data_type == "number":
            try:
                float(input)
                return True
            except:
                return False

        elif data_type == "date":
            try:
                datetime.datetime.strptime(input, date_format)
                return True
            except ValueError:
                return False

    def lang(self, input, lang='ar'):
        if input is not None:
            input = str(input)
            if lang == 'ar':
                alphabet = ['ا', 'ب', 'ي', 'و', 'ه', 'ن', 'م', 'ل', 'ك', 'ق', 'ف', 'غ', 'ع', 'ظ', 'ط', 'ض', 'ص',
                            'ش', 'س', 'ز', 'ر', 'ذ', 'د', 'خ', 'ح', 'ج', 'ث', 'ت']
            elif lang == 'en':
                alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
                            'r', 's', 't', 'u', 'v', 'w', 's', 'y', 'z']

            for i in input:
                if isinstance(i, str):
                    for j in alphabet:
                        if j in i:
                            return True
        return False

    def freq(self, input, prev=None, freq=None):
        if prev is None:
            return True

        else:
            return abs(prev - input) == freq

    def check_dict(self, input, col_name, lookup):
        lst = [str(input).__contains__(i) for i
               in [row['DESCRIPTION_P'] for col, row in lookup.iterrows() if row['CL_ID_P'] == col_name[:-2].lower()]]

        if any(lst):
            return True
        else:
            return False