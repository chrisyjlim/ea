import pandas as pd
import numpy as np
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, MatchesPatternValidation
from decimal import Decimal, DecimalException, InvalidOperation
    
def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True

def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True
    
def check_bool(b):
    try:
        bool(b)
    except ValueError:
        return False
    return True
    
def check_fibre(s):
    try:
        return s.str.match(r'(^\w*(\d|_)*\-\w*\-\d)')
    except ValueError:
        return False
    return False

decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]
bool_validation = [CustomElementValidation(lambda b: b is not np.nan, 'this is not an boolean')]
fibre_validation = [CustomElementValidation(lambda s: s is not np.nan, 'this is not matching')]


			
			
		