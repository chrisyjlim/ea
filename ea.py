import pandas as pd
import os
import hashlib
import glob
import sys
import file_schema_util
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation, MatchesPatternValidation, DateFormatValidation
from file_schema_util import decimal_validation, int_validation, null_validation, bool_validation, fibre_validation 
from decimal import Decimal, DecimalException, InvalidOperation

#validate data based on following rules and return valid records
def validate_data(df): 

    df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
    
    schema = pandas_schema.Schema([
    Column('Account_ID', int_validation + null_validation),
    Column('CODE', int_validation + null_validation),
    Column('Implemented Date',  [DateFormatValidation('%d/%m/%Y %H:%M')]),
    Column('Active Indicator', decimal_validation),
    Column('Account Type', null_validation),
    Column('Service', null_validation),
    Column('BU', null_validation),
    Column('Request Date',  [DateFormatValidation('%d/%m/%Y %H:%M')]),
    Column('Account status', null_validation),
    Column('Status Code', int_validation + null_validation),
    Column('$ Amount', decimal_validation + null_validation),
    Column('Version', null_validation),
    Column('Agent ID', int_validation + null_validation),
    Column('FIBRE', [ MatchesPatternValidation(r'(^\w*(\d|_)*\-\w*\-\d)')]),
    Column('last Updated Date', [DateFormatValidation('%d/%m/%Y %H:%M')]),
    Column('Property TYPE', null_validation),
    Column('Post Code', int_validation + null_validation)
    
    ])

    errors = schema.validate(df)
    errors_index_rows = [e.row for e in errors]
    
    #for error in errors:
    #    print(error)
    
    valid_data = df.drop(index=errors_index_rows).reset_index(drop=True)
    
    return valid_data
    

def calculate_hash(df):
    #calculate and apply MD5 hash of Account_ID and CODE 
    df['hash'] = (df['Account_ID'].map(str) + '-' + df['FIBRE'].map(str)).apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    return df

    
#output the dataframe to JSON files 
def output_batch(df, batch_size, output_dir):
    batch = 0
    #open a dummy file to declare the handler
    
    file_list = []
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    file_handle = open("{}/1.json".format(output_dir, batch), "w+")
    
    for i in df.index:
        #create a new file each time the record number (from 1) mod batch size is 0
        if((i)%batch_size == 0):         
            
            batch+=1
            if(file_handle != None):
                file_handle.close()
            file_handle = open("{}/{}.json".format(output_dir, batch), "w+")
            file_list.append("{}/{}.json".format(output_dir, batch))
        
        file_handle.write(df.loc[i].to_json() + "\n")

    return file_list
    
#reads all JSON files from directory into a single dataframe
def read_JSON_to_DF(directory):

    files = glob.glob(os.path.join(directory, "*.json"))     
    df_file = (pd.read_json(f, lines=True) for f in files)
    df   = pd.concat(df_file, ignore_index=True)
    return df


def get_top_amount_by_agent_postcode(df):

    result = df.groupby(['Agent ID','Post Code']).agg(sum=("$ Amount", "sum"))
    result = result.sort_values(by='sum', ascending=False, na_position='first')
    
    return result
    
#return postcodes sorted by fastest cumulative response time
def get_top_response_postcodes(df):
    
    #cast the string to date
    df['Implemented Date'] = pd.to_datetime(df['Implemented Date'], format='%d/%m/%Y %H:%M')
    df['Request Date'] = pd.to_datetime(df['Request Date'], format='%d/%m/%Y %H:%M')
    df["responseTime"] = (df["Implemented Date"].values - df["Request Date"].values)

    #get the median response (response time / number of jobs)
    result = df.groupby(['Post Code']).agg(totalResponseTime=("responseTime", "median"))
    result = result.sort_values(by='totalResponseTime', ascending=True)
    
    return result

    
def process_file(input_filename, output_directory):

    
    df=pd.read_csv(input_filename, dtype=str)
    df.columns = df.columns.str.strip()
    df=validate_data(df)
    df=calculate_hash(df)
    output_batch(df, 1000, output_directory) 
    
    return df



if __name__=='__main__':
    #if there aren't 2 arguments specified exit immediately
    if (len(sys.argv) != 3):
        print("usage - ea.py <input_file> <output_dir>")
        quit()
    
    input_file = (str(sys.argv[1]))
    output_dir = str(sys.argv[2])
    
    process_file(input_file, output_dir)
    
    df = read_JSON_to_DF(output_dir)
    print(get_top_response_postcodes(df))
    print(get_top_amount_by_agent_postcode(df))
    