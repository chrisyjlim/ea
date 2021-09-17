import pytest
import ea
import json
import pandas as pd
import math
from pandas._testing import assert_frame_equal

class TestClass:

    def test_validate(self):
        src=pd.read_csv("testdata/test_validate_src.csv", dtype=str)
        src.columns = src.columns.str.strip()
        src=ea.validate_data(src)
        tgt=pd.read_csv("testdata/test_validate_tgt.csv", dtype=str)
        tgt.columns = tgt.columns.str.strip() 
        assert_frame_equal(src, tgt)

        
    #checks that the JSON load method
    def test_json_file(self):
        src=pd.read_csv("testdata/test_json_src.csv")
        src.columns = src.columns.str.strip()
        tgt=ea.read_JSON_to_DF("testdata/test_json")
        assert_frame_equal(src, tgt)
        
    #test that based on the batch and number of rows, the number of files/rows is correct
    def test_batch(self):
        batch_size = 3
        src=pd.read_csv("testdata/test_batch_src.csv", dtype=str)
        src_rows = len(src.index)
        tgt_rows = 0
        row_check = True
        first = True
        num_files = math.ceil(src_rows / batch_size)
        
        final_file_row = src_rows % batch_size
        file_list = ea.output_batch(src, batch_size, "testdata/test_batch")
        file_list.reverse()
        print(file_list)
        
        for file in file_list:
            
            row_count = sum(1 for line in open(file))
            tgt_rows += row_count
            if(first):
                first = False
                if(final_file_row != row_count):
                    
                    row_check = False
            else:
                if(row_count != batch_size):
                    row_check = False
        assert num_files == len(file_list) and src_rows == tgt_rows and row_check
        