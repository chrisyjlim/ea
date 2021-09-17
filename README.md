Developed and tested on Python 3.9.6

Dependencies - pytest, pandas, pandas-schema, numpy

usage - python ea.py <input_file> <output_directory>

Assumptions
- Account ID and FIBRE have been concatenated with | and hashed with md5
- Trailing whitespace removed from column header
- Blank columns removed

Attached pytest run data validation test cases and unit tests on methods. Test for Hash method was not done

List post codes based on fastest response  <- I interpreted this as the average response time for jobs across the postcode (eg 2 jobs (1 day, 3 days) would give a median response time of 2 days