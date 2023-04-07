import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

folders = ['claims_2006', 'claims_2007', "claims_2008", "claims_2009", "claims_2010", "claims_2011", 'claims_2012',
           'claims_2013',
           'claims_2014', 'claims_2015', 'claims_2016', 'claims_2017', 'claims_2018', 'claims_2019', 'claims_2020',
           'claims_2021', 'claims_2022']

# 'enroll_synth', 'enroll2_2006', 'enroll2_2007', 'enroll2_2008', 'enroll2_2009', 'enroll2_2010', 'enroll2_2011', 'enroll2_2012',
# 'enroll2_2013', 'enroll2_2014', 'enroll2_2015', 'enroll2_2016', 'enroll2_2017', 'enroll2_2018', 'enroll2_2019', 'enroll2_2020',
# 'enroll2_2021', 'enroll2_2022', 'header', 'pp_dx_lookup', 'pp_pos_lookup', 'pp_pr_lookup', 'pp_rev_lookup', 'pp_rx_lookup'

extension = ".csv"

dir_name1 = "C:/Users/rakovski/Dropbox (Chapman)/IQVIA/Cancer Paper/Data"
os.chdir(dir_name1)
codes_df = pd.read_csv("Merge_cancer_codes.csv")
code_list = codes_df['code'].values.tolist()

i = 0  # just to count how many files we have
year = 2006
for folder in folders:
    dir_name = 'E:/IQVIA/Data/'+folder
    os.chdir(dir_name)  # change directory from working dir to dir with files
    cancer_df = pd.DataFrame()

    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):  # check for ".csv" extension
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)
            # print(file_name)
            # print(os.path.splitext(file_name)[0])
            # , compression='gzip', header=None,    sep='|', quotechar='"', error_bad_lines=False
            df = pd.read_csv(file_name)
            # now we have the csv file. Lets filter things out:
            cancer_df = pd.concat([cancer_df, df.loc[(np.any(df.iloc[:, 22:34].isin(code_list), axis=1)), :]],ignore_index=True)
            i += 1
            print(i)
    cancer_df.to_csv(f"{year}_cancer_df.csv")
    year += 1


