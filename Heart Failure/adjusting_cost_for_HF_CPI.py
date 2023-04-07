import pandas as pd
import numpy as np
import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd

cpi_df = pd.read_csv("C:/Users/rakovski\Dropbox (Chapman)/HeartFailure/Data/HeartFailure/CPI.csv")

print(cpi_df)
cpi_df['year'] = cpi_df['year'].astype(float)

print(cpi_df.dtypes)


folders = ['Pure']  # 'Inclusive',

# 'enroll_synth', 'enroll2_2006', 'enroll2_2007', 'enroll2_2008', 'enroll2_2009', 'enroll2_2010', 'enroll2_2011', 'enroll2_2012',
# 'enroll2_2013', 'enroll2_2014', 'enroll2_2015', 'enroll2_2016', 'enroll2_2017', 'enroll2_2018', 'enroll2_2019', 'enroll2_2020',
# 'enroll2_2021', 'enroll2_2022', 'header', 'pp_dx_lookup', 'pp_pos_lookup', 'pp_pr_lookup', 'pp_rev_lookup', 'pp_rx_lookup'

extension = ".csv"

i = 0  # just to count how many files we have

for folder in folders:
    dir_name = 'C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/'+folder
    os.chdir(dir_name)  # change directory from working dir to dir with files
    adjusted_df = pd.DataFrame()

    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)
            df = pd.read_csv(file_name)
            print(df.dtypes)

            cols_to_select = [col for col in df.columns if("cost" in col)]  #  or ("0" in col)
            df = pd.merge(df,cpi_df, on = "year" )
            df['inflation_factor'] = df["CPI"] / df["CPI"][0]
            for column in cols_to_select:
                df[column] = df[column].astype(float)
                df[f"cpi_adjusted{column}"] = df[column] / df["inflation_factor"]
        df.to_csv(f"CPI_adjusted_{name}.csv")

