import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np




extension = ".csv"

codes_df = pd.read_csv("E:/Sidy Breast Cancer/Data/cancer_codes.csv")
codes_df['Codes'] = codes_df['Codes'].str.replace('.', '')
code_list = codes_df['Codes'].values.tolist()
print(code_list)

i = 0  # just to count how many files we have
year = 2006

dir_name = "E:/Cancer Project/Data/Cancer data"
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
            print(len(cancer_df))
            i += 1
            print(i)
            cancer_df.to_csv(f"E:/Sidy Breast Cancer/Data/{year}_breast_cancer_df.csv")
            year += 1


