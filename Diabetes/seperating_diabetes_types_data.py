import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np



type_1_df = pd.read_csv("E:/Diabetes Project/Type_1.csv")
type_1_df['type_1'] = type_1_df['type_1'].str.replace('.', '')
type_1_list = type_1_df['type_1'].values.tolist()
print(type_1_list)


type_2_df = pd.read_csv("E:/Diabetes Project/Type_2.csv")
type_2_df['type_2'] = type_2_df['type_2'].str.replace('.', '')
type_2_list = type_2_df['type_2'].values.tolist()
print(type_2_list)


extension = ".csv"
i = 0  # just to count how many files we have
dir_name = 'E:/Diabetes Project/Data'
os.chdir(dir_name)  # change directory from working dir to dir with files
type1 = pd.DataFrame()
type2 = pd.DataFrame()

for item in os.listdir(dir_name):  # loop through items in dir
    if item.endswith(extension):  # check for ".csv" extension
        file_name = os.path.abspath(item)  # get full path of files
        name = Path(file_name).stem
        print(name)
        df = pd.read_csv(file_name)
        type1 = pd.concat([type1, df.loc[(np.any(df.iloc[:, 22:35].isin(type_1_list), axis=1)), :]],ignore_index=True)
        ###type2 = pd.concat([type2, df.loc[(np.any(df.iloc[:, 22:35].isin(type_2_list), axis=1)), :]],ignore_index=True)

        i += 1
        print(i)
type1.to_csv("diabetes_type1.csv")
###type2.to_csv("diabetes_type2.csv")
