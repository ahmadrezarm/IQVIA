import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

extension = ".csv"
i = 0
dir_name = "E:/Cancer Project/Data/anti_inflammatory_data/anti_inflammatory"
os.chdir(dir_name)  # change directory from working dir to dir with files
anti_inflammatry_df = pd.DataFrame()

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
            anti_inflammatry_df = pd.concat([anti_inflammatry_df, df],ignore_index=True)
            print(len(anti_inflammatry_df))

            #no_anti_inflammatry_df = pd.concat([no_anti_inflammatry_df, df.loc[(df.iloc[:, 9].isin(code_list), axis=1), :]],ignore_index=True)
                                                                # ComCliamEnrollDF.loc[(ComCliamEnrollDF.iloc[:, 21].isin(DirectCodeList)
            i += 1
            print(i)
anti_inflammatry_df.to_csv("E:/Cancer Project/Data/anti_inflammatory_data/anti_inflammatory/total_anti_inflammatory_df.csv")


