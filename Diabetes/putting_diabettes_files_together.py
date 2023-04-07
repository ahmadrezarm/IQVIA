import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

extension = ".csv"
i = 0
dir_name = "E:/Diabetes Project/Data"
os.chdir(dir_name)  # change directory from working dir to dir with files
diabetes_df = pd.DataFrame()

for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):  # check for ".csv" extension
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)

            df = pd.read_csv(file_name)
            # now we have the csv file. Lets filter things out:
            diabetes_df = pd.concat([diabetes_df, df],ignore_index=True)
            print(len(diabetes_df))

            i += 1
            print(i)
diabetes_df.to_csv("E:/Diabetes Project/Data/total_diabetes_df.csv")

