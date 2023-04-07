import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

extension = ".csv"
i = 0
dir_name = "E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data"
os.chdir(dir_name)  # change directory from working dir to dir with files

aspirin_df = pd.DataFrame()

for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):  # check for ".csv" extension
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)

            df = pd.read_csv(file_name)
            # now we have the csv file. Lets filter things out:
            aspirin_df = pd.concat([aspirin_df, df],ignore_index=True)
            print(len(aspirin_df))
            i += 1
            print(i)
aspirin_df.to_csv("E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data/with_headers/total_aspirin_df.csv")


