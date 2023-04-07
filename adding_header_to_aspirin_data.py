import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np


columns_list = pd.read_csv('C:/Users/rakovski/Dropbox (Chapman)/IQVIA/header/header_claims_2006.csv').iloc[0].tolist()
print(columns_list)
extension = '.csv'

dir_name = "E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data"
os.chdir(dir_name)  # change directory from working dir to dir with files

for item in os.listdir(dir_name):  # loop through items in dir
    if item.endswith(extension):  # check for ".csv" extension
        file_name = os.path.abspath(item)  # get full path of files
        name = Path(file_name).stem
        print(name)
        df = pd.read_csv(file_name)
        print(df.columns)
        df.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'], inplace=True)
        df.columns = columns_list
        print(df.columns)
        df.to_csv(f"E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data/with_headers/{name}.csv")


