import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

extension = ".csv"
i = 0
dir_name = "E:/Cancer Project/Data/anti_inflammatory_data/non_anti_inflammatry"
os.chdir(dir_name)  # change directory from working dir to dir with files
ids_df = pd.DataFrame()

for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):  # check for ".csv" extension
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)

            df = pd.read_csv(file_name)
            # now we have the csv file. Lets filter things out:
            ids_df = pd.concat([ids_df, df],ignore_index=True)
            print(len(ids_df))

            i += 1
            print(i)
ids_list = ids_df['pat_id'].tolist()
unique_ids_list = list(set(ids_list))
unique_ids_df = pd.DataFrame(unique_ids_list, columns = ['pat_id'] )
unique_ids_df.to_csv(f"{dir_name}/total_unique_pat_ids_df.csv")
five_percent_sample = unique_ids_df.sample(frac = 0.05, replace = False)
print(len(five_percent_sample))
five_percent_sample.to_csv(f"{dir_name}/five_percent_sample_unique_pat_ids.csv")
print(len(unique_ids_df))
