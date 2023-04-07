import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

# Here, I am pulling out all the data based on pat_id who took Aspirin prescription at least once.
# Next step (not in this file): We are going to match: If they have claims for hospital costs in the same day they claimed aspirin, we get them out, they are out treatment group.

aspirin_df = pd.read_csv(f"E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_df.csv")

print(len(set(aspirin_df['0'])))

pat_ids = list(set(aspirin_df['0']))

folders = ['claims_2006', 'claims_2007', "claims_2008", "claims_2009", "claims_2010", "claims_2011", 'claims_2012',
           'claims_2013',
           'claims_2014', 'claims_2015', 'claims_2016', 'claims_2017', 'claims_2018', 'claims_2019', 'claims_2020',
           'claims_2021', 'claims_2022']

extension = ".csv"

i = 0  # just to count how many files we have
year = 2006
for folder in folders:
    dir_name = 'E:/IQVIA/Data/'+folder
    os.chdir(dir_name)  # change directory from working dir to dir with files
    aspirin_took_df = pd.DataFrame()

    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):  # check for ".csv" extension
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)
            df = pd.read_csv(file_name)
            # now we have the csv file. Lets filter things out:
            aspirin_took_df = pd.concat([aspirin_took_df, df.loc[df['0'].isin(pat_ids), :]],ignore_index=True)
            i += 1
            print(i)
            print(len(aspirin_took_df))
    aspirin_took_df.to_csv(f"E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data/{year}_aspirin_df.csv")
    year += 1


