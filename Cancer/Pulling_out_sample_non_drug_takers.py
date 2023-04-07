import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

# first, people who never got prescription for anti_inflammatory.
# We need to groupby pat_id. But, we can say that if a person never got such prescription, the drug codes never appeared on the ndc column.
# So, we can pull our the data for rows without anti_inflamatory ndc code, and then groupby pat_id

folders = ['claims_2006', 'claims_2007', "claims_2008", "claims_2009", "claims_2010", "claims_2011", 'claims_2012',
           'claims_2013', 'claims_2014', 'claims_2015', 'claims_2016', 'claims_2017', 'claims_2018', 'claims_2019', 'claims_2020',
           'claims_2021', 'claims_2022']

# 'enroll_synth', 'enroll2_2006', 'enroll2_2007', 'enroll2_2008', 'enroll2_2009', 'enroll2_2010', 'enroll2_2011', 'enroll2_2012',
# 'enroll2_2013', 'enroll2_2014', 'enroll2_2015', 'enroll2_2016', 'enroll2_2017', 'enroll2_2018', 'enroll2_2019', 'enroll2_2020',
# 'enroll2_2021', 'enroll2_2022', 'header', 'pp_dx_lookup', 'pp_pos_lookup', 'pp_pr_lookup', 'pp_rev_lookup', 'pp_rx_lookup'



patient_ids = pd.read_csv("E:/Cancer Project/Data/anti_inflammatory_data/non_anti_inflammatry/five_percent_sample_unique_pat_ids.csv")
print(patient_ids.dtypes)
#codes_df['ndc_codes'] = codes_df['ndc_codes'].astype(str)
#print(codes_df.dtypes)
patient_ids_list = patient_ids['pat_id'].values.tolist()


extension = ".csv"
i = 0  # just to count how many files we have
year = 2006

for folder in folders:
    dir_name = 'E:/IQVIA/Data/'+folder
    os.chdir(dir_name)  # change directory from working dir to dir with files
    sample_df = pd.DataFrame()

    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith(extension):  # check for ".csv" extension
            file_name = os.path.abspath(item)  # get full path of files
            name = Path(file_name).stem
            print(name)

            df = pd.read_csv(file_name)
            #print(df["0"])
            #print(df.columns)
            # now we have the csv file. Lets filter things out:
            sample_df = pd.concat([sample_df, df.loc[df['0'].isin(patient_ids_list), :]],ignore_index=True)
            print(len(sample_df))

            i += 1
            print(i)
    sample_df.to_csv(f"E:/Cancer Project/Data/anti_inflammatory_data/non_anti_inflammatry/five percent sample/{year}_non_anti_inflammatory_takers.csv")
    print("Got one year!")
    year += 1

