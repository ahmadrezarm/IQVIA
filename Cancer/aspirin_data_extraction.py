import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np



codes_df = pd.read_csv("E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/NDC_Aspirin.csv")
print(codes_df.dtypes)
#codes_df['ndc_codes'] = codes_df['ndc_codes'].astype(str)
#print(codes_df.dtypes)
code_list = codes_df['ndc_codes'].values.tolist()

aspirin = pd.DataFrame()
df = pd.read_csv('E:/Cancer Project/Data/anti_inflammatory_data/anti_inflammatory/total_anti_inflammatory_df.csv')

aspirin = pd.concat([aspirin, df.loc[df['9'].isin(code_list), :]],ignore_index=True)
print(len(aspirin))

aspirin.to_csv(f"E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_df.csv")

