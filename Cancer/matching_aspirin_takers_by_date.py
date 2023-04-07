import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

###### neds to be edited. not the one we need!
# here, we are just getting out the patients from the all_aspirin_df who has multiple records of different record types.
# Next, we will check for the dates to get the actual treatment group.

aspirin_df = pd.read_csv("E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data/with_headers/total_aspirin_df.csv")

grouped = aspirin_df.groupby('pat_id')['rectype'].nunique()

patients_with_multiple_records = grouped[grouped > 1].index.tolist()

multiple_records_aspirin_df = aspirin_df[aspirin_df['pat_id'].isin(patients_with_multiple_records)]

multiple_records_aspirin_df.to_csv('E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data/with_headers/people_with_multiple_records_aspirin_df.csv')