import pandas as pd
import numpy as np


df = pd.read_csv("E:/Cancer Project/Data/anti_inflammatory_data/anti_inflammatory/total_anti_inflammatory_df.csv")

print(df['0'].unique().shape)

print(df.columns)

df.drop(columns = ['Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0.1.1'], inplace = True)

print(df.columns)