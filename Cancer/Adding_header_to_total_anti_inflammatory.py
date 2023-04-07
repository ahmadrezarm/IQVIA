import pandas as pd
import numpy as np


anti_inflammatory_df = pd.read_csv("E:/Cancer Project/Data/anti_inflammatory_data/anti_inflammatory/total_anti_inflammatory_df.csv")

print(anti_inflammatory_df['0'].unique().shape)

print(anti_inflammatory_df.columns)

anti_inflammatory_df.drop(columns = ['Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0.1.1'], inplace = True)


columns_list = pd.read_csv('C:/Users/rakovski/Dropbox (Chapman)/IQVIA/header/header_claims_2006.csv').iloc[0].tolist()
print(columns_list)

anti_inflammatory_df.columns = columns_list

print(anti_inflammatory_df.head())

anti_inflammatory_df.to_csv("E:/Cancer Project/Data/anti_inflammatory_data/anti_inflammatory/total_anti_inflammatory_df_with_header.csv")


