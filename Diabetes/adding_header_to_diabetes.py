import pandas as pd
import numpy as np


diabetes_df = pd.read_csv("E:/Diabetes Project/Data/diabetes_type2.csv")


print(diabetes_df.columns)

diabetes_df.drop(columns = ['Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0.1.1'], inplace = True)


columns_list = pd.read_csv('C:/Users/rakovski/Dropbox (Chapman)/IQVIA/header/header_claims_2006.csv').iloc[0].tolist()
print(columns_list)


diabetes_df.columns = columns_list

print(diabetes_df.columns)

diabetes_df.to_csv("E:/Diabetes Project/Data/diabetes_type2_with_headers.csv")


