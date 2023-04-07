# calculating pure costs:

# -*- coding: utf-8 -*-


import fnmatch
import numpy as np
import pandas as pd
import os
import tarfile
import gzip
import shutil
from joblib import Parallel, delayed


#Heart failure data
HFDxCodeDF = pd.read_csv("C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/HeartFailureDXcode.csv")
ComCliamEnrollDF = pd.read_csv("C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/HeartFailurClaimEnrollData20221212.csv", dtype=str)

ComCliamEnrollDF.loc[~ComCliamEnrollDF['ndc'].isna(),['ndc']]
ComCliamEnrollDF['dayssup'] = ComCliamEnrollDF['dayssup'].astype('float').astype('Int64')
ComCliamEnrollDF['quan'] = ComCliamEnrollDF['quan'].astype(float)
ComCliamEnrollDF['srv_unit'] = ComCliamEnrollDF['srv_unit'].astype(float)
ComCliamEnrollDF['from_dt'] = pd.DatetimeIndex(ComCliamEnrollDF['from_dt'])
ComCliamEnrollDF['to_dt'] = pd.DatetimeIndex(ComCliamEnrollDF['to_dt'])
ComCliamEnrollDF['allowed'] = ComCliamEnrollDF['allowed'].astype(float)
ComCliamEnrollDF['paid'] = ComCliamEnrollDF['paid'].astype(float)
ComCliamEnrollDF['deductible'] = ComCliamEnrollDF['deductible'].astype(float)
ComCliamEnrollDF['copay'] = ComCliamEnrollDF['copay'].astype(float)
ComCliamEnrollDF['coinsamt'] = ComCliamEnrollDF['coinsamt'].astype(float)
ComCliamEnrollDF['cobamt'] = ComCliamEnrollDF['cobamt'].astype(float)
ComCliamEnrollDF['dispense_fee'] = ComCliamEnrollDF['dispense_fee'].astype(float)
ComCliamEnrollDF['paid_dt'] = pd.DatetimeIndex(ComCliamEnrollDF['paid_dt'])
ComCliamEnrollDF['der_yob'] = ComCliamEnrollDF['der_yob'].astype('float').astype('Int64')
ComCliamEnrollDF['year'] = ComCliamEnrollDF['year'].astype('float').astype('Int64')
ComCliamEnrollDF['month'] = ComCliamEnrollDF['month'].astype('float').astype('Int64')



#After filter out only one anicllary type
#29344687
#print(ComCliamEnrollDF['pat_id_x'].unique().shape) #545129
DirectCodeList = HFDxCodeDF.loc[HFDxCodeDF['Direct_code']==1, :].iloc[:,0].to_list()
#admitted by HF, but did not find in diagnosis 60070 recorders
##print(ComCliamEnrollDF.loc[(ComCliamEnrollDF.iloc[:, 21].isin(DirectCodeList)) & (~np.any(ComCliamEnrollDF.iloc[:, 22:34].isin(DirectCodeList), axis=1)), :].shape)
#admitted by HF, but did not find in diagnosis 2844 patients
##print(ComCliamEnrollDF.loc[(ComCliamEnrollDF.iloc[:, 21].isin(DirectCodeList)) & (~np.any(ComCliamEnrollDF.iloc[:, 22:34].isin(DirectCodeList), axis=1)), :]['pat_id_x'].unique().shape)
#were not admitted by HF, but found HF 27392209 recorders
##print(ComCliamEnrollDF.loc[(~ComCliamEnrollDF.iloc[:, 21].isin(DirectCodeList)) & (np.any(ComCliamEnrollDF.iloc[:, 22:34].isin(DirectCodeList), axis=1)), :].shape)
#were not admitted by HF, but found HF  544095 patients
##print(ComCliamEnrollDF.loc[(~ComCliamEnrollDF.iloc[:, 21].isin(DirectCodeList)) & (np.any(ComCliamEnrollDF.iloc[:, 22:34].isin(DirectCodeList), axis=1)), :]['pat_id_x'].unique().shape)

#working all data related to Heart failure 29284617
ComCliamEnrollDF = ComCliamEnrollDF.loc[(np.any(ComCliamEnrollDF.iloc[:, 22:34].isin(DirectCodeList), axis=1)), :]
len(DirectCodeList)
#how many patients, gender, age, state, region, zip code, product type, pay type, pcob_type, mcob_type
##print(ComCliamEnrollDF['pat_id_x'].unique().shape) #545045
##print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['der_sex'].value_counts()) #male 270460
##print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['pat_state'].value_counts()) #male 260171
#ComCliamEnrollDF.drop_duplicates('pat_id_x')['pat_state'].value_counts().to_csv('E:/IQVIAStudy/Data/HFStates.csv')
##print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['pat_region'].value_counts()) #male 260171
#ComCliamEnrollDF.drop_duplicates('pat_id_x')['pat_region'].value_counts().to_csv('E:/IQVIAStudy/Data/HFRegions.csv')
##print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['pat_zip3'].value_counts()) #male 260171
#ComCliamEnrollDF.drop_duplicates('pat_id_x')['pat_zip3'].value_counts().to_excel('E:/IQVIAStudy/Data/HFZip3.xlsx')
#print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['prd_type'].value_counts().to_csv("/home/ahmad/Desktop/heart Faiure Data/Just run his code/prd_type.csv")) #male 260171
#print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['pcob_type'].value_counts().to_csv('/home/ahmad/Desktop/heart Faiure Data/Just run his code/pcob_type.csv')) #male 260171
#print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['mcob_type'].value_counts().to_csv('/home/ahmad/Desktop/heart Faiure Data/Just run his code/mcob_type.csv')) #male 260171
#print(ComCliamEnrollDF.drop_duplicates('pat_id_x')['pay_type'].value_counts().to_csv('/home/ahmad/Desktop/heart Faiure Data/Just run his code/pay_type.csv')) #male 260171


ComCliamEnrollDF[DirectCodeList] = 0
ComCliamEnrollDF[DirectCodeList] = ComCliamEnrollDF.iloc[:, 22:34].apply(lambda x: pd.Series(np.isin(DirectCodeList, x).astype(int)), axis=1)





code_list = ['I503', 'I5030', 'I5031', 'I5032', 'I5033', 'I504', 'I5040', 'I5041', 'I5042', 'I5043', 'I502', 'I5021', 'I5022', 'I5023']

for code in code_list:
  code_list2 = ['I503', 'I5030', 'I5031', 'I5032', 'I5033', 'I504', 'I5040', 'I5041', 'I5042', 'I5043', 'I502', 'I5021', 'I5022', 'I5023']
  print(code)
  code_list2.remove(code)
  print(code_list2)
  ComCliamEnrollDF["trget_col"] = 0
  ComCliamEnrollDF["trget_col"] = ((ComCliamEnrollDF[code] > 0) & (ComCliamEnrollDF[code_list2].sum(axis=1) == 0)).astype(int)
  (ComCliamEnrollDF.groupby(['trget_col', 'year'])['allowed'].sum() / ComCliamEnrollDF.groupby(['trget_col', 'year'])['pat_id_x'].nunique()).to_csv(f"C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/Pure/{code}_pure.csv")