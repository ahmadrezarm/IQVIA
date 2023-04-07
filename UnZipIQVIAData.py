import fnmatch
import numpy as np
import pandas as pd
import os
import tarfile
import gzip
import shutil
from joblib import Parallel, delayed

######################Unzippe the data############################
SourceFilepath ='C:\\Data\\IQVIA\\'
pattern = "*.csv.gz"
alldatafiles = np.empty((0,1))
for path, subdirs, files in os.walk(SourceFilepath):
    print(subdirs)
    for name in files:
        if fnmatch.fnmatch(name, pattern):
            print(name)
            #alldatafiles = np.append(alldatafiles,path+"\\"+name)
            zipfilepath = path+"\\"+name
            newpath = path.replace('IQVIA', 'IQVIA_Unzipped')
            newpath = newpath +'\\'
            newname = name.replace('.gz','')
            if not os.path.exists(newpath):
                os.makedirs(newpath)

            with gzip.open(zipfilepath, 'rb') as f_in:
                with open(newpath +"\\"+newname, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
######################Unzippe the data############################

################ Extract heart failure summary data##########################
DiagcodeSourceFilepath ='C:\\Data\\IQVIA_Unzipped\\pp_dx_lookup\\'

pattern = "*.csv"
DxcodeDF = pd.DataFrame()
alldatafiles = np.empty((0,1))
for path, subdirs, files in os.walk(DiagcodeSourceFilepath):
    print(subdirs)
    for name in files:
        if fnmatch.fnmatch(name, pattern):
            singleDF = pd.read_csv(path +"\\" + name, header=None, sep="|")
            DxcodeDF = pd.concat([DxcodeDF,singleDF], ignore_index=True)

Dxheaders= pd.read_csv("C:\\Data\\IQVIA_Unzipped\\header\\header_pp_dx_lookup.csv", header=None,sep='|')
DxcodeDF.columns = Dxheaders.iloc[0,:].to_list()

sum(DxcodeDF.iloc[:,2].str.contains('HEART FAILURE'))
DxcodeDF.loc[DxcodeDF.iloc[:,2].str.contains('HEART FAILURE'),'diagnosis_desc']
#DxcodeDF.loc[DxcodeDF.iloc[:,2].str.contains('HEART FAILURE'),:].to_csv('C:\\Data\\Individual Folders\\Jianwei Zheng\\HeartFailureData\\HeartFailureDXcode.csv', index=None)

#HFDxCodeDF = pd.read_csv('C:\\Data\\Individual Folders\\Jianwei Zheng\\HeartFailureData\\HeartFailureDXcode.csv')
HFDxCodeDF = pd.read_csv("E:/IQVIAStudy/Data/HeartFailureDXcode.csv")

#read all claims data about heart failure
SourceFilepath ='E:/IQVIAStudy/IQVIA_Dataset_20220613/'
pattern = "claims"
alldatafiles = np.empty((0,1))
for path, subdirs, files in os.walk(SourceFilepath):
    #print(subdirs)
    for name in files:
        if path.__contains__(pattern):
            print(path)
            if fnmatch.fnmatch(name, "*.csv"):
                alldatafiles = np.append(alldatafiles,path+"/"+name)


DxcodeList = HFDxCodeDF.iloc[:,0].to_list()
#for i in range(len(alldatafiles)):

def readsingleHFCalimsDF(filename, refDxcodeList):
    print(filename)
    singleClaimDF = pd.read_csv(filename, header=None, sep='|', dtype=str)
    #singleClaimDF.iloc[:,21:34]
    singleHFDF = singleClaimDF.loc[np.any(singleClaimDF.iloc[:, 22:34].isin(refDxcodeList), axis=1), :]
    #validPID = singleHFDF.loc[singleHFDF['rectype'].isin(['M', 'P', 'F', 'S']), 'pat_id'].unique()
    #singleHFDF = singleHFDF.loc[singleHFDF['pat_id'].isin(validPID), :]
    print(singleHFDF.shape)
    return singleHFDF

#HFClaimDF = pd.concat([HFClaimDF, singleHFDF], ignore_index=True)
num_cores = 20
HFCliamDFRes = Parallel(n_jobs=num_cores)(delayed(readsingleHFCalimsDF)(alldatafiles[iseg], DxcodeList) for iseg in range(len(alldatafiles)))

HFClaimDF = pd.DataFrame()
HFClaimDF = pd.concat(HFCliamDFRes, ignore_index=True)

#Claim2016headers= pd.read_csv("C:\\Data\\IQVIA_Unzipped\\header\\header_claims_2016.csv", header=None,sep='|')
Claim2022headers= pd.read_csv("E:/IQVIAStudy/IQVIA_Dataset_20220613/header/header_claims_2022.csv", header=None,sep='|')

HFClaimDF.columns = Claim2022headers.iloc[0,:].to_list()
HFClaimDF.to_csv('E:/IQVIAStudy/Data/HeartFailurClaimData20221012.csv', index=None)
del HFCliamDFRes
################ Extract heart failure summary data##########################

HFClaimDF = pd.read_csv('C:\\Data\\Individual Folders\\Jianwei Zheng\\HeartFailureData\\HeartFailurClaimData.csv')
