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

'''
ComCliamEnrollDF.groupby(['I509', 'pay_type', 'year'])['allowed'].sum() / ComCliamEnrollDF.groupby(['I509', 'pay_type', 'year'])['pat_id_x'].nunique()

GroupDF = ComCliamEnrollDF.groupby(['I509', 'year'])['allowed'].sum() / ComCliamEnrollDF.groupby(['I509', 'year'])['pat_id_x'].nunique()

for icode in DirectCodeList:
    print(icode)
    (ComCliamEnrollDF.groupby([icode, 'year'])['allowed'].sum() / ComCliamEnrollDF.groupby([icode, 'year'])['pat_id_x'].nunique())
'''

############### Save the datarfame in this point to prevent recalculations in the future:
#ComCliamEnrollDF.to_csv('C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/preprocessed_df.csv')
###############

#.to_csv('/home/ahmad/Desktop/heart Faiure Data/Just run his code/'+icode+'.csv')
# calculating pure costs:

ComCliamEnrollDF['I503G'] =0
ComCliamEnrollDF['I503G'] = ((ComCliamEnrollDF['I503'] +ComCliamEnrollDF['I5030'] + ComCliamEnrollDF['I5031']+ComCliamEnrollDF['I5032']+ComCliamEnrollDF['I5033'] >0)).astype(int)

ComCliamEnrollDF['I502G'] =0
ComCliamEnrollDF['I502G'] = (ComCliamEnrollDF['I502'] +ComCliamEnrollDF['I5020'] + ComCliamEnrollDF['I5021']+ComCliamEnrollDF['I5022']+ComCliamEnrollDF['I5023'] >0).astype(int)

# now the target:
ComCliamEnrollDF['I504G'] =0
ComCliamEnrollDF['I504G'] = (ComCliamEnrollDF['I504'] +ComCliamEnrollDF['I5040'] + ComCliamEnrollDF['I5041']+ComCliamEnrollDF['I5042']+ComCliamEnrollDF['I5043'] >0).astype(int)

#(ComCliamEnrollDF.groupby(['I503G', 'year'])['allowed'].sum() / ComCliamEnrollDF.groupby(['I503G', 'year'])['pat_id_x'].nunique()).to_csv('C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/I503G.csv')

"""# Cost Breakdown"""

# My stratgey implementation:
codes_needed = ['I502G', 'I503G', 'I504G']
for code in codes_needed:

    new_df = ComCliamEnrollDF.loc[(ComCliamEnrollDF[code] == 1), :]

    DirectComCliamEnrollDF = new_df

    TotalInfoDF = pd.DataFrame()
    for i in range(2006, 2022):
        num_patients = (DirectComCliamEnrollDF.loc[(DirectComCliamEnrollDF['year'] == i), :]['pat_id_x']).unique().shape[0]
        print(i, 'year', num_patients)
        # print(i, (ComCliamEnrollDF.loc[(ComCliamEnrollDF['year'] == i), ['paid']]).sum()/num_patients)
        # HFClaimDF2021 = ComCliamEnrollDF.loc[(ComCliamEnrollDF['year'] == i), :]

        singleHFDF = DirectComCliamEnrollDF.loc[(DirectComCliamEnrollDF['year'] == i),
                     :]  # it filters out data of a single year. It constructs a dataframe called SingleHFDF for each year

        ###########hospitalization cost
        uniqueConf_nums = singleHFDF.loc[singleHFDF['rectype'] == 'F', :]['conf_num'].unique()
        HospitalizationDF = singleHFDF.loc[singleHFDF['conf_num'].isin(uniqueConf_nums), :]
        # baseline information
        num_patients = len(singleHFDF['pat_id_x'].unique())  # 82771 total patients
        PatientInfoDF = singleHFDF.drop_duplicates('pat_id_x', keep='first')
        male_count = sum(PatientInfoDF['der_sex'] == 'M')
        ##num_agegreater85 = (PatientInfoDF['der_yob'] == 0).sum()
        ##PatientInfoLess85DF = PatientInfoDF.loc[(PatientInfoDF['der_yob'] != 0), :]
        ##PatientInfoLess85DF['age'] = i - PatientInfoLess85DF['der_yob']
        ##age_mean = PatientInfoLess85DF['age'].mean()
        ##age_std = PatientInfoLess85DF['age'].std()

        # How many patients has hospitalization
        num_patTohosptial = len(HospitalizationDF['pat_id_x'].unique())  # 22313

        # print(i, "total patients:", num_patients)
        # print(i, "total patients got into hosptialization:", num_patTohosptial) # the number of patient who at least got into hosptialization onece
        # print(i, "average cost per patient:", singleHFDF['allowed'].sum() / num_patients)

        avecostperPatient = HospitalizationDF['allowed'].sum() / num_patTohosptial
        num_totalHos = \
        HospitalizationDF.groupby(['pat_id_x', 'conf_num']).size().reset_index().rename(columns={0: 'count'})[
            'pat_id_x'].value_counts().sum()

        # print(i, "total hosptializations:", num_totalHos)
        # HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()

        readmissionrate = sum(
            HospitalizationDF.groupby(['pat_id_x', 'conf_num']).size().reset_index().rename(columns={0: 'count'})[
                'pat_id_x'].value_counts() > 1) / num_patTohosptial

        # print(i, "mean of hospitalization total spending:", (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()).mean())
        avecost_hospital = (HospitalizationDF.groupby(['conf_num'])['allowed'].sum()).replace(0, np.NaN).mean()
        mediancost_hospital = (HospitalizationDF.groupby(['conf_num'])['allowed'].sum()).replace(0, np.NaN).median()

        # print(i, "std of hospitalization total spending:",
        #      (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()).std())
        std_hospital = (HospitalizationDF.groupby(['conf_num'])['allowed'].sum()).replace(0, np.NaN).std()
        # (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()).median()
        # print(i, "mean of patients spending:", (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() - HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()).mean())

        avecost_hospitaltopatients = (HospitalizationDF.groupby(['pat_id_x', 'conf_num'])['allowed'].sum() -
                                      HospitalizationDF.groupby(['pat_id_x', 'conf_num'])['paid'].sum()) \
            .replace(0, np.NaN).mean()

        mediancost_hospitaltopatients = (HospitalizationDF.groupby(['pat_id_x', 'conf_num'])['allowed'].sum() -
                                         HospitalizationDF.groupby(['pat_id_x', 'conf_num'])['paid'].sum()) \
            .replace(0, np.NaN).median()

        # print(i, "std of patients spending:", (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() -
        #                                        HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()).std())
        std_hospitaltopatients = (HospitalizationDF.groupby(['pat_id_x', 'conf_num'])['allowed'].sum() -
                                  HospitalizationDF.groupby(['pat_id_x', 'conf_num'])['paid'].sum()) \
            .replace(0, np.NaN).std()

        # hospital stay

        HospitalizationStayDF = HospitalizationDF.loc[HospitalizationDF['rectype'] == 'F',]
        meanhostay = (HospitalizationStayDF.groupby(['pat_id_x', 'conf_num'])['to_dt'].max() - \
                      HospitalizationStayDF.groupby(['pat_id_x', 'conf_num'])['from_dt'].min()).mean().days
        medianhostay = (HospitalizationStayDF.groupby(['pat_id_x', 'conf_num'])['to_dt'].max() - \
                        HospitalizationStayDF.groupby(['pat_id_x', 'conf_num'])['from_dt'].min()).median().days
        stdhostay = (HospitalizationStayDF.groupby(['pat_id_x', 'conf_num'])['to_dt'].max() - \
                     HospitalizationStayDF.groupby(['pat_id_x', 'conf_num'])['from_dt'].min()).std().days

        # subpheno type heart failure
        '''
    
        #left heart failure
        HospitalizationDF['IsI501'] = (HospitalizationDF.iloc[:, 22:34].apply(lambda x: (x.loc[x.isin(DirectCodeList)]).str[:4])).apply(
            lambda x: ','.join(x.drop_duplicates().sort_values().dropna().astype(str)),
            axis=1
        ).str.contains('I501')
        HospitalizationDF.groupby(['I501','conf_num'])['allowed'].sum()
        HospitalizationDF.groupby(['I501'])['pat_id_x'].nunique()
        HospitalizationDF.groupby(['I502G'])['pat_id_x'].nunique()
        HospitalizationDF.groupby(['I503G'])['pat_id_x'].nunique()
        HospitalizationDF.groupby(['I504G'])['pat_id_x'].nunique()
        g = HospitalizationDF.groupby('conf_num')['I501','allowed'].sum()
    
        g.loc[g['I501'] >= 1, ['allowed']] /HospitalizationDF.groupby(['I501'])['pat_id_x'].nunique()
    
        g.loc[g['I501'] >= 1, ['allowed']].median()
        g.loc[g['I501'] >= 1, ['allowed']].mean()
        g.loc[g['I501'] >= 1, ['allowed']].std()
    
    
        #systolic
        HospitalizationDF['IsI502'] = (
            HospitalizationDF.iloc[:, 22:34].apply(lambda x: (x.loc[x.isin(DirectCodeList)]).str[:4])).apply(
            lambda x: ','.join(x.drop_duplicates().sort_values().dropna().astype(str)),
            axis=1
        ).str.contains('I502')
    
        g = HospitalizationDF.groupby('conf_num')['IsI502', 'allowed'].sum()
        g.loc[g['IsI502'] >= 1, ['allowed']].median()
        g.loc[g['IsI502'] >= 1, ['allowed']].mean()
        g.loc[g['IsI502'] >= 1, ['allowed']].std()
    
    
        #diastolic
        HospitalizationDF['IsI503'] = (
            HospitalizationDF.iloc[:, 22:34].apply(lambda x: (x.loc[x.isin(DirectCodeList)]).str[:4])).apply(
            lambda x: ','.join(x.drop_duplicates().sort_values().dropna().astype(str)),
            axis=1
        ).str.contains('I503')
    
        g = HospitalizationDF.groupby('conf_num')['IsI503', 'allowed'].sum()
        g.loc[g['IsI503'] >= 1, ['allowed']].median()
        g.loc[g['IsI503'] >= 1, ['allowed']].mean()
        g.loc[g['IsI503'] >= 1, ['allowed']].std()
    
        #combine systlic and diastolic
        HospitalizationDF['IsI504'] = (
            HospitalizationDF.iloc[:, 22:34].apply(lambda x: (x.loc[x.isin(DirectCodeList)]).str[:4])).apply(
            lambda x: ','.join(x.drop_duplicates().sort_values().dropna().astype(str)),
            axis=1
        ).str.contains('I504')
    
        g = HospitalizationDF.groupby('conf_num')['IsI504', 'allowed'].sum()
        g.loc[g['IsI504'] >= 1, ['allowed']].median()
        g.loc[g['IsI504'] >= 1, ['allowed']].mean()
        g.loc[g['IsI504'] >= 1, ['allowed']].std()
        '''

        # 'num_agegreater85': num_agegreater85,'age_mean': age_mean, 'age_std': age_std,
        DicHosptialInfo = {'year': i, 'num_patients': num_patients, 'avecost_patient': avecostperPatient,
                           'num_patientstohos': num_patTohosptial,
                           'num_totalhos': num_totalHos, 'hosptial_rate': num_patTohosptial / num_patients,
                           'male_count': male_count,
                           'avecost_hospital': avecost_hospital, 'std_hospital': std_hospital,
                           'mediancost_hospital': mediancost_hospital, 'readminssion_rate': readmissionrate,
                           'meanhostay': meanhostay,
                           'stdhostay': stdhostay, 'medianhostay': medianhostay,
                           'avecost_hospitaltopatients': avecost_hospitaltopatients,
                           'std_hospitaltopatients': std_hospitaltopatients,
                           'mediancost_hospitaltopatients': mediancost_hospitaltopatients}

        ########in hosptial procedure cost

        #########Management cost (non-hosptial)
        singleHFDF.loc[singleHFDF['rectype'] == 'M', :].shape
        MangementDF = singleHFDF.loc[(singleHFDF['rectype'] == 'M') & (singleHFDF['conf_num'].isna()), :]
        MangementDF = MangementDF.loc[~MangementDF['proc_cde'].isna(), :]
        MangementDF['proc_cde'].value_counts(dropna=False)

        # 1
        num_patients_management = len(MangementDF['pat_id_x'].unique())

        # 2
        average_costmanagement = MangementDF['allowed'].sum() / num_patients_management

        top10procode = (MangementDF.groupby(['proc_cde'])['allowed'].sum()).sort_values(ascending=False).iloc[:10].index
        print(i, top10procode)
        Top10ProManagementDF = MangementDF.loc[MangementDF['proc_cde'].isin(top10procode),]

        # 3
        Top10MangecostPercent = Top10ProManagementDF['allowed'].sum() / MangementDF['allowed'].sum()

        DicManagement = {'num_patients_management': num_patients_management,
                         'perecent_patienthasmanage': num_patients_management / num_patients,
                         'average_costmanagement': average_costmanagement, 'Top10MangecostPercent': Top10MangecostPercent, }

        # 4

        for idx, iprocde in enumerate(top10procode):
            # print(iprocde)
            itopaveragecost = Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, ['allowed']].sum()[0] / len(Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, :]['pat_id_x'].unique())
            itopaveragepatientcost = (Top10ProManagementDF.loc[
                                          Top10ProManagementDF['proc_cde'] == iprocde, ['allowed']].sum()[0] -
                                      Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, ['paid']].sum()[0]) / len(Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, :]['pat_id_x'].unique())
            DicManagement['top' + str(idx + 1) + 'code_management'] = iprocde
            DicManagement['top' + str(idx + 1) + 'code_avecost_management'] = itopaveragecost
            DicManagement['top' + str(idx + 1) + 'code_avepatientcost_management'] = itopaveragepatientcost

        ######Surgery cost

        singleHFDF.loc[singleHFDF['rectype'] == 'S', :].shape
        if singleHFDF.loc[singleHFDF['rectype'] == 'S', :].shape[0] > 0:
            singleHFDF.loc[(singleHFDF['rectype'] == 'S') & (singleHFDF['conf_num'].isna()), :].shape
            SurgeryDF = singleHFDF.loc[(singleHFDF['rectype'] == 'S'), :]
            singleHFDF.loc[singleHFDF['ndc'].isna(), ['ndc']]
            # 1
            num_patients_havesurgery = len(SurgeryDF['pat_id_x'].unique())
            rate_patients_havetwosugery = sum(
                SurgeryDF.groupby(['pat_id_x']).size().reset_index().rename(columns={0: 'count'})[
                    'count'] > 1) / num_patients_havesurgery

            # 2
            average_costsugery = SurgeryDF['allowed'].sum() / num_patients_havesurgery

            top10surgerycode = (SurgeryDF.groupby(['proc_cde'])['allowed'].sum()).sort_values(ascending=False).iloc[
                               :10].index
            print(i, top10surgerycode)
            Top10SurgeryDF = SurgeryDF.loc[SurgeryDF['proc_cde'].isin(top10surgerycode),]

            # 3
            Top10SurgerycostPercent = Top10SurgeryDF['allowed'].sum() / SurgeryDF['allowed'].sum()
            #
            DicSurgery = {'num_patients_havesurgery': num_patients_havesurgery,
                          'rate_patients_havetwosugery': rate_patients_havetwosugery,
                          'average_costsugery': average_costsugery, 'Top10SurgerycostPercent': Top10SurgerycostPercent}

            # 4

            for idx, iprocde in enumerate(top10surgerycode):
                # print(iprocde)
                itopaveragecost = Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, ['allowed']].sum()[
                                      0] / len(
                    Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, :]['pat_id_x'].unique())
                itopaveragepatientcost = (Top10SurgeryDF.loc[
                                              Top10SurgeryDF['proc_cde'] == iprocde, ['allowed']].sum()[0] -
                                          Top10SurgeryDF.loc[
                                              Top10SurgeryDF['proc_cde'] == iprocde, ['paid']].sum()[0]) / len(
                    Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, :]['pat_id_x'].unique())
                DicSurgery['top' + str(idx + 1) + 'code_surgery'] = iprocde
                DicSurgery['top' + str(idx + 1) + 'code_avecost_surgery'] = itopaveragecost
                DicSurgery['top' + str(idx + 1) + 'code_avepatientcost_surgery'] = itopaveragepatientcost

            ######Drug cost

            if singleHFDF.loc[singleHFDF['rectype'] == 'P', :].shape[0] > 0:
                singleHFDF.loc[(singleHFDF['rectype'] == 'P') & (singleHFDF['conf_num'].isna()), :].shape
                DrugDF = singleHFDF.loc[(singleHFDF['rectype'] == 'P'), :]

                # 1
                num_patients_havedrug = len(DrugDF['pat_id_x'].unique())
                rate_patients_havetwodrug = sum(
                    DrugDF.groupby(['pat_id_x']).size().reset_index().rename(columns={0: 'count'})[
                        'count'] > 1) / num_patients_havedrug

                # 2
                average_costdrug = DrugDF['allowed'].sum() / num_patients_havedrug
                DicDrug = {'num_patients_havedrug': num_patients_havedrug,
                           'rate_patients_havetwodrug': rate_patients_havetwodrug,
                           'average_costdrug': average_costdrug}

                if i > 2010:
                    top10drugcode = (DrugDF.groupby(['ndc'])['allowed'].sum()).sort_values(ascending=False).iloc[
                                    :10].index
                    print(i, top10drugcode)
                    Top10DrugDF = DrugDF.loc[DrugDF['ndc'].isin(top10drugcode),]
                    # 3
                    Top10DrugcostPercent = Top10DrugDF['allowed'].sum() / DrugDF['allowed'].sum()
                else:
                    Top10DrugcostPercent = 0

                DicDrug['Top10DrugcostPercent'] = Top10DrugcostPercent

                # 4

                if i > 2010:
                    for idx, iprocde in enumerate(top10drugcode):
                        # print(iprocde)
                        itopaveragecost = Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, ['allowed']].sum()[
                                              0] / len(
                            Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, :]['pat_id_x'].unique())
                        itopaveragepatientcost = (Top10DrugDF.loc[
                                                      Top10DrugDF['ndc'] == iprocde, ['allowed']].sum()[0] -
                                                  Top10DrugDF.loc[
                                                      Top10DrugDF['ndc'] == iprocde, ['paid']].sum()[0]) / len(
                            Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, :]['pat_id_x'].unique())
                        DicDrug['top' + str(idx + 1) + 'code_drug'] = iprocde
                        DicDrug['top' + str(idx + 1) + 'code_avecost_drug'] = itopaveragecost
                        DicDrug['top' + str(idx + 1) + 'code_avepatientcost_drug'] = itopaveragepatientcost
                else:

                    for idx in range(10):
                        DicDrug['top' + str(idx + 1) + 'code_drug'] = 0
                        DicDrug['top' + str(idx + 1) + 'code_avecost_drug'] = 0
                        DicDrug['top' + str(idx + 1) + 'code_avepatientcost_drug'] = 0

                ###HCPCS information

                top10HCPCScode = (DrugDF.groupby(['proc_cde'])['allowed'].sum()).sort_values(ascending=False).iloc[
                                 :10].index
                print(i, 'top10HCPCScode:', top10HCPCScode)
                Top10HCPCSDF = DrugDF.loc[DrugDF['proc_cde'].isin(top10HCPCScode),]

                # 3

                Top10HCPCScostPercent = Top10HCPCSDF['allowed'].sum() / DrugDF['allowed'].sum()
                DicDrug['Top10HCPCScostPercent'] = Top10HCPCScostPercent
                for idx, iprocde in enumerate(top10HCPCScode):
                    # print(iprocde)
                    itopaveragecost = Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, ['allowed']].sum()[
                                          0] / len(
                        Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, :]['pat_id_x'].unique())
                    itopaveragepatientcost = (Top10HCPCSDF.loc[
                                                  Top10HCPCSDF['proc_cde'] == iprocde, ['allowed']].sum()[0] -
                                              Top10HCPCSDF.loc[
                                                  Top10HCPCSDF['proc_cde'] == iprocde, ['paid']].sum()[0]) / len(
                        Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, :]['pat_id_x'].unique())
                    DicDrug['top' + str(idx + 1) + 'code_HCPCS'] = iprocde
                    DicDrug['top' + str(idx + 1) + 'code_avecost_HCPCS'] = itopaveragecost
                    DicDrug['top' + str(idx + 1) + 'code_avepatientcost_HCPCS'] = itopaveragepatientcost

        newDic = {**DicHosptialInfo, **DicManagement, **DicSurgery, **DicDrug}
        singleYearDF = pd.DataFrame.from_dict(newDic, orient='index')
        singleYearDF = singleYearDF.transpose()
        TotalInfoDF = pd.concat([TotalInfoDF, singleYearDF], ignore_index=True)

    TotalInfoDF.to_csv(f"C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/{code}_CostBreakdown_inclusie_Summary.csv",
                       index=None)