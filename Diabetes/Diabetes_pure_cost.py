import os, zipfile
import gzip
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np


######### type 1

#HFDxCodeDF = pd.read_csv("C:/Users/rakovski/Dropbox (Chapman)/HeartFailure/Data/HeartFailure/HeartFailureDXcode.csv")
#####ComCliamEnrollDF = pd.read_csv("E:/Cancer Project/Data/anti_inflammatory_data/Aspirin/Aspirin_takers_all_data/with_headers/2007_aspirin_df.csv", dtype=str)

ComCliamEnrollDF = pd.read_csv("E:/Diabetes Project/Data/diabetes_type1_with_headers.csv", dtype=str)
###print(ComCliamEnrollDF['diag1'].value_counts())


print(ComCliamEnrollDF.columns)
type_1_df = pd.read_csv("E:/Diabetes Project/Type_1.csv")
type_2_df = pd.read_csv("E:/Diabetes Project/Type_2.csv")

type_1_df['type_1'] = type_1_df['type_1'].str.replace('.', '').astype(str)
type_1_df = pd.DataFrame(type_1_df, columns =['type_1'])
type_1_list = type_1_df['type_1'].tolist()        #[['type_1']] .to_list()
print(type_1_list)


type_2_df['type_2'] = type_2_df['type_2'].str.replace('.', '').astype(str)
type_2_df = pd.DataFrame(type_2_df, columns =['type_2'])
type_2_list = type_2_df['type_2'].tolist()        #[type_2]
print(type_2_list)


# convert the date column to datetime format
ComCliamEnrollDF['paid_dt'] = pd.to_datetime(ComCliamEnrollDF['paid_dt'])
# extract the month and year into separate columns
ComCliamEnrollDF['month'] = ComCliamEnrollDF['paid_dt'].dt.month
print(ComCliamEnrollDF['month'].min())
print(ComCliamEnrollDF['month'].max())

ComCliamEnrollDF['year'] = ComCliamEnrollDF['paid_dt'].dt.year
print(ComCliamEnrollDF['year'].min())
print(ComCliamEnrollDF['year'].max())

######ComCliamEnrollDF.loc[~ComCliamEnrollDF['ndc'].isna(),['ndc']]
####ComCliamEnrollDF['dayssup'] = ComCliamEnrollDF['dayssup'].astype('float').astype('Int64')
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
####ComCliamEnrollDF['der_yob'] = ComCliamEnrollDF['der_yob'].astype('float').astype('Int64')
ComCliamEnrollDF['year'] = ComCliamEnrollDF['year'].astype('float').astype('Int64')
ComCliamEnrollDF['month'] = ComCliamEnrollDF['month'].astype('float').astype('Int64')

print('I am here')



for code in type_1_list:
    ComCliamEnrollDF[code] = 0
###ComCliamEnrollDF[type_2_list] = 0


for code in type_1_list:
    ComCliamEnrollDF[code] = np.where(np.any(ComCliamEnrollDF.iloc[:, 22:35].isin([code]), axis=1), 1, 0)


for code in type_2_list:
    ComCliamEnrollDF[code] = 0
###ComCliamEnrollDF[type_2_list] = 0


for code in type_2_list:
    ComCliamEnrollDF[code] = np.where(np.any(ComCliamEnrollDF.iloc[:, 22:35].isin([code]), axis=1), 1, 0)

####ComCliamEnrollDF[type_2_list] = ComCliamEnrollDF.iloc[:, 22:35].apply(lambda x: pd.Series(np.isin(type_2_list, x).astype(int)), axis=1)


print('got here')


ComCliamEnrollDF['type_2']= 0
ComCliamEnrollDF['type_2'] = (ComCliamEnrollDF[type_2_list].sum(axis=1) > 0).astype(int)
print(ComCliamEnrollDF['type_2'].value_counts())

# the target:
ComCliamEnrollDF['type_1'] = 0
ComCliamEnrollDF['type_1'] = ((ComCliamEnrollDF[type_1_list].sum(axis=1) > 0) & (ComCliamEnrollDF['type_2'] ==0)).astype(int)         #(ComCliamEnrollDF[type_1_list].sum(axis=1) > 0) &
print(ComCliamEnrollDF['type_1'].value_counts())
print('Here here')

#####(ComCliamEnrollDF.groupby(['type_1', 'year'])['allowed'].sum() / ComCliamEnrollDF.groupby(['type_1', 'year'])['pat_id'].nunique()).to_csv('E:/Diabetes Project/Analysis results/type_1_pure_total_cost.csv')


print('I got the average cost!')





############# breakdown


diabetes_type_1_DF = ComCliamEnrollDF.loc[(ComCliamEnrollDF['type_1'] == 1), :]

DirectComCliamEnrollDF = diabetes_type_1_DF

TotalInfoDF = pd.DataFrame()
for i in range(2006, 2022):

        num_patients = (DirectComCliamEnrollDF.loc[(DirectComCliamEnrollDF['year'] == i), :]['pat_id']).unique().shape[0]
        print(i, 'year', num_patients)
        # print(i, (ComCliamEnrollDF.loc[(ComCliamEnrollDF['year'] == i), ['paid']]).sum()/num_patients)
        # HFClaimDF2021 = ComCliamEnrollDF.loc[(ComCliamEnrollDF['year'] == i), :]

        singleHFDF = DirectComCliamEnrollDF.loc[(DirectComCliamEnrollDF['year'] == i),:]  # it filters out data of a single year. It constructs a dataframe called SingleHFDF for each year

        ###########hospitalization cost
        uniqueConf_nums = singleHFDF.loc[singleHFDF['rectype'] == 'F', :]['conf_num'].unique()
        HospitalizationDF = singleHFDF.loc[singleHFDF['conf_num'].isin(uniqueConf_nums), :]
        # baseline information
        num_patients = len(singleHFDF['pat_id'].unique())  # 82771 total patients
        if num_patients == 0:
            num_patients = 0.000000001
        PatientInfoDF = singleHFDF.drop_duplicates('pat_id', keep='first')
        #male_count = sum(PatientInfoDF['der_sex'] == 'M')
        ##num_agegreater85 = (PatientInfoDF['der_yob'] == 0).sum()
        ##PatientInfoLess85DF = PatientInfoDF.loc[(PatientInfoDF['der_yob'] != 0), :]
        ##PatientInfoLess85DF['age'] = i - PatientInfoLess85DF['der_yob']
        ##age_mean = PatientInfoLess85DF['age'].mean()
        ##age_std = PatientInfoLess85DF['age'].std()

        # How many patients has hospitalization
        num_totalHos = HospitalizationDF.groupby(['pat_id', 'conf_num']).size().reset_index().rename(columns={0: 'count'})['pat_id'].value_counts().sum()

        num_patTohosptial = len(HospitalizationDF['pat_id'].unique())  # 22313
        if num_patTohosptial == 0:
            num_patTohosptial = 0.000000001

        avecostperPatient = HospitalizationDF['allowed'].sum() / num_patTohosptial
        readmissionrate = sum(HospitalizationDF.groupby(['pat_id', 'conf_num']).size().reset_index().rename(columns={0: 'count'})['pat_id'].value_counts() > 1) / num_patTohosptial

        # print(i, "mean of hospitalization total spending:", (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()).mean())
        avecost_hospital = (HospitalizationDF.groupby(['conf_num'])['allowed'].sum()).replace(0, np.NaN).mean()
        mediancost_hospital = (HospitalizationDF.groupby(['conf_num'])['allowed'].sum()).replace(0, np.NaN).median()

        # print(i, "std of hospitalization total spending:",
        #      (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()).std())
        std_hospital = (HospitalizationDF.groupby(['conf_num'])['allowed'].sum()).replace(0, np.NaN).std()
        # (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum()).median()
        # print(i, "mean of patients spending:", (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() - HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()).mean())

        avecost_hospitaltopatients = (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() -
                                      HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()) \
            .replace(0, np.NaN).mean()

        mediancost_hospitaltopatients = (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() -
                                         HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()) \
            .replace(0, np.NaN).median()

        # print(i, "std of patients spending:", (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() -
        #                                        HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()).std())
        std_hospitaltopatients = (HospitalizationDF.groupby(['pat_id', 'conf_num'])['allowed'].sum() -
                                  HospitalizationDF.groupby(['pat_id', 'conf_num'])['paid'].sum()) \
            .replace(0, np.NaN).std()

        # hospital stay

        HospitalizationStayDF = HospitalizationDF.loc[HospitalizationDF['rectype'] == 'F',]
        meanhostay = (HospitalizationStayDF.groupby(['pat_id', 'conf_num'])['to_dt'].max() - \
                      HospitalizationStayDF.groupby(['pat_id', 'conf_num'])['from_dt'].min()).mean().days
        medianhostay = (HospitalizationStayDF.groupby(['pat_id', 'conf_num'])['to_dt'].max() - \
                        HospitalizationStayDF.groupby(['pat_id', 'conf_num'])['from_dt'].min()).median().days
        stdhostay = (HospitalizationStayDF.groupby(['pat_id', 'conf_num'])['to_dt'].max() - \
                     HospitalizationStayDF.groupby(['pat_id', 'conf_num'])['from_dt'].min()).std().days

        # subpheno type heart failure

        # 'num_agegreater85': num_agegreater85,'age_mean': age_mean, 'age_std': age_std,
        DicHosptialInfo = {'year': i, 'num_patients': num_patients, 'avecost_patient': avecostperPatient,
                           'num_patientstohos': num_patTohosptial,
                           'num_totalhos': num_totalHos, 'hosptial_rate': num_patTohosptial / num_patients,
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
        num_patients_management = len(MangementDF['pat_id'].unique())
        # 2
        if num_patients_management == 0:
            num_patients_management = 0.000000001


        average_costmanagement = MangementDF['allowed'].sum() / num_patients_management

        top10procode = (MangementDF.groupby(['proc_cde'])['allowed'].sum()).sort_values(ascending=False).iloc[:10].index
        print(i, top10procode)
        Top10ProManagementDF = MangementDF.loc[MangementDF['proc_cde'].isin(top10procode),]

            # 3
        Management_cost_sum = MangementDF['allowed'].sum()
        if Management_cost_sum == 0:
            Management_cost_sum = 0.000000001

        Top10MangecostPercent = Top10ProManagementDF['allowed'].sum() / Management_cost_sum

        DicManagement = {'num_patients_management': num_patients_management,
                         'perecent_patienthasmanage': num_patients_management / num_patients,
                         'average_costmanagement': average_costmanagement, 'Top10MangecostPercent': Top10MangecostPercent }
#############################################################
        # 4
        if num_patients_management != 0.000000001:
            for idx, iprocde in enumerate(top10procode):
                # print(iprocde)
                itopaveragecost = Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, ['allowed']].sum()[0] / len(Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, :]['pat_id'].unique())
                itopaveragepatientcost = (Top10ProManagementDF.loc[
                                              Top10ProManagementDF['proc_cde'] == iprocde, ['allowed']].sum()[0] -
                                          Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, ['paid']].sum()[0]) / len(Top10ProManagementDF.loc[Top10ProManagementDF['proc_cde'] == iprocde, :]['pat_id'].unique())
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
            num_patients_havesurgery = len(SurgeryDF['pat_id'].unique())

            if num_patients_havesurgery == 0:
                num_patients_havesurgery = 0.000000001

            rate_patients_havetwosugery = sum(SurgeryDF.groupby(['pat_id']).size().reset_index().rename(columns={0: 'count'})['count'] > 1) / num_patients_havesurgery


            # 2
            average_costsugery = SurgeryDF['allowed'].sum() / num_patients_havesurgery

            top10surgerycode = (SurgeryDF.groupby(['proc_cde'])['allowed'].sum()).sort_values(ascending=False).iloc[:10].index
            print(i, top10surgerycode)
            Top10SurgeryDF = SurgeryDF.loc[SurgeryDF['proc_cde'].isin(top10surgerycode),]

            # 3
            Surgery_sum = SurgeryDF['allowed'].sum()
            if Surgery_sum == 0:
                Surgery_sum = 0.000000001

            Top10SurgerycostPercent = Top10SurgeryDF['allowed'].sum() / Surgery_sum
            #
            DicSurgery = {'num_patients_havesurgery': num_patients_havesurgery,
                          'rate_patients_havetwosugery': rate_patients_havetwosugery,
                          'average_costsugery': average_costsugery, 'Top10SurgerycostPercent': Top10SurgerycostPercent}

            # 4

            for idx, iprocde in enumerate(top10surgerycode):
                # print(iprocde)
                len_top_surgery = len(Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, :]['pat_id'].unique())
                if len_top_surgery == 0:
                    len_top_surgery = 0.000000001

                itopaveragecost = Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, ['allowed']].sum()[0] / len_top_surgery

                itopaveragepatientcost = (Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, ['allowed']].sum()[0] -
                                          Top10SurgeryDF.loc[Top10SurgeryDF['proc_cde'] == iprocde, ['paid']].sum()[0]) / len_top_surgery

                DicSurgery['top' + str(idx + 1) + 'code_surgery'] = iprocde
                DicSurgery['top' + str(idx + 1) + 'code_avecost_surgery'] = itopaveragecost
                DicSurgery['top' + str(idx + 1) + 'code_avepatientcost_surgery'] = itopaveragepatientcost

        else:
            DicSurgery = {}

            ######Drug cost

        if singleHFDF.loc[singleHFDF['rectype'] == 'P', :].shape[0] > 0:
                singleHFDF.loc[(singleHFDF['rectype'] == 'P') & (singleHFDF['conf_num'].isna()), :].shape
                DrugDF = singleHFDF.loc[(singleHFDF['rectype'] == 'P'), :]

                # 1
                num_patients_havedrug = len(DrugDF['pat_id'].unique())

                if num_patients_havedrug == 0:
                    num_patients_havedrug = 0.000000001

                rate_patients_havetwodrug = sum(DrugDF.groupby(['pat_id']).size().reset_index().rename(columns={0: 'count'})['count'] > 1) / num_patients_havedrug

                # 2
                average_costdrug = DrugDF['allowed'].sum() / num_patients_havedrug
                DicDrug = {'num_patients_havedrug': num_patients_havedrug,
                           'rate_patients_havetwodrug': rate_patients_havetwodrug,
                           'average_costdrug': average_costdrug}

                if i > 2010:
                    top10drugcode = (DrugDF.groupby(['ndc'])['allowed'].sum()).sort_values(ascending=False).iloc[:10].index
                    print(i, top10drugcode)
                    Top10DrugDF = DrugDF.loc[DrugDF['ndc'].isin(top10drugcode),]
                    # 3
                    Drug_sum = DrugDF['allowed'].sum()

                    if Drug_sum == 0:
                        Drug_sum = 0.000000001

                    Top10DrugcostPercent = Top10DrugDF['allowed'].sum() / Drug_sum
                else:
                    Top10DrugcostPercent = 0

                DicDrug['Top10DrugcostPercent'] = Top10DrugcostPercent

                # 4

                if i > 2010:
                    for idx, iprocde in enumerate(top10drugcode):
                        # print(iprocde)
                        len_drug = len(Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, :]['pat_id'].unique())
                        if len_drug == 0:
                            len_drug = 0.000000001
                        itopaveragecost = Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, ['allowed']].sum()[0] / len_drug

                        itopaveragepatientcost = (Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, ['allowed']].sum()[0] -Top10DrugDF.loc[Top10DrugDF['ndc'] == iprocde, ['paid']].sum()[0]) / len_drug
                        DicDrug['top' + str(idx + 1) + 'code_drug'] = iprocde
                        DicDrug['top' + str(idx + 1) + 'code_avecost_drug'] = itopaveragecost
                        DicDrug['top' + str(idx + 1) + 'code_avepatientcost_drug'] = itopaveragepatientcost
                else:

                    for idx in range(10):
                        DicDrug['top' + str(idx + 1) + 'code_drug'] = 0
                        DicDrug['top' + str(idx + 1) + 'code_avecost_drug'] = 0
                        DicDrug['top' + str(idx + 1) + 'code_avepatientcost_drug'] = 0

                ###HCPCS information

                top10HCPCScode = (DrugDF.groupby(['proc_cde'])['allowed'].sum()).sort_values(ascending=False).iloc[:10].index
                print(i, 'top10HCPCScode:', top10HCPCScode)
                Top10HCPCSDF = DrugDF.loc[DrugDF['proc_cde'].isin(top10HCPCScode),]

                # 3
                Drug_sum = DrugDF['allowed'].sum()

                if Drug_sum == 0:
                    Drug_sum = 0.000000001

                Top10HCPCScostPercent = Top10HCPCSDF['allowed'].sum() / Drug_sum
                DicDrug['Top10HCPCScostPercent'] = Top10HCPCScostPercent
                for idx, iprocde in enumerate(top10HCPCScode):
                    # print(iprocde)
                    len_top_hspcs = len(Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, :]['pat_id'].unique())

                    if len_top_hspcs == 0:
                        len_top_hspcs = 0.000000001

                    itopaveragecost = Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, ['allowed']].sum()[0] / len_top_hspcs

                    itopaveragepatientcost = (Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, ['allowed']].sum()[0] -Top10HCPCSDF.loc[Top10HCPCSDF['proc_cde'] == iprocde, ['paid']].sum()[0]) / len_top_hspcs
                    DicDrug['top' + str(idx + 1) + 'code_HCPCS'] = iprocde
                    DicDrug['top' + str(idx + 1) + 'code_avecost_HCPCS'] = itopaveragecost
                    DicDrug['top' + str(idx + 1) + 'code_avepatientcost_HCPCS'] = itopaveragepatientcost
        else:
            DicDrug = {}
        newDic = {**DicHosptialInfo, **DicManagement, **DicSurgery, **DicDrug}
        singleYearDF = pd.DataFrame.from_dict(newDic, orient='index')
        singleYearDF = singleYearDF.transpose()
        TotalInfoDF = pd.concat([TotalInfoDF, singleYearDF], ignore_index=True)
        print(" I looped once!")
TotalInfoDF.to_csv(f"E:/Diabetes Project/Analysis results/type_1_cost_breakdown.csv",index=None)


