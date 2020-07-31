import numpy as np
import pandas as pd

def installments_payments_process(installments_payments):
    
    installments_payments['days_diff'] = installments_payments['DAYS_INSTALMENT'] - installments_payments['DAYS_ENTRY_PAYMENT']
    installments_payments['AMT_diff'] = installments_payments['AMT_INSTALMENT'] - installments_payments['AMT_PAYMENT']
    installments_payments['AMT_ratio'] = installments_payments['AMT_PAYMENT'] / installments_payments['AMT_INSTALMENT']
    
    installments_payments = installments_payments.sort_values(['SK_ID_CURR','SK_ID_PREV','NUM_INSTALMENT_NUMBER'])
    
    installments_group = installments_payments.groupby('SK_ID_CURR').agg({
        'SK_ID_PREV': ['nunique', 'count'],
        'NUM_INSTALMENT_NUMBER': ['max', 'mean', 'sum', 'std'],
        'days_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_PAYMENT': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    installments_group.columns = ['_'.join(column)+'_installments' for column in installments_group.columns]
    installments_group.reset_index(inplace=True)
    
    installments_on_time = installments_payments.loc[installments_payments['days_diff']>0]

    installments_on_time_group = installments_on_time.groupby('SK_ID_CURR').agg({
        'SK_ID_PREV': 'count',
        'days_diff': ['max', 'min', 'sum'],
        'AMT_PAYMENT': ['max', 'min', 'sum'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    installments_on_time_group.columns = ['_'.join(column)+'_good_installments' for column in installments_on_time_group.columns]
    installments_on_time_group.reset_index(inplace=True)

    installments_group = pd.merge(installments_group, installments_on_time_group, how = 'left', on = 'SK_ID_CURR')
    
    installments_overdue = installments_payments.loc[installments_payments['days_diff']<0]
    installments_overdue_group = installments_overdue.groupby('SK_ID_CURR').agg({
        'SK_ID_PREV': 'count',
        'days_diff': ['max', 'min', 'sum'],
        'AMT_PAYMENT': ['max', 'min', 'sum'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    installments_overdue_group.columns = ['_'.join(column)+'_bad_installments' for column in installments_overdue_group.columns]
    installments_overdue_group.reset_index(inplace=True)

    installments_group = pd.merge(installments_group, installments_overdue_group, how = 'left', on = 'SK_ID_CURR')
    
    tlc = 250
    instal_tlc_250 = installments_payments.loc[installments_payments['AMT_diff']< tlc]
    instal_tlc_250_group = instal_tlc_250.groupby('SK_ID_CURR').agg({
        'days_diff': ['max', 'mean', 'min', 'sum'],
        'AMT_PAYMENT': ['max', 'min'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_tlc_250_group.columns = ['_'.join(column)+'_instal_tlc_250' for column in instal_tlc_250_group.columns]
    instal_tlc_250_group.reset_index(inplace=True)

    installments_group = pd.merge(installments_group, instal_tlc_250_group, how = 'left', on = 'SK_ID_CURR')
    
    days_diff_thre = 0
    instal_tlc_250_ot = instal_tlc_250.loc[instal_tlc_250['days_diff']<days_diff_thre]
    instal_tlc_250_ot_group_tmp = instal_tlc_250_ot.groupby(['SK_ID_CURR','SK_ID_PREV']).agg({
        'DAYS_INSTALMENT': ['nunique',],
        'days_diff': ['max', 'mean', 'min'],
        'NUM_INSTALMENT_NUMBER': ['max', 'min', 'mean'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_tlc_250_ot_group_tmp.columns = ['_'.join(column)+'_instal_tlc_250_ot_tmp' for column in instal_tlc_250_ot_group_tmp.columns]
    instal_tlc_250_ot_group_tmp.reset_index(inplace=True)
    
    instal_tlc_250_ot_group = instal_tlc_250_ot_group_tmp.groupby('SK_ID_CURR').agg({
        'DAYS_INSTALMENT_nunique_instal_tlc_250_ot_tmp': ['max', 'min', 'sum', 'mean'],
        'days_diff_max_instal_tlc_250_ot_tmp': ['max', 'min', 'mean'],
        'days_diff_min_instal_tlc_250_ot_tmp': ['max', 'min', 'mean'],
        'days_diff_mean_instal_tlc_250_ot_tmp': ['max', 'min', 'mean'],
        'NUM_INSTALMENT_NUMBER_max_instal_tlc_250_ot_tmp': ['max', 'min', 'mean'],
        'NUM_INSTALMENT_NUMBER_min_instal_tlc_250_ot_tmp': ['max', 'min', 'mean'],
        'NUM_INSTALMENT_NUMBER_mean_instal_tlc_250_ot_tmp': ['max', 'min', 'mean'],
        'AMT_diff_max_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_diff_mean_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_diff_sum_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_diff_std_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_ratio_max_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_ratio_mean_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_ratio_sum_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
        'AMT_ratio_std_instal_tlc_250_ot_tmp': ['max', 'min', 'sum'],
    })
    instal_tlc_250_ot_group.columns = ['_'.join(column) for column in instal_tlc_250_ot_group.columns]
    instal_tlc_250_ot_group.reset_index(inplace=True)

    installments_group = pd.merge(installments_group, instal_tlc_250_ot_group, how = 'left', on = 'SK_ID_CURR')
    
    X = 5
    Y = 10
    tlc = 250
    instal_tlc_250 = installments_payments.loc[(installments_payments['AMT_diff']<tlc)&(installments_payments['days_diff']<-Y)]
    instal_tlc_250_XY =instal_tlc_250.groupby(['SK_ID_CURR','SK_ID_PREV']).head(X)
    instal_tlc_250_XY.reset_index(inplace=True)

    instal_tlc_250_XY_group = instal_tlc_250_XY.groupby(['SK_ID_CURR']).agg({
        'SK_ID_PREV':['nunique','count'],
        'days_diff': ['max', 'min', 'mean'],
        'AMT_diff': ['max', 'min', 'mean'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std'],
    })
    instal_tlc_250_XY_group.columns = ['_'.join(column)+f'_{X}{Y}_{tlc}' for column in instal_tlc_250_XY_group.columns]
    instal_tlc_250_XY_group.reset_index(inplace = True)

    installments_group = pd.merge(installments_group, instal_tlc_250_XY_group, how = 'left', on = 'SK_ID_CURR')
    
    X = 10
    Y = 20
    tlc = 250
    instal_tlc_250 = installments_payments.loc[(installments_payments['AMT_diff']<tlc)&(installments_payments['days_diff']<-Y)]
    instal_tlc_250_XY =instal_tlc_250.groupby(['SK_ID_CURR','SK_ID_PREV']).head(X)
    instal_tlc_250_XY.reset_index(inplace=True)

    instal_tlc_250_XY_group = instal_tlc_250_XY.groupby(['SK_ID_CURR']).agg({
        'SK_ID_PREV':['nunique','count'],
        'days_diff': ['max', 'min', 'mean'],
        'AMT_diff': ['max', 'min', 'mean'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_tlc_250_XY_group.columns = ['_'.join(column)+f'_{X}{Y}_{tlc}' for column in instal_tlc_250_XY_group.columns]
    instal_tlc_250_XY_group.reset_index(inplace = True)

    installments_group = pd.merge(installments_group, instal_tlc_250_XY_group, how = 'left', on = 'SK_ID_CURR')
    
    X = 20
    Y = 30
    tlc = 250
    instal_tlc_250 = installments_payments.loc[(installments_payments['AMT_diff']<tlc)&(installments_payments['days_diff']<-Y)]
    instal_tlc_250_XY =instal_tlc_250.groupby(['SK_ID_CURR','SK_ID_PREV']).head(X)
    instal_tlc_250_XY.reset_index(inplace=True)

    instal_tlc_250_XY_group = instal_tlc_250_XY.groupby(['SK_ID_CURR']).agg({
        'SK_ID_PREV':['nunique','count'],
        'days_diff': ['max', 'min', 'mean'],
        'AMT_diff': ['max', 'min', 'mean'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_tlc_250_XY_group.columns = ['_'.join(column)+f'_{X}{Y}_{tlc}' for column in instal_tlc_250_XY_group.columns]
    instal_tlc_250_XY_group.reset_index(inplace = True)

    installments_group = pd.merge(installments_group, instal_tlc_250_XY_group, how = 'left', on = 'SK_ID_CURR')
    
    X = 30
    Y = 40
    tlc = 250
    instal_tlc_250 = installments_payments.loc[(installments_payments['AMT_diff']<tlc)&(installments_payments['days_diff']<-Y)]
    instal_tlc_250_XY =instal_tlc_250.groupby(['SK_ID_CURR','SK_ID_PREV']).head(X)
    instal_tlc_250_XY.reset_index(inplace=True)

    instal_tlc_250_XY_group = instal_tlc_250_XY.groupby(['SK_ID_CURR']).agg({
        'SK_ID_PREV':['nunique','count'],
        'days_diff': ['max', 'min', 'mean'],
        'AMT_diff': ['max', 'min', 'mean'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_tlc_250_XY_group.columns = ['_'.join(column)+f'_{X}{Y}_{tlc}' for column in instal_tlc_250_XY_group.columns]
    instal_tlc_250_XY_group.reset_index(inplace = True)

    installments_group = pd.merge(installments_group, instal_tlc_250_XY_group, how = 'left', on = 'SK_ID_CURR')
    
    return installments_group


if __name__ == "__main__":
    
    installments_payments = pd.read_csv('../input/installments_payments.csv')
    
    installments_group = installments_payments_process(installments_payments)
    
    with open('../processed/installments_payments_processed.pickle', 'wb') as handle:
        pickle.dump(installments_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

