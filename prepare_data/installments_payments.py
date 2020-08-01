import numpy as np
import pandas as pd
import pickle

def installments_payments_days_process(installments_payments, days=None):
    
    if days == None:
        installments_payments = installments_payments
    else:
        installments_payments_last_days = installments_payments[installments_payments.DAYS_ENTRY_PAYMENT >= -days]    
    
    installments_last_days_group = installments_payments_last_days.groupby('SK_ID_CURR').agg({
        'SK_ID_PREV': ['nunique', 'count'],
        'NUM_INSTALMENT_NUMBER': ['max', 'mean', 'sum', 'std'],
        'days_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_PAYMENT': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    installments_last_days_group.columns = ['_'.join(column)+f'_installments_last_{days}' for column in installments_last_days_group.columns]
    installments_last_days_group.reset_index(inplace=True)
    
    installments_last_days_on_time = installments_payments_last_days.loc[installments_payments_last_days['days_diff']>0]
    installments_last_days_on_time_group = installments_last_days_on_time.groupby('SK_ID_CURR').agg({
        'SK_ID_PREV': 'count',
        'days_diff': ['max', 'min', 'sum'],
        'AMT_PAYMENT': ['max', 'min', 'sum'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    installments_last_days_on_time_group.columns = ['_'.join(column)+f'_good_installments_last_{days}' for column in installments_last_days_on_time_group.columns]
    installments_last_days_on_time_group.reset_index(inplace=True)

    installments_last_days_group = pd.merge(installments_last_days_group, installments_last_days_on_time_group, how = 'left', on = 'SK_ID_CURR')
    
    installments_last_days_overdue = installments_payments_last_days.loc[installments_payments_last_days['days_diff']<0]
    installments_last_days_overdue_group = installments_last_days_overdue.groupby('SK_ID_CURR').agg({
        'SK_ID_PREV': 'count',
        'days_diff': ['max', 'min', 'sum'],
        'AMT_PAYMENT': ['max', 'min', 'sum'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    installments_last_days_overdue_group.columns = ['_'.join(column)+f'_bad_installments_last_{days}' for column in installments_last_days_overdue_group.columns]
    installments_last_days_overdue_group.reset_index(inplace=True)

    installments_last_days_group = pd.merge(installments_last_days_group, installments_last_days_overdue_group, how = 'left', on = 'SK_ID_CURR')
    
    tlc = 250
    instal_last_days_tlc_250 = installments_payments_last_days.loc[installments_payments_last_days['AMT_diff']< tlc]
    instal_last_days_tlc_250_group = instal_last_days_tlc_250.groupby('SK_ID_CURR').agg({
        'days_diff': ['max', 'mean', 'min', 'sum'],
        'AMT_PAYMENT': ['max', 'min'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_last_days_tlc_250_group.columns = ['_'.join(column)+f'_instal_tlc_250_last_{days}' for column in instal_last_days_tlc_250_group.columns]
    instal_last_days_tlc_250_group.reset_index(inplace=True)

    installments_last_days_group = pd.merge(installments_last_days_group, instal_last_days_tlc_250_group, how = 'left', on = 'SK_ID_CURR')
    
    days_diff_thre = 0
    instal_last_days_tlc_250_ot = instal_last_days_tlc_250.loc[instal_last_days_tlc_250['days_diff']<days_diff_thre]
    instal_last_days_tlc_250_ot_group_tmp = instal_last_days_tlc_250_ot.groupby(['SK_ID_CURR','SK_ID_PREV']).agg({
        'DAYS_INSTALMENT': ['nunique',],
        'days_diff': ['max', 'mean', 'min'],
        'NUM_INSTALMENT_NUMBER': ['max', 'min', 'mean'],
        'AMT_diff': ['max', 'mean', 'sum', 'std'],
        'AMT_ratio': ['max', 'mean', 'sum', 'std']
    })
    instal_last_days_tlc_250_ot_group_tmp.columns = ['_'.join(column)+f'_instal_tlc_250_ot_tmp_last_{days}' for column in instal_last_days_tlc_250_ot_group_tmp.columns]
    instal_last_days_tlc_250_ot_group_tmp.reset_index(inplace=True)
    
    instal_last_days_tlc_250_ot_group = instal_last_days_tlc_250_ot_group_tmp.groupby('SK_ID_CURR').agg({
        f'DAYS_INSTALMENT_nunique_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum', 'mean'],
        f'days_diff_max_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'mean'],
        f'days_diff_min_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'mean'],
        f'days_diff_mean_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'mean'],
        f'NUM_INSTALMENT_NUMBER_max_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'mean'],
        f'NUM_INSTALMENT_NUMBER_min_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'mean'],
        f'NUM_INSTALMENT_NUMBER_mean_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'mean'],
        f'AMT_diff_max_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_diff_mean_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_diff_sum_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_diff_std_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_ratio_max_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_ratio_mean_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_ratio_sum_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
        f'AMT_ratio_std_instal_tlc_250_ot_tmp_last_{days}': ['max', 'min', 'sum'],
    })
    instal_last_days_tlc_250_ot_group.columns = ['_'.join(column) for column in instal_last_days_tlc_250_ot_group.columns]
    instal_last_days_tlc_250_ot_group.reset_index(inplace=True)

    installments_last_days_group = pd.merge(installments_last_days_group, instal_last_days_tlc_250_ot_group, how = 'left', on = 'SK_ID_CURR')
    
    return installments_last_days_group
    
def installments_payments_process(installments_payments):
    
    installments_payments['days_diff'] = installments_payments['DAYS_INSTALMENT'] - installments_payments['DAYS_ENTRY_PAYMENT']
    installments_payments['AMT_diff'] = installments_payments['AMT_INSTALMENT'] - installments_payments['AMT_PAYMENT']
    installments_payments['AMT_ratio'] = installments_payments['AMT_PAYMENT'] / installments_payments['AMT_INSTALMENT']
    
    installments_payments = installments_payments.sort_values(['SK_ID_CURR','SK_ID_PREV','NUM_INSTALMENT_NUMBER'])
    
    installments_group = installments_payments_days_process(installments_payments, days=None)
    installments_group_last_730 = installments_payments_days_process(installments_payments, days=730)
    installments_group_last_365 = installments_payments_days_process(installments_payments, days=365)
    installments_group_last_180 = installments_payments_days_process(installments_payments, days=180)
    installments_group_last_90 = installments_payments_days_process(installments_payments, days=90)
    
    installments_group = pd.merge(installments_group, installments_group_last_730, how = 'left', on = 'SK_ID_CURR')
    installments_group = pd.merge(installments_group, installments_group_last_365, how = 'left', on = 'SK_ID_CURR')
    installments_group = pd.merge(installments_group, installments_group_last_180, how = 'left', on = 'SK_ID_CURR')
    installments_group = pd.merge(installments_group, installments_group_last_90, how = 'left', on = 'SK_ID_CURR')
    
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

