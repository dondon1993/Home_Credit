import pandas as pd
import numpy as np
import pickle
# process a subset of credit_card_balance data according to months from current time
def credit_card_balance_months_process(credit_card_balance, months=None):
    
    if days == None:
        credit_card_balance_last_months = credit_card_balance
    else:
        credit_card_balance_last_months = credit_card_balance.loc[credit_card_balance['MONTHS_BALANCE'] >= -months]
    
    credit_card_balance_last_months_group = credit_card_balance_last_months.groupby('SK_ID_CURR').agg({
            'diff_AMT_PAYMENT': ['sum','count', 'max', 'min', 'mean'],
            'ratio_AMT_PAYMENT': ['sum','max', 'min', 'mean'],
            'credit_utilization_ratio_1': ['sum','max', 'min', 'mean'],
            'credit_utilization_ratio_2': ['sum','max', 'min', 'mean'],
            'credit_utilization_diff_1': ['sum','max', 'min', 'mean'],
            'credit_utilization_diff_2': ['sum','max', 'min', 'mean'],
            'AMT_BALANCE': ['mean','sum','max','min'],
            'AMT_CREDIT_LIMIT_ACTUAL': ['mean', 'sum','max','min'],
            'AMT_INST_MIN_REGULARITY': ['mean', 'sum','max','min'],
            'AMT_PAYMENT_TOTAL_CURRENT': ['mean', 'sum','max','min'],
            'CNT_INSTALMENT_MATURE_CUM': ['mean', 'sum','max','min'],
            'SK_DPD': ['mean', 'max', 'min', 'sum'],
            'SK_DPD_DEF': ['mean', 'max', 'min', 'sum'],
    })
    credit_card_balance_last_months_group.columns = ['_'.join(column)+f'_credit_card_last_{months}' for column in credit_card_balance_last_months_group.columns]
    credit_card_balance_last_months_group.reset_index(inplace = True)
    
    return credit_card_balance_last_months_group

def credit_card_balance_process(credit_card_balance):
    
    credit_card_balance['diff_AMT_PAYMENT'] = credit_card_balance['AMT_PAYMENT_CURRENT'] - credit_card_balance['AMT_INST_MIN_REGULARITY']
    credit_card_balance['ratio_AMT_PAYMENT'] = credit_card_balance['AMT_PAYMENT_CURRENT'] / credit_card_balance['AMT_INST_MIN_REGULARITY']
    credit_card_balance['credit_utilization_ratio_1'] = credit_card_balance['AMT_BALANCE']/credit_card_balance['AMT_CREDIT_LIMIT_ACTUAL']
    credit_card_balance['credit_utilization_ratio_2'] = (credit_card_balance['AMT_BALANCE'] - credit_card_balance['AMT_PAYMENT_TOTAL_CURRENT'])/credit_card_balance['AMT_CREDIT_LIMIT_ACTUAL']
    credit_card_balance['credit_utilization_diff_1'] = credit_card_balance['AMT_BALANCE'] - credit_card_balance['AMT_CREDIT_LIMIT_ACTUAL']
    credit_card_balance['credit_utilization_diff_2'] = (credit_card_balance['AMT_BALANCE'] - credit_card_balance['AMT_PAYMENT_TOTAL_CURRENT']) - credit_card_balance['AMT_CREDIT_LIMIT_ACTUAL']

    credit_card_balance_group = credit_card_balance.groupby('SK_ID_CURR').agg({'SK_ID_PREV': 'nunique'})
    credit_card_balance_group.columns = ['num_credit_card']
    credit_card_balance_group.reset_index(inplace=True)
    # Aggregation with respect to under payment data
    tmp = credit_card_balance.loc[credit_card_balance['diff_AMT_PAYMENT']<0]
    credit_card_balance_DPD_group = tmp.groupby(['SK_ID_CURR']).agg({
            'diff_AMT_PAYMENT': ['sum','count', 'max', 'min', 'mean'],
            'ratio_AMT_PAYMENT': ['sum','max', 'min', 'mean'],
            'credit_utilization_ratio_1': ['sum','max', 'min', 'mean'],
            'credit_utilization_ratio_2': ['sum','max', 'min', 'mean'],
            'credit_utilization_diff_1': ['sum','max', 'min', 'mean'],
            'credit_utilization_diff_2': ['sum','max', 'min', 'mean'],
            'AMT_BALANCE': ['mean','sum','max','min'],
            'AMT_CREDIT_LIMIT_ACTUAL': ['mean', 'sum','max','min'],
            'AMT_INST_MIN_REGULARITY': ['mean', 'sum','max','min'],
            'AMT_PAYMENT_TOTAL_CURRENT': ['mean', 'sum','max','min'],
            'CNT_INSTALMENT_MATURE_CUM': ['mean', 'sum','max','min'],
            'SK_DPD': ['mean', 'max', 'min', 'sum'],
            'SK_DPD_DEF': ['mean', 'max', 'min', 'sum'],
    })
    credit_card_balance_DPD_group.columns = ['_'.join(column) + 'under_pay_credit_card' for column in credit_card_balance_DPD_group.columns]
    credit_card_balance_DPD_group.reset_index(inplace = True)

    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_DPD_group, how='left', on='SK_ID_CURR')
    # Aggregation with respect to all data, data in last 2 years, 1 year, 6 months and 3 months
    credit_card_balance_group_all = credit_card_balance_months_process(credit_card_balance, months=None)
    credit_card_balance_group_last_24months = credit_card_balance_months_process(credit_card_balance, months=24)
    credit_card_balance_group_last_12months = credit_card_balance_months_process(credit_card_balance, months=12)
    credit_card_balance_group_last_6months = credit_card_balance_months_process(credit_card_balance, months=6)
    credit_card_balance_group_last_3months = credit_card_balance_months_process(credit_card_balance, months=3)
    
    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_group_all, how='left', on='SK_ID_CURR')
    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_group_last_24months, how='left', on='SK_ID_CURR')
    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_group_last_12months, how='left', on='SK_ID_CURR')
    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_group_last_6months, how='left', on='SK_ID_CURR')
    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_group_last_3months, how='left', on='SK_ID_CURR')

    return credit_card_balance_group


if __name__ == "__main__":
    
    credit_card_balance = pd.read_csv('../input/credit_card_balance.csv')
    
    credit_card_balance_group = credit_card_balance_process(credit_card_balance)
    
    with open('../processed/credit_card_balance_processed.pickle', 'wb') as handle:
        pickle.dump(credit_card_balance_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

