import pandas as pd
import numpy as np

def credit_card_balance_process(credit_card_balance):
    
    credit_card_balance['diff_AMT_PAYMENT'] = credit_card_balance['AMT_PAYMENT_CURRENT'] - credit_card_balance['AMT_INST_MIN_REGULARITY']
    
    credit_card_balance_group = credit_card_balance.groupby('SK_ID_CURR').agg({'SK_ID_PREV': 'nunique'})
    credit_card_balance_group.columns = ['num_credit_card']
    credit_card_balance_group.reset_index(inplace=True)
    
    tmp = credit_card_balance.loc[credit_card_balance['diff_AMT_PAYMENT']<0]
    credit_card_balance_DPD_group = tmp.groupby(['SK_ID_CURR']).agg({
            'diff_AMT_PAYMENT': ['mean','count','sum','max','min'],
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
    
    credit_card_balance_sum_group = credit_card_balance.groupby('SK_ID_CURR').agg({
            'diff_AMT_PAYMENT': ['sum','count'],
            'AMT_BALANCE': ['mean','sum','max','min'],
            'AMT_CREDIT_LIMIT_ACTUAL': ['mean', 'sum','max','min'],
            'AMT_INST_MIN_REGULARITY': ['mean', 'sum','max','min'],
            'AMT_PAYMENT_TOTAL_CURRENT': ['mean', 'sum','max','min'],
            'CNT_INSTALMENT_MATURE_CUM': ['mean', 'sum','max','min'],
            'SK_DPD': ['mean', 'max', 'min', 'sum'],
            'SK_DPD_DEF': ['mean', 'max', 'min', 'sum'],
    })
    credit_card_balance_sum_group.columns = ['_'.join(column)+'_credit_card' for column in credit_card_balance_sum_group.columns]
    credit_card_balance_sum_group.reset_index(inplace = True)

    credit_card_balance_group = pd.merge(credit_card_balance_group, credit_card_balance_sum_group, how = 'left', on = 'SK_ID_CURR')
    
    return credit_card_balance_group


if __name__ == "__main__":
    
    credit_card_balance = pd.read_csv('./input/credit_card_balance.csv')
    
    credit_card_balance_group = credit_card_balance_process(credit_card_balance)
    
    with open('../processed/credit_card_balance_processed.pickle', 'wb') as handle:
        pickle.dump(credit_card_balance_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

