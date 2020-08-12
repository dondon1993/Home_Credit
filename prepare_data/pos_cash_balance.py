import numpy as np
import pandas as pd
import pickle
# process POS_CASH_balance data
def POS_CASH_balance_group_process(POS_CASH_balance):
    
    POS_CASH_balance_group = POS_CASH_balance.groupby('SK_ID_CURR').agg({
            'SK_ID_PREV': ['nunique'],
            'SK_DPD': ['max', 'mean', 'sum'],
            'SK_DPD_DEF': ['max', 'mean', 'sum'],
            'MONTHS_BALANCE': ['max', 'mean', 'size'],
    })
    POS_CASH_balance_group.columns = ['_'.join(column)+'_POS_CASH' for column in POS_CASH_balance_group.columns]
    POS_CASH_balance_group.reset_index(inplace = True)
    
    POS_CASH_balance_comp = POS_CASH_balance.loc[POS_CASH_balance['NAME_CONTRACT_STATUS']=='Completed']
    POS_CASH_balance_comp_group = POS_CASH_balance_comp.groupby('SK_ID_CURR').agg({'SK_ID_CURR': 'count'})
    POS_CASH_balance_comp_group.columns = ['loan_completed_POS_CASH']
    POS_CASH_balance_comp_group.reset_index(inplace = True)

    POS_CASH_balance_group = pd.merge(POS_CASH_balance_group, POS_CASH_balance_comp_group, how = 'left', on = 'SK_ID_CURR')
    
    POS_CASH_balance_group_num = POS_CASH_balance.groupby(['SK_ID_CURR','SK_ID_PREV']).agg({
            'CNT_INSTALMENT': ['max', 'count']
    })
    POS_CASH_balance_group_num.columns = ['_'.join(column)+'_POS_CASH' for column in POS_CASH_balance_group_num.columns]
    POS_CASH_balance_group_num.reset_index(inplace = True)

    POS_CASH_balance_group_1 = POS_CASH_balance_group_num.groupby('SK_ID_CURR').agg({
        'CNT_INSTALMENT_max_POS_CASH': ['sum'],
        'CNT_INSTALMENT_count_POS_CASH': ['sum']
    })
    POS_CASH_balance_group_1.columns = ['months_total_exp_POS_CASH', 'month_total_fact_POS_CASH']
    POS_CASH_balance_group_1.reset_index(inplace = True)

    POS_CASH_balance_group = pd.merge(POS_CASH_balance_group, POS_CASH_balance_group_1, how = 'left', on = 'SK_ID_CURR')
    
    return POS_CASH_balance_group


if __name__ == "__main__":
    
    POS_CASH_balance = pd.read_csv('../input/POS_CASH_balance.csv')
    
    POS_CASH_balance_group = POS_CASH_balance_group_process(POS_CASH_balance)
    
    with open('../processed/POS_CASH_balance_processed.pickle', 'wb') as handle:
        pickle.dump(POS_CASH_balance_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

