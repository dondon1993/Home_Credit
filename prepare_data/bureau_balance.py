import numpy as np
import pandas as pd

def bureau_balance_process(bureau_balance, bureau):
    
    columns = ['SK_ID_CURR', 'SK_ID_BUREAU']
    bureau_balance = pd.merge(bureau_balance, bureau[columns], how = 'left', on = ['SK_ID_BUREAU'])
    
    bureau_balance_group = bureau_balance.groupby('SK_ID_CURR').agg({'MONTHS_BALANCE': ['max', 'min']})
    bureau_balance_group.columns = ['_'.join(column)+'_bureau_balance' for column in bureau_balance_group.columns]
    bureau_balance_group.reset_index(inplace=True)

    bureau_balance_group['history_bureau_balance'] = bureau_balance_group['MONTHS_BALANCE_max_bureau_balance'] -                                                      bureau_balance_group['MONTHS_BALANCE_min_bureau_balance']
    
    bureau_balance_account_group = bureau_balance.groupby(['SK_ID_CURR', 'SK_ID_BUREAU']).agg({'MONTHS_BALANCE': ['max', 'min']})
    bureau_balance_account_group.columns = ['_'.join(column)+'_bureau_balance' for column in bureau_balance_account_group.columns]
    bureau_balance_account_group.reset_index(inplace=True)

    bureau_balance_account_group['duration_bureau_balance'] = bureau_balance_account_group['MONTHS_BALANCE_max_bureau_balance'] -                                                              bureau_balance_account_group['MONTHS_BALANCE_min_bureau_balance'] + 1
    
    states = bureau_balance['STATUS'].unique()
    for state in states:
        tmp = bureau_balance.loc[bureau_balance['STATUS']==state]
        tmp_account_group = tmp.groupby(['SK_ID_CURR','SK_ID_BUREAU']).agg({'MONTHS_BALANCE': ['count']})
        tmp_account_group.columns = [f'num_month_{state}_bureau_balance']
        tmp_account_group.reset_index(inplace=True)

        bureau_balance_account_group = pd.merge(bureau_balance_account_group, tmp_account_group, how='left',                                                on = ['SK_ID_CURR','SK_ID_BUREAU'])
        bureau_balance_account_group[f'ratio_{state}_bureau_balance'] = bureau_balance_account_group[f'num_month_{state}_bureau_balance'] /                                                                        bureau_balance_account_group['duration_bureau_balance']
        
    bureau_balance_account_group_group = bureau_balance_account_group.groupby('SK_ID_CURR').agg({'duration_bureau_balance':['max', 'min', 'mean', 'sum']})
    bureau_balance_account_group_group.columns = ['_'.join(column)+'_bureau_balance' for column in bureau_balance_account_group_group.columns]
    bureau_balance_account_group_group.reset_index(inplace=True)

    bureau_balance_group = pd.merge(bureau_balance_group, bureau_balance_account_group_group, how='left', on='SK_ID_CURR')

    columns = bureau_balance_account_group.columns[-16:]

    for column in columns:
        bureau_balance_account_group_group = bureau_balance_account_group.groupby('SK_ID_CURR').agg({column:['max', 'mean']})
        bureau_balance_account_group_group.columns = ['_'.join(column)+'_bureau_balance' for column in bureau_balance_account_group_group.columns]
        bureau_balance_account_group_group.reset_index(inplace=True)

        bureau_balance_group = pd.merge(bureau_balance_group, bureau_balance_account_group_group, how='left', on='SK_ID_CURR')
        
    bureau_balance_bad = bureau_balance.loc[(bureau_balance['STATUS'].notnull())&(bureau_balance['STATUS']!='C')
                                           &(bureau_balance['STATUS']!='X')&(bureau_balance['STATUS']!='0')]

    bureau_balance_bad_group = bureau_balance_bad.groupby('SK_ID_CURR').agg({'SK_ID_CURR': 'count'})
    bureau_balance_bad_group.columns = ['_'.join(column)+'_bad_bureau_balance' for column in bureau_balance_bad_group.columns]
    bureau_balance_bad_group.reset_index(inplace=True)

    bureau_balance_group = pd.merge(bureau_balance_group, bureau_balance_bad_group, how='left', on='SK_ID_CURR')
    
    return bureau_balance_group


if __name__ == "__main__":
    
    bureau_balance = pd.read_csv('../input/bureau_balance.csv')
    bureau = pd.read_csv('../input/bureau.csv')
    
    bureau_balance_group = bureau_balance_process(bureau_balance, bureau)
    
    with open('../processed/bureau_balance_processed.pickle', 'wb') as handle:
        pickle.dump(bureau_balance_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

