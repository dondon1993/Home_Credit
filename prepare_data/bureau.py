import numpy as np
import pandas as pd

def bureau_process(bureau):
    
    bureau['Annuity_calculated'] = bureau['AMT_CREDIT_SUM']/ (bureau['DAYS_CREDIT_ENDDATE']-bureau['DAYS_CREDIT']) * 30
    bureau.loc[bureau['CREDIT_TYPE']=='Credit card','Annuity_calculated']=0
    
    bureau_group = bureau.groupby('SK_ID_CURR').agg({'SK_ID_CURR':'count'})
    bureau_group.columns = ['record_bureau_count']
    bureau_group.reset_index(inplace = True)
    
    bureau_stats = bureau.groupby('SK_ID_CURR').agg({
                'DAYS_CREDIT': ['max', 'min', 'mean'],
                'CREDIT_DAY_OVERDUE': ['max', 'min', 'mean'],
                'DAYS_CREDIT_ENDDATE': ['max', 'min', 'mean'],
                'DAYS_ENDDATE_FACT': ['max', 'min', 'mean'],
                'AMT_CREDIT_MAX_OVERDUE': ['max', 'min', 'mean', 'sum'],
                'CNT_CREDIT_PROLONG': ['max', 'min', 'mean', 'sum'],
                'AMT_CREDIT_SUM': ['max', 'min', 'mean', 'sum'],
                'AMT_CREDIT_SUM_DEBT': ['max', 'min', 'mean', 'sum'],
                'AMT_CREDIT_SUM_OVERDUE': ['max', 'min', 'mean', 'sum'],
                'AMT_CREDIT_SUM_LIMIT': ['max', 'min', 'mean', 'sum'],
                'DAYS_CREDIT_UPDATE': ['max', 'min', 'mean'],
                'AMT_ANNUITY': ['max', 'min', 'mean', 'sum'],
    })
    bureau_stats.columns = ['_'.join(column)+'_stats_bureau' for column in bureau_stats.columns]
    bureau_stats.reset_index(inplace = True)

    bureau_group = pd.merge(bureau_group, bureau_stats, how = 'left', on = ['SK_ID_CURR'])
    
    bureau_active = bureau.loc[bureau['CREDIT_ACTIVE']=='Active']
    bureau_active_group = bureau_active.groupby('SK_ID_CURR').agg({
        'SK_ID_CURR': 'count',
        'AMT_CREDIT_SUM_DEBT': ['max', 'min', 'sum'],
        'AMT_ANNUITY': ['max', 'min', 'sum'],
        'Annuity_calculated': ['max', 'min', 'sum'],
        'DAYS_CREDIT': ['max', 'min', 'mean'],
        'CREDIT_DAY_OVERDUE': ['max', 'min', 'mean'],
        'DAYS_CREDIT_ENDDATE': ['max', 'min', 'mean'],
        'DAYS_ENDDATE_FACT': ['max', 'min', 'mean'],
        'AMT_CREDIT_MAX_OVERDUE': ['max', 'min', 'mean', 'sum'],
        'CNT_CREDIT_PROLONG': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM_OVERDUE': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM_LIMIT': ['max', 'min', 'mean', 'sum'],
        'DAYS_CREDIT_UPDATE': ['max', 'min', 'mean'],
    })
    bureau_active_group.columns = ['_'.join(column)+'_active_bureau' for column in bureau_active_group.columns]
    bureau_active_group.reset_index(inplace=True)

    bureau_group = pd.merge(bureau_group, bureau_active_group, how='left', on = ['SK_ID_CURR'])
    bureau_group['active_ratio_bureau'] = bureau_group['SK_ID_CURR_count_active_bureau']/bureau_group['record_bureau_count']
    
    bureau_closed = bureau.loc[bureau['CREDIT_ACTIVE']=='Closed']
    bureau_closed['Days_diff'] = bureau_closed['DAYS_ENDDATE_FACT'] - bureau_closed['DAYS_CREDIT_ENDDATE']

    bureau_closed_group = bureau_closed.groupby('SK_ID_CURR').agg({
        'Days_diff': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM_DEBT': ['max', 'min', 'sum'],
        'AMT_ANNUITY': ['max', 'min', 'sum'],
        'Annuity_calculated': ['max', 'min', 'sum'],
        'DAYS_CREDIT': ['max', 'min', 'mean'],
        'CREDIT_DAY_OVERDUE': ['max', 'min', 'mean'],
        'DAYS_CREDIT_ENDDATE': ['max', 'min', 'mean'],
        'DAYS_ENDDATE_FACT': ['max', 'min', 'mean'],
        'AMT_CREDIT_MAX_OVERDUE': ['max', 'min', 'mean', 'sum'],
        'CNT_CREDIT_PROLONG': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM_OVERDUE': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT_SUM_LIMIT': ['max', 'min', 'mean', 'sum'],
        'DAYS_CREDIT_UPDATE': ['max', 'min', 'mean'],
    })
    bureau_closed_group.columns = ['_'.join(column)+'_closed_bureau' for column in bureau_closed_group.columns]
    bureau_closed_group.reset_index(inplace=True)

    bureau_group = pd.merge(bureau_group, bureau_closed_group, how = 'left', on = ['SK_ID_CURR'])
    
    return bureau_group


if __name__ == "__main__":
    
    bureau = pd.read_csv('../input/bureau.csv')
    
    bureau_group = bureau_process(bureau)
    
    with open('../processed/bureau_processed.pickle', 'wb') as handle:
        pickle.dump(bureau_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

