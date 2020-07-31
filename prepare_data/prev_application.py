import pandas as pd
import numpy as np
import pickle

def prev_application_process(previous_application,POS_CASH_balance,credit_card_balance,installments_payments):
    
    previous_application = previous_application.loc[previous_application['NAME_CONTRACT_TYPE']!='XNA']
    POS_CASH_balance = POS_CASH_balance.sort_values(['SK_ID_CURR','SK_ID_PREV','MONTHS_BALANCE'])
    credit_card_balance = credit_card_balance.sort_values(['SK_ID_CURR','SK_ID_PREV','MONTHS_BALANCE'])
    installments_payments = installments_payments.sort_values(['SK_ID_CURR','SK_ID_PREV','NUM_INSTALMENT_NUMBER'])
    
    POS_CASH_latest = POS_CASH_balance.groupby(['SK_ID_CURR','SK_ID_PREV']).nth(-1)
    POS_CASH_latest.reset_index(inplace=True)
    POS_CASH_latest.columns = [column + '_latest_POS_CASH' if column not in ['SK_ID_CURR', 'SK_ID_PREV'] else column for column in POS_CASH_latest.columns]

    credit_card_latest = credit_card_balance.groupby(['SK_ID_CURR','SK_ID_PREV']).nth(-1)
    credit_card_latest.reset_index(inplace=True)
    credit_card_latest.columns = [column + '_latest_credit_card' if column not in ['SK_ID_CURR', 'SK_ID_PREV'] else column for column in credit_card_latest.columns]

    installments_payments_latest = installments_payments.groupby(['SK_ID_CURR', 'SK_ID_PREV']).nth(-1)
    installments_payments_latest.reset_index(inplace=True)
    installments_payments_latest.columns = [column + '_latest_installments' if column not in ['SK_ID_CURR', 'SK_ID_PREV'] else column for column in installments_payments_latest.columns]
    
    columns_POS_CASH = ['SK_ID_CURR', 'SK_ID_PREV', 'CNT_INSTALMENT_latest_POS_CASH','CNT_INSTALMENT_FUTURE_latest_POS_CASH',
                       'NAME_CONTRACT_STATUS_latest_POS_CASH', ]
    previous_application = pd.merge(previous_application, POS_CASH_latest[columns_POS_CASH], how='left', on=['SK_ID_CURR','SK_ID_PREV'])
    
    columns_credit_card = ['SK_ID_CURR', 'SK_ID_PREV', 'AMT_BALANCE_latest_credit_card', 'AMT_INST_MIN_REGULARITY_latest_credit_card',
                          'NAME_CONTRACT_STATUS_latest_credit_card',]
    previous_application = pd.merge(previous_application, credit_card_latest[columns_credit_card], how = 'left', on=['SK_ID_CURR','SK_ID_PREV'])
    
    previous_application['STATUS'] = 'Unkown'
    previous_application.loc[previous_application['NAME_CONTRACT_TYPE']=='Revolving loans','STATUS'] =     previous_application.loc[previous_application['NAME_CONTRACT_TYPE']=='Revolving loans','NAME_CONTRACT_STATUS_latest_credit_card']
    previous_application.loc[previous_application['NAME_CONTRACT_TYPE']!='Revolving loans','STATUS'] =     previous_application.loc[previous_application['NAME_CONTRACT_TYPE']!='Revolving loans','NAME_CONTRACT_STATUS_latest_POS_CASH']
    
    columns = ['DAYS_FIRST_DRAWING',
               'DAYS_FIRST_DUE',
               'DAYS_LAST_DUE_1ST_VERSION',
               'DAYS_LAST_DUE',
               'DAYS_TERMINATION'
              ]

    for column in columns:
        previous_application.loc[previous_application[column]==365243,column]=np.nan
        
    previous_application['APP_CREDIT_PERC'] = previous_application['AMT_APPLICATION'] / previous_application['AMT_CREDIT']
    previous_application['diff_AMT'] = previous_application['AMT_CREDIT'] - previous_application['AMT_APPLICATION']
    previous_application['diff_days_1'] = previous_application['DAYS_FIRST_DUE'] - previous_application['DAYS_LAST_DUE_1ST_VERSION']
    previous_application['diff_days_2'] = previous_application['DAYS_LAST_DUE_1ST_VERSION'] - previous_application['DAYS_LAST_DUE']
    previous_application['diff_days_3'] = previous_application['DAYS_LAST_DUE'] - previous_application['DAYS_TERMINATION']
    
    previous_application_group = previous_application.groupby('SK_ID_CURR').agg({'SK_ID_CURR': 'count'})
    previous_application_group.columns = ['num_record_total_previous_application']
    previous_application_group.reset_index(inplace = True)

    states = previous_application['NAME_CONTRACT_STATUS'].unique()
    for state in states:
        tmp = previous_application.loc[previous_application['NAME_CONTRACT_STATUS']==state]

        tmp_group = tmp.groupby('SK_ID_CURR').agg({'SK_ID_CURR': 'count'})
        tmp_group.columns = [f'num_record_{state}_previous_application']
        tmp_group.reset_index(inplace = True)

        previous_application_group = pd.merge(previous_application_group, tmp_group, how = 'left', on = 'SK_ID_CURR')
        previous_application_group[f'ratio_{state}_previous_application'] = previous_application_group[f'num_record_{state}_previous_application']/previous_application_group['num_record_total_previous_application']

    previous_application_group[f'ratio_app_ref_previous_application'] = previous_application_group[f'ratio_Approved_previous_application'] / previous_application_group[f'ratio_Refused_previous_application']
    previous_application_group[f'ratio_app_cancel_previous_application'] = previous_application_group[f'ratio_Approved_previous_application'] / previous_application_group[f'ratio_Canceled_previous_application']   
    
    num_agg = {
        'AMT_ANNUITY': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT': ['max', 'min', 'mean', 'sum'],
        'AMT_APPLICATION': ['min', 'max', 'mean'],
        'APP_CREDIT_PERC': ['min', 'max', 'mean', 'std'],
        'AMT_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'AMT_GOODS_PRICE': ['min', 'max', 'mean'],
        'HOUR_APPR_PROCESS_START': ['min', 'max', 'mean'],
        'DAYS_FIRST_DUE': ['max','min','mean'],
        'RATE_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'DAYS_LAST_DUE_1ST_VERSION': ['max', 'min', 'mean'],
        'DAYS_DECISION': ['min', 'max', 'mean'],
        'CNT_PAYMENT': ['max', 'min', 'mean', 'sum'],
    }
    
    tmp = previous_application.groupby('SK_ID_CURR').agg(num_agg)
    tmp.columns = [('_'.join(column) +'_previous_application') for column in tmp.columns]
    tmp.reset_index(inplace = True)
    previous_application_group = pd.merge(previous_application_group, tmp, how = 'left', on = 'SK_ID_CURR')
    
    previous_application_app = previous_application.loc[previous_application['NAME_CONTRACT_STATUS']=='Approved']
    app_group = previous_application_app.groupby('SK_ID_CURR').agg({
        'AMT_ANNUITY': ['max', 'mean', 'sum'],
        'AMT_CREDIT': ['max', 'mean', 'sum'],
        'AMT_APPLICATION': ['min', 'max', 'mean'],
        'APP_CREDIT_PERC': ['min', 'max', 'mean', 'std'],
        'AMT_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'AMT_GOODS_PRICE': ['min', 'max', 'mean'],
        'HOUR_APPR_PROCESS_START': ['min', 'max', 'mean'],
        'DAYS_FIRST_DRAWING': ['max', 'min', 'mean'],
        'DAYS_FIRST_DUE': ['max', 'min', 'mean'],
        'DAYS_LAST_DUE_1ST_VERSION': ['max', 'min', 'mean'],
        'DAYS_LAST_DUE': ['max', 'min', 'mean'],
        'DAYS_TERMINATION': ['max', 'min', 'mean'],
        'RATE_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'DAYS_DECISION': ['min', 'max', 'mean'],
        'CNT_PAYMENT': ['max', 'min', 'mean', 'sum'],
        'diff_AMT': ['max', 'min', 'mean'],
        'diff_days_1': ['max', 'min', 'mean'],
        'diff_days_2': ['max', 'min', 'mean'],
        'diff_days_3': ['max', 'min', 'mean']
    })
    app_group.columns = [('_'.join(column) + '_app' + '_previous_application') for column in app_group.columns]
    app_group.reset_index(inplace = True)

    previous_application_group = pd.merge(previous_application_group, app_group, how = 'left', on = 'SK_ID_CURR')

    tlc = 10
    previous_application_tlc = previous_application.loc[(previous_application['NAME_CONTRACT_STATUS']=='Approved')&(previous_application['diff_days_2']<-tlc)]
    previous_application_tlc_group = previous_application_tlc.groupby(['SK_ID_CURR']).agg({
        'SK_ID_PREV':['count'],
        'AMT_ANNUITY': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT': ['max', 'min', 'mean', 'sum'],
        'AMT_APPLICATION': ['min', 'max', 'mean'],
        'APP_CREDIT_PERC': ['min', 'max', 'mean', 'std'],
        'AMT_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'AMT_GOODS_PRICE': ['min', 'max', 'mean'],
        'HOUR_APPR_PROCESS_START': ['min', 'max', 'mean'],
        'DAYS_FIRST_DUE': ['max','min','mean'],
        'RATE_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'DAYS_LAST_DUE_1ST_VERSION': ['max', 'min', 'mean'],
        'DAYS_DECISION': ['min', 'max', 'mean'],
        'CNT_PAYMENT': ['max', 'min', 'mean', 'sum'],
        'diff_AMT': ['max', 'min', 'mean'],
        'diff_days_1': ['max', 'min', 'mean'],
        'diff_days_2': ['max', 'min', 'mean'],
        'diff_days_3': ['max', 'min', 'mean'],
    })
    previous_application_tlc_group.columns = ['_'.join(column)+f'_{tlc}_prev_application' for column in previous_application_tlc_group.columns]
    previous_application_tlc_group.reset_index(inplace = True)

    previous_application_group = pd.merge(previous_application_group, previous_application_tlc_group, how='left', on='SK_ID_CURR')
    previous_application_group[f'ratio_{tlc}'] = previous_application_group[f'SK_ID_PREV_count_{tlc}_prev_application']/previous_application_group['num_record_Approved_previous_application']
    
    tlc = 30
    previous_application_tlc = previous_application.loc[(previous_application['NAME_CONTRACT_STATUS']=='Approved')&(previous_application['diff_days_2']<-tlc)]
    previous_application_tlc_group = previous_application_tlc.groupby(['SK_ID_CURR']).agg({
        'SK_ID_PREV':['count'],
        'AMT_ANNUITY': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT': ['max', 'min', 'mean', 'sum'],
        'AMT_APPLICATION': ['min', 'max', 'mean'],
        'APP_CREDIT_PERC': ['min', 'max', 'mean', 'std'],
        'AMT_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'AMT_GOODS_PRICE': ['min', 'max', 'mean'],
        'HOUR_APPR_PROCESS_START': ['min', 'max', 'mean'],
        'DAYS_FIRST_DUE': ['max','min','mean'],
        'RATE_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'DAYS_LAST_DUE_1ST_VERSION': ['max', 'min', 'mean'],
        'DAYS_DECISION': ['min', 'max', 'mean'],
        'CNT_PAYMENT': ['max', 'min', 'mean', 'sum'],
        'diff_AMT': ['max', 'min', 'mean'],
        'diff_days_1': ['max', 'min', 'mean'],
        'diff_days_2': ['max', 'min', 'mean'],
        'diff_days_3': ['max', 'min', 'mean'],
    })
    previous_application_tlc_group.columns = ['_'.join(column)+f'_{tlc}_prev_application' for column in previous_application_tlc_group.columns]
    previous_application_tlc_group.reset_index(inplace = True)

    previous_application_group = pd.merge(previous_application_group, previous_application_tlc_group, how='left', on='SK_ID_CURR')
    previous_application_group[f'ratio_{tlc}'] = previous_application_group[f'SK_ID_PREV_count_{tlc}_prev_application']/previous_application_group['num_record_Approved_previous_application']
    
    previous_application_uo = previous_application.loc[previous_application['NAME_CONTRACT_STATUS']=='Unused offer']
    previous_application_uo['diff_AMT'] = previous_application_uo['AMT_CREDIT'] - previous_application_uo['AMT_APPLICATION']
    uo_group = previous_application_uo.groupby('SK_ID_CURR').agg({
        'AMT_ANNUITY': ['max', 'mean'],
        'AMT_CREDIT': ['max', 'mean'],
        'NFLAG_LAST_APPL_IN_DAY': ['max', 'mean'],
        'diff_AMT': ['max', 'min', 'mean'],
    })
    uo_group.columns = [('_'.join(column) + '_uo' + '_previous_application') for column in uo_group.columns]
    uo_group.reset_index(inplace = True)

    previous_application_group = pd.merge(previous_application_group, uo_group, how = 'left', on = 'SK_ID_CURR')
    
    previous_application_active = previous_application.loc[previous_application['STATUS']=='Active']
    previous_application_active['AMT_BALANCE_CASH_LOANS'] = previous_application_active['AMT_ANNUITY'] * previous_application_active['CNT_INSTALMENT_FUTURE_latest_POS_CASH']
    
    previous_application_active_group = previous_application_active.groupby('SK_ID_CURR').agg({
        'AMT_ANNUITY': ['max', 'min', 'mean', 'sum'],
        'AMT_CREDIT': ['max', 'min', 'mean', 'sum'],
        'AMT_BALANCE_CASH_LOANS': ['max', 'min', 'mean', 'sum'],
        'AMT_BALANCE_latest_credit_card': ['max', 'min', 'mean', 'sum'],
        'AMT_INST_MIN_REGULARITY_latest_credit_card': ['max', 'min', 'mean', 'sum']
    })
    previous_application_active_group.columns = ['_'.join(column)+'_active_prev_application' for column in previous_application_active_group.columns]
    previous_application_active_group.reset_index(inplace=True)

    previous_application_group = pd.merge(previous_application_group, previous_application_active_group, how='left', on = 'SK_ID_CURR')
    
    previous_application_active_credit = previous_application_active.loc[previous_application_active['NAME_CONTRACT_TYPE']=='Revolving loans']
    previous_application_active_credit_group = previous_application_active_credit.groupby('SK_ID_CURR').agg({
        'AMT_ANNUITY': 'sum'
    })
    previous_application_active_credit_group.columns = ['_'.join(column) + '_active_credit_prev_application' for column in previous_application_active_credit_group.columns]
    previous_application_active_credit_group.reset_index(inplace = True)

    previous_application_group = pd.merge(previous_application_group, previous_application_active_credit_group, how = 'left', on = 'SK_ID_CURR')
    
    previous_application_closed = previous_application.loc[previous_application['STATUS']=='Closed']
    previous_application_closed['days_diff'] = previous_application_closed['DAYS_LAST_DUE'] - previous_application_closed['DAYS_LAST_DUE_1ST_VERSION']

    previous_application_closed_group = previous_application_closed.groupby('SK_ID_CURR').agg({
        'days_diff': ['max', 'min', 'sum', 'mean'],
    })
    previous_application_closed_group.columns = ['_'.join(column)+'_closed_prev_application' for column in previous_application_closed_group.columns]
    previous_application_closed_group.reset_index(inplace=True)

    previous_application_group = pd.merge(previous_application_group, previous_application_closed_group, how='left', on='SK_ID_CURR')
    
    previous_application_ref = previous_application.loc[previous_application['NAME_CONTRACT_STATUS']=='Refused']
    ref_group = previous_application_ref.groupby('SK_ID_CURR').agg({
        'AMT_ANNUITY': ['max', 'mean', 'sum'],
        'AMT_CREDIT': ['max', 'mean', 'sum'],
        'AMT_APPLICATION': ['min', 'max', 'mean'],
        'APP_CREDIT_PERC': ['min', 'max', 'mean', 'std'],
        'AMT_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'AMT_GOODS_PRICE': ['min', 'max', 'mean'],
        'HOUR_APPR_PROCESS_START': ['min', 'max', 'mean'],
        'DAYS_FIRST_DRAWING': ['max', 'min', 'mean'],
        'DAYS_FIRST_DUE': ['max', 'min', 'mean'],
        'DAYS_LAST_DUE_1ST_VERSION': ['max', 'min', 'mean'],
        'DAYS_LAST_DUE': ['max', 'min', 'mean'],
        'DAYS_TERMINATION': ['max', 'min', 'mean'],
        'RATE_DOWN_PAYMENT': ['min', 'max', 'mean'],
        'DAYS_DECISION': ['min', 'max', 'mean'],
        'CNT_PAYMENT': ['max', 'min', 'mean', 'sum'],
        'diff_AMT': ['max', 'min', 'mean'],
        'diff_days_1': ['max', 'min', 'mean'],
        'diff_days_2': ['max', 'min', 'mean'],
        'diff_days_3': ['max', 'min', 'mean']
    })
    ref_group.columns = [('_'.join(column) + '_ref' + '_previous_application') for column in ref_group.columns]
    ref_group.reset_index(inplace = True)

    previous_application_group = pd.merge(previous_application_group, ref_group, how = 'left', on = 'SK_ID_CURR')
    
    return previous_application_group


if __name__ == "__main__":
    
    previous_application = pd.read_csv('../input/previous_application.csv')
    POS_CASH_balance = pd.read_csv('../input/POS_CASH_balance.csv')
    credit_card_balance = pd.read_csv('../input/credit_card_balance.csv')
    installments_payments = pd.read_csv('../input/installments_payments.csv')
    
    previous_application_group = prev_application_process(previous_application,POS_CASH_balance,
								credit_card_balance,installments_payments)
    
    with open('../processed/previous_application_processed.pickle', 'wb') as handle:
        pickle.dump(previous_application_group, handle, protocol = pickle.HIGHEST_PROTOCOL)

