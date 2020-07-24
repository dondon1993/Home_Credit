import numpy as np
import pandas as pd

def ratio_dif_features(df, features):
    
    divide_features = ['AMT_INCOME_TOTAL', 'AMT_CREDIT', 'AMT_ANNUITY', 'AMT_GOODS_PRICE', 
                      # 'CNT_FAM_MEMBERS'
                      ]
    
    for feature in features:
        if feature not in df.columns:
            continue
        for div_feature in divide_features:
            df[f'{feature}_{div_feature}_ratio'] = df[feature]/df[div_feature]

    return df

def data_merge(df,bureau_group,bureau_balance_group,previous_application_group,installments_group,POS_CASH_balance_group,credit_card_balance_group):
    
    df = pd.merge(df, bureau_group, how='left', on='SK_ID_CURR')
    df = pd.merge(df, bureau_balance_group, how = 'left', on = 'SK_ID_CURR')
    df = pd.merge(df, previous_application_group, how='left', on = 'SK_ID_CURR')
    df = pd.merge(df, installments_group, how = 'left', on = 'SK_ID_CURR')
    df = pd.merge(df, POS_CASH_balance_group, how = 'left', on = 'SK_ID_CURR')
    df = pd.merge(df, credit_card_balance_group, how='left', on='SK_ID_CURR')
    
    return df

def data_process(df):
    
    Train = pd.read_csv('./input/application_train.csv')
    Test = pd.read_csv('./input/application_test.csv')
    Train = Train.append(Test).reset_index()
    Train = Train[Train['CODE_GENDER'] != 'XNA']
    
    docs = [_f for _f in Train.columns if 'FLAG_DOC' in _f]
    live = [_f for _f in Train.columns if ('FLAG_' in _f) & ('FLAG_DOC' not in _f) & ('_FLAG_' not in _f)]
    
    Train['DAYS_EMPLOYED'].replace(365243, np.nan, inplace= True)

    inc_by_org = Train[['AMT_INCOME_TOTAL', 'ORGANIZATION_TYPE']].groupby('ORGANIZATION_TYPE').median()['AMT_INCOME_TOTAL']

    df['DAYS_EMPLOYED'].replace(365243, np.nan, inplace= True)
        # Some simple new features (percentages)
    df['DAYS_EMPLOYED_PERC'] = df['DAYS_EMPLOYED'] / df['DAYS_BIRTH']
    df['INCOME_CREDIT_PERC'] = df['AMT_INCOME_TOTAL'] / df['AMT_CREDIT']
    df['INCOME_PER_PERSON'] = df['AMT_INCOME_TOTAL'] / df['CNT_FAM_MEMBERS']
    df['NEW_INC_PER_CHLD'] = df['AMT_INCOME_TOTAL'] / (1 + df['CNT_CHILDREN'])
    df['ANNUITY_INCOME_PERC'] = df['AMT_ANNUITY'] / df['AMT_INCOME_TOTAL']
    df['NEW_ANNUITY_TO_INCOME_RATIO'] = df['AMT_ANNUITY'] / (1 + df['AMT_INCOME_TOTAL'])
    df['PAYMENT_RATE'] = df['AMT_ANNUITY'] / df['AMT_CREDIT']
    df['NEW_SOURCES_PROD'] = df['EXT_SOURCE_1'] * df['EXT_SOURCE_2'] * df['EXT_SOURCE_3']
    df['NEW_EXT_SOURCES_MEAN'] = df[['EXT_SOURCE_1', 'EXT_SOURCE_2', 'EXT_SOURCE_3']].mean(axis=1)
    df['NEW_SCORES_STD'] = df[['EXT_SOURCE_1', 'EXT_SOURCE_2', 'EXT_SOURCE_3']].std(axis=1)
    df['NEW_SCORES_STD'] = df['NEW_SCORES_STD'].fillna(df['NEW_SCORES_STD'].mean())
    df['NEW_CAR_TO_BIRTH_RATIO'] = df['OWN_CAR_AGE'] / df['DAYS_BIRTH']
    df['NEW_CAR_TO_EMPLOY_RATIO'] = df['OWN_CAR_AGE'] / df['DAYS_EMPLOYED']
    df['NEW_PHONE_TO_BIRTH_RATIO'] = df['DAYS_LAST_PHONE_CHANGE'] / df['DAYS_BIRTH']
    df['NEW_PHONE_TO_BIRTH_RATIO'] = df['DAYS_LAST_PHONE_CHANGE'] / df['DAYS_EMPLOYED']

    df['ANNUITY_LENGTH'] = df['AMT_CREDIT'] / df['AMT_ANNUITY']
    df['ANN_LENGTH_EMPLOYED_RATIO'] = df['ANNUITY_LENGTH'] / df['DAYS_EMPLOYED']
    df['CHILDREN_RATIO'] = df['CNT_CHILDREN'] / df['CNT_FAM_MEMBERS']

    df['credit_div_goods'] = df['AMT_CREDIT'] / df['AMT_GOODS_PRICE']
    df['credit_minus_goods'] = df['AMT_CREDIT'] - df['AMT_GOODS_PRICE']
    df['reg_div_publish'] = df['DAYS_REGISTRATION'] / df['DAYS_ID_PUBLISH']
    df['birth_div_reg'] = df['DAYS_BIRTH'] / df['DAYS_REGISTRATION']
    df['document_sum'] = df['FLAG_DOCUMENT_2'] + df['FLAG_DOCUMENT_3'] + df['FLAG_DOCUMENT_4'] + df['FLAG_DOCUMENT_5'] + df['FLAG_DOCUMENT_6'] + df['FLAG_DOCUMENT_7'] + df['FLAG_DOCUMENT_8'] + df['FLAG_DOCUMENT_9'] + df['FLAG_DOCUMENT_10'] + df['FLAG_DOCUMENT_11'] + df['FLAG_DOCUMENT_12'] + df['FLAG_DOCUMENT_13'] + df['FLAG_DOCUMENT_14'] + df['FLAG_DOCUMENT_15'] + df['FLAG_DOCUMENT_16'] + df['FLAG_DOCUMENT_17'] + df['FLAG_DOCUMENT_18'] + df['FLAG_DOCUMENT_19'] + df['FLAG_DOCUMENT_20'] + df['FLAG_DOCUMENT_21']
    
    df['NEW_DOC_IND_AVG'] = df[docs].mean(axis=1)
    df['NEW_DOC_IND_STD'] = df[docs].std(axis=1)
    df['NEW_DOC_IND_KURT'] = df[docs].kurtosis(axis=1)
    df['NEW_LIVE_IND_SUM'] = df[live].sum(axis=1)
    df['NEW_LIVE_IND_STD'] = df[live].std(axis=1)
    df['NEW_LIVE_IND_KURT'] = df[live].kurtosis(axis=1)
    df['NEW_INC_BY_ORG'] = df['ORGANIZATION_TYPE'].map(inc_by_org)
    
    EXT_columns = ['EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3']
    df['EXT_mean'] = df[EXT_columns].mean(axis=1)
    df['EXT_max'] = df[EXT_columns].max(axis=1)
    df['EXT_min'] = df[EXT_columns].min(axis=1)    
    df['num_nan'] = 0
    for column in EXT_columns:
        df.loc[df[column].isnull(),'num_nan'] += 1
    
    df[f'ratio_installments_510_250'] = df[f'SK_ID_PREV_nunique_510_250']/df['SK_ID_PREV_nunique_installments']
    df[f'ratio_installments_1020_250'] = df[f'SK_ID_PREV_nunique_1020_250']/df['SK_ID_PREV_nunique_installments']
    df[f'ratio_installments_2030_250'] = df[f'SK_ID_PREV_nunique_2030_250']/df['SK_ID_PREV_nunique_installments']
    df[f'ratio_installments_3040_250'] = df[f'SK_ID_PREV_nunique_3040_250']/df['SK_ID_PREV_nunique_installments']
    
    df.loc[df['DAYS_LAST_DUE_1ST_VERSION_max_app_previous_application']>0, 'LAST_DUE_CAT'] = 0
    df.loc[df['DAYS_LAST_DUE_1ST_VERSION_max_app_previous_application']<=0, 'LAST_DUE_CAT'] = 1
    
    df['w_prev_application'] = 1
    df.loc[df['num_record_total_previous_application'].isnull(), 'w_prev_application'] = 0
    
    df['CREDIT_GOODS_ratio'] = df['AMT_CREDIT']/df['AMT_GOODS_PRICE']
    
    df['ANNUITY_ratio_1'] = df['AMT_ANNUITY'] / df['AMT_ANNUITY_max_previous_application']
    df['ANNUITY_ratio_2'] = df['AMT_ANNUITY'] / df['AMT_ANNUITY_mean_previous_application']
    df['Credit_ratio_1'] = df['AMT_CREDIT'] / df['AMT_CREDIT_max_previous_application']
    df['Credit_ratio_2'] = df['AMT_CREDIT'] / df['AMT_CREDIT_mean_previous_application']
    df['ANNUITY_ratio_3'] = df['AMT_ANNUITY'] / df['AMT_ANNUITY_max_app_previous_application']
    df['ANNUITY_ratio_4'] = df['AMT_ANNUITY'] / df['AMT_ANNUITY_mean_app_previous_application']
    df['Credit_ratio_3'] = df['AMT_CREDIT'] / df['AMT_CREDIT_max_app_previous_application']
    df['Credit_ratio_4'] = df['AMT_CREDIT'] / df['AMT_CREDIT_mean_app_previous_application']
    
    columns = ['ANNUITY_ratio_1','ANNUITY_ratio_2','ANNUITY_ratio_3','ANNUITY_ratio_4',
              'Credit_ratio_1','Credit_ratio_2','Credit_ratio_3','Credit_ratio_4']
    for column in columns:
        df.loc[(df[column]==np.inf)|(df[column]==-np.inf),column] = np.nan
        
    df.fillna({
        'AMT_CREDIT_SUM_DEBT_sum_active_bureau': 0,
        'AMT_ANNUITY_sum_active_bureau': 0,
        'Annuity_calculated_sum_active_bureau': 0,
        'AMT_BALANCE_CASH_LOANS_sum_active_prev_application': 0,
        'AMT_ANNUITY_sum_active_prev_application': 0,
        'AMT_BALANCE_latest_credit_card_sum_active_prev_application':0,
        'AMT_INST_MIN_REGULARITY_latest_credit_card_sum_active_prev_application': 0
    },inplace = True)
    
    df.loc[(df['Annuity_calculated_sum_active_bureau']==np.inf)|(df['Annuity_calculated_sum_active_bureau']==-np.inf),'Annuity_calculated_sum_active_bureau'] = 0
    
    df['balance_total'] = df['AMT_CREDIT'] +        df['AMT_CREDIT_SUM_DEBT_sum_active_bureau'] +        df['AMT_BALANCE_CASH_LOANS_sum_active_prev_application'] +        df['AMT_BALANCE_latest_credit_card_sum_active_prev_application']
    df['ratio_balance_income'] = df['balance_total'] / df['AMT_INCOME_TOTAL']
    
    df['ANNUITY_total'] = train['AMT_ANNUITY'] +        df['AMT_ANNUITY_sum_active_bureau'] +        df['Annuity_calculated_sum_active_bureau'] +        df['AMT_ANNUITY_sum_active_prev_application'] +        df['AMT_INST_MIN_REGULARITY_latest_credit_card_sum_active_prev_application']
    
    df['ratio_annuity_income'] = df['ANNUITY_total'] / df['AMT_INCOME_TOTAL']
    df['diff_annuity_income'] = df['ANNUITY_total'] - df['AMT_INCOME_TOTAL']
    
    df['weight'] = 1
    
    features = ['AMT_CREDIT_MAX_OVERDUE_max_stats_bureau','AMT_CREDIT_MAX_OVERDUE_mean_stats_bureau','AMT_CREDIT_MAX_OVERDUE_max_active_bureau',
           'AMT_CREDIT_MAX_OVERDUE_mean_active_bureau','AMT_CREDIT_SUM_max_stats_bureau','AMT_CREDIT_SUM_mean_stats_bureau',
           'AMT_CREDIT_SUM_max_active_bureau','AMT_CREDIT_SUM_mean_active_bureau','AMT_CREDIT_SUM_DEBT_max_stats_bureau',
           'AMT_CREDIT_SUM_DEBT_mean_stats_bureau','AMT_CREDIT_SUM_DEBT_max_active_bureau','AMT_CREDIT_SUM_OVERDUE_max_stats_bureau',
           'AMT_CREDIT_SUM_OVERDUE_mean_stats_bureau','AMT_CREDIT_SUM_OVERDUE_max_active_bureau','AMT_CREDIT_SUM_OVERDUE_mean_active_bureau',
           'AMT_CREDIT_SUM_LIMIT_max_stats_bureau','AMT_CREDIT_SUM_LIMIT_mean_stats_bureau','AMT_CREDIT_SUM_LIMIT_min_stats_bureau',
           'AMT_CREDIT_SUM_LIMIT_max_active_bureau','AMT_CREDIT_SUM_LIMIT_mean_active_bureau','AMT_CREDIT_SUM_LIMIT_min_active_bureau',
           'AMT_diff_max_installments','AMT_diff_mean_installments','AMT_diff_sum_installments','AMT_diff_max_good_installments_x',
           'AMT_diff_mean_good_installments_x','AMT_diff_sum_good_installments_x','AMT_diff_max_good_installments_y','AMT_diff_mean_good_installments_y',
           'AMT_diff_sum_good_installments_y','AMT_PAYMENT_max_installments','AMT_PAYMENT_min_installments','AMT_PAYMENT_mean_installments',
           'AMT_PAYMENT_max_good_installments_x','AMT_PAYMENT_min_good_installments_x','AMT_PAYMENT_mean_good_installments_x','AMT_PAYMENT_max_good_installments_y',
           'AMT_PAYMENT_min_good_installments_y','AMT_PAYMENT_mean_good_installments_y']
    
    df = ratio_dif_features(df, features)
    
    df = reduce_mem_usage(df)
    
    del Train, Test
    gc.collect()
    return df

if __name__ == "__main__":
    
    train = pd.read_csv('../input/application_train.csv')
    test = pd.read_csv('../input/application_test.csv')
	with open('../processed/bureau_group.pickle', 'rb') as handle:
		bureau_group = pickle.load(handle)
    with open('../processed/bureau_balance_group.pickle', 'rb') as handle:
		bureau_balance_group = pickle.load(handle)
    with open('../processed/credit_card_balance_group.pickle', 'rb') as handle:
		credit_card_balance_group = pickle.load(handle)
    with open('../processed/installments_group.pickle', 'rb') as handle:
		installments_group = pickle.load(handle)
    with open('../processed/POS_CASH_balance_group.pickle', 'rb') as handle:
		POS_CASH_balance_group = pickle.load(handle)
    with open('../processed/previous_application_group.pickle', 'rb') as handle:
		previous_application_group = pickle.load(handle)
    
    for feature in train.columns:
        if train[feature].dtypes == 'object':

            group = train.groupby(feature).agg({'TARGET': 'mean'})
            group.columns = [f'{feature}_Target_mean']
            group.reset_index(inplace=True)

            train = pd.merge(train, group, how='left', on=feature)
            test = pd.merge(test, group, how='left', on=feature)
            gc.collect()

            train[feature].fillna('nan', inplace=True)
            test[feature].fillna('nan', inplace=True)

            lb = LabelEncoder()

            lb = lb.fit(list(set(train[feature].unique()).union(set(test[feature].unique()))))
            train[feature] = lb.transform(train[feature])
            test[feature] = lb.transform(test[feature])
            
    train = data_merge(train,bureau_group,bureau_balance_group,previous_application_group,
						installments_group,POS_CASH_balance_group,credit_card_balance_group)
    test = data_merge(test,bureau_group,bureau_balance_group,previous_application_group,
						installments_group,POS_CASH_balance_group,credit_card_balance_group)
    
    train = data_process(train)
    test = data_process(test)
    
    with open('../processed/train_processed.pickle', 'wb') as handle:
        pickle.dump(train, handle, protocol = pickle.HIGHEST_PROTOCOL)
        
    with open('../processed/test_processed.pickle', 'wb') as handle:
        pickle.dump(test, handle, protocol = pickle.HIGHEST_PROTOCOL)

