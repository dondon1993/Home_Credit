import numpy as np
import pandas as pd
import pickle, gc, shap, math, random, time

import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder
from sklearn import metrics
from sklearn import model_selection
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostClassifier
from sklearn.linear_model import LinearRegression
from sklearn.svm import NuSVR, SVR


def train_model_classification(X, y, params, groups, folds, model_type='lgb', eval_metric='auc', columns=None, plot_feature_importance=False, model=None,
                               verbose=10000, early_stopping_rounds=200, n_estimators=50000, weight = None, seed='no'):
    """
    A function to train a variety of regression models.
    Returns dictionary with oof predictions, test predictions, scores and, if necessary, feature importances.
    
    :params: X - training data, can be pd.DataFrame
    :params: X_test - test data, can be pd.DataFrame
    :params: y - target
    :params: folds - folds to split data
    :params: model_type - type of model to use
    :params: eval_metric - metric to use
    :params: columns - columns to use. If None - use all columns
    :params: plot_feature_importance - whether to plot feature importance of LGB
    :params: model - sklearn model, works only for "sklearn" model type
    
    """
    columns = X.columns if columns is None else columns
    models = []
    
    metrics_dict = {'auc': {'lgb_metric_name': 'auc',
                        'catboost_metric_name': 'AUC',
                        'sklearn_scoring_function': metrics.roc_auc_score},
                    }

    result_dict = {}
    
    # out-of-fold predictions on train data
    oof = np.zeros(len(X))
    
    scores = []
    train_loss = []
    feature_importance = pd.DataFrame()
    
    if groups is None:
        splits = folds.split(X)
        
    elif groups == 'stra':
        splits = folds.split(X,y)
        
    else:
        splits = folds.split(X, groups = groups)
        print('no')
        
    for fold_n, (train_index, valid_index) in enumerate(splits):
        
        print(f'Fold {fold_n + 1} started at {time.ctime()}')
            
        if type(X) == np.ndarray:
            X_train, X_valid = X[columns][train_index], X[columns][valid_index]
            y_train, y_valid = y[train_index], y[valid_index]
            weight_train = weight[train_index]
        else:
            X_train, X_valid = X[columns].iloc[train_index], X[columns].iloc[valid_index]
            y_train, y_valid = y.iloc[train_index], y.iloc[valid_index]
            weight_train = weight[train_index]
            
        if model_type == 'lgb':
            
            model = lgb.LGBMClassifier(**params, n_estimators = n_estimators, n_jobs = -1)
            model.fit(X_train, y_train, sample_weight = weight_train, 
                    eval_set=[(X_train, y_train), (X_valid, y_valid)], eval_metric=metrics_dict[eval_metric]['lgb_metric_name'],
                    verbose=verbose, early_stopping_rounds=early_stopping_rounds)
            
            y_pred_valid = model.predict_proba(X_valid)[:, 1]
            y_pred_train = model.predict_proba(X_train)[:, 1]
            models.append(model)
            
        if model_type == 'xgb':
            train_data = xgb.DMatrix(data=X_train, label=y_train, feature_names=X.columns)
            valid_data = xgb.DMatrix(data=X_valid, label=y_valid, feature_names=X.columns)

            watchlist = [(train_data, 'train'), (valid_data, 'valid_data')]
            model = xgb.train(dtrain=train_data, num_boost_round=n_estimators, evals=watchlist, early_stopping_rounds=early_stopping_rounds, verbose_eval=verbose, params=params)
            y_pred_valid = model.predict(xgb.DMatrix(X_valid, feature_names=X.columns), ntree_limit=model.best_ntree_limit)
            y_pred_train = model.predict(xgb.DMatrix(X_train, feature_names=X.columns), ntree_limit=model.best_ntree_limit)
            models.append(model)
        
        if model_type == 'sklearn':
            model = model
            model.fit(X_train, y_train)
            
            y_pred_valid = model.predict(X_valid).reshape(-1,)
            score = metrics_dict[eval_metric]['sklearn_scoring_function'](y_valid, y_pred_valid)
            print(f'Fold {fold_n}. {eval_metric}: {score:.4f}.')
            print('')
            models.append(model)
        
        if model_type == 'cat':
            model = CatBoostClassifier(iterations=n_estimators,  eval_metric=metrics_dict[eval_metric]['catboost_metric_name'], **params,
                                      )
            model.fit(X_train, y_train, eval_set=(X_valid, y_valid), cat_features=[], use_best_model=True, verbose=verbose,early_stopping_rounds=early_stopping_rounds)
            y_pred_valid = model.predict_proba(X_valid)[:,1]
            y_pred_train = model.predict_proba(X_train)[:,1]
            models.append(model)
        
        oof[valid_index] = y_pred_valid.reshape(-1,)
        if eval_metric != 'group_mae':
            scores.append(metrics_dict[eval_metric]['sklearn_scoring_function'](y_valid, y_pred_valid))
            train_loss.append(metrics_dict[eval_metric]['sklearn_scoring_function'](y_train, y_pred_train))
        else:
            scores.append(metrics_dict[eval_metric]['scoring_function'](y_valid, y_pred_valid, X_valid['type']))

        with open(f'./models/models_{model_type}_{seed}.pickle', 'wb') as handle:
            pickle.dump(models, handle, protocol = pickle.HIGHEST_PROTOCOL)
            
        gc.collect()
        
        if model_type == 'lgb' and plot_feature_importance:
            # feature importance
            fold_importance = pd.DataFrame()
            fold_importance["feature"] = columns
            fold_importance["importance"] = model.feature_importances_
            fold_importance["shap_values"] = abs(shap.TreeExplainer(model).shap_values(X_valid)[:,:len(columns)]).mean(axis=0).T
            fold_importance["fold"] = fold_n + 1
            feature_importance = pd.concat([feature_importance, fold_importance], axis=0)

    print('Train loss mean: {0:.6f}, std: {1:.6f}.'.format(np.mean(train_loss), np.std(train_loss)))
    print('CV mean score: {0:.6f}, std: {1:.6f}.'.format(np.mean(scores), np.std(scores)))
    
    result_dict['oof'] = oof
    result_dict['scores'] = scores
    result_dict['models'] = models
    
    if model_type == 'lgb':
        if plot_feature_importance:
            feature_importance["importance"] /= folds.n_splits
            cols = feature_importance[["feature", "importance"]].groupby("feature").mean().sort_values(
                by="importance", ascending=False)[:50].index

            best_features = feature_importance.loc[feature_importance.feature.isin(cols)]

            plt.figure(figsize=(16, 12));
            sns.barplot(x="importance", y="feature", data=best_features.sort_values(by="importance", ascending=False));
            plt.title('LGB Features (avg over folds)');
            
            result_dict['feature_importance'] = feature_importance
        
    return result_dict

  
def predict(df, models, model_type = 'lgb'):
    prediction = np.zeros(len(df))
    for model in models:
        
        if model_type == 'lgb':
            pred = model.predict_proba(df)[:, 1]
        if model_type == 'xgb':
            pred = model.predict(xgb.DMatrix(df, feature_names=df.columns), ntree_limit=model.best_ntree_limit)
        if model_type == 'cat':
            pred = model.predict_proba(df)[:,1]
        prediction += pred/len(models)
        
    return prediction

  
class train_config:
    
    def __init__(self, n_splits, features, model_type, model_params, eval_metric, early_stopping_rounds, n_estimators,
                 save_oof, seed):
        
        self.n_splits = n_splits
        self.features = features
        self.model_type = model_type
        self.model_params = model_params
        self.eval_metric = eval_metric
        self.early_stopping_rounds = early_stopping_rounds
        self.n_estimators = n_estimators
        self.save_oof = save_oof
        self.seed = seed

        
def model_train(df_train, train_config):
    
    n_splits = train_config.n_splits
    seed = train_config.seed
    folds = KFold(n_splits, shuffle = True, random_state = seed)
    
    X_train = df_train[train_config.features]
    y_train = df_train['TARGET']
    weight = df_train['weight']
    
    result_dict = train_model_classification(
                             X=X_train, 
                             y=y_train, 
                             params=train_config.model_params, 
                             groups = None, 
                             folds=folds, model_type=train_config.model_type, eval_metric=train_config.eval_metric, 
                             plot_feature_importance=True,verbose=100, early_stopping_rounds=train_config.early_stopping_rounds,
                             n_estimators=train_config.n_estimators,weight = weight, seed = seed)
    
    return result_dict


if __name__ == "__main__":
    
    with open('./processed/train_processed.pickle', 'rb') as handle:
        train = pickle.load(handle)

    with open('./processed/test_processed.pickle', 'rb') as handle:
        test = pickle.load(handle)
        
    config_path = sys.argv[1]
    with open(config_path) as json_file:
        config = json.load(json_file)
        
    t_config = train_config(
        n_splits = config['n_splits'], 
        features = config['features'],
        model_type = config['model_type'],
        model_params = config['model_params'], 
        eval_metric = config['eval_metric'],
        early_stopping_rounds = config['early_stopping_rounds'],
        n_estimators = config['n_estimators'],
        save_oof = config['save_oof'],
        seed = config['seed'],
    )
    
    result_dict = model_train(train, t_config)
    
    models = result_dict['models']
    prediction = predict(test[t_config.features], models)
    
    if t_config.save_oof == True:
        
        train[f'oof_{t_config.model_type}_{t_config.seed}'] = result_dict['oof']
        test[f'oof_{t_config.model_type}_{t_config.seed}'] = prediction
        
        with open('./processed/train_processed.pickle', 'wb') as handle:
            pickle.dump(train, handle, protocol = pickle.HIGHEST_PROTOCOL)
        with open('./processed/test_processed.pickle', 'wb') as handle:
            pickle.dump(test, handle, protocol = pickle.HIGHEST_PROTOCOL)
    
    sample_submission = pd.read_csv('./input/sample_submission.csv')
    sample_submission['TARGET'] = prediction
    sample_submission.to_csv(f'./submissions/submission_{t_config.model_type}_{t_config.seed}.csv', index=False)

