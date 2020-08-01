# Home Credit Default Risk Overview

Home Credit strives to broaden financial inclusion for the unbanked population by providing a positive and safe borrowing experience. In order to make sure this underserved population has a positive loan experience, Home Credit makes use of a variety of alternative data--including telco and transactional information--to predict their clients' repayment abilities.

While Home Credit is currently using various statistical and machine learning methods to make these predictions, they're challenging Kagglers to help them unlock the full potential of their data. Doing so will ensure that clients capable of repayment are not rejected and that loans are given with a principal, maturity, and repayment calendar that will empower their clients to be successful.

For each SK_ID_CURR in the test set, you must predict a probability for the TARGET variable. Submissions are evaluated on area under the ROC curve between the predicted probability and the observed target.

# File Descriptions

* application_{train|test}.csv

  * This is the main table, broken into two files for Train (with TARGET) and Test (without TARGET). 
  * Static data for all applications. One row represents one loan in our data sample.
* bureau.csv

  * All client's previous credits provided by other financial institutions that were reported to Credit Bureau (for clients who have a loan in our sample). 
  * For every loan in our sample, there are as many rows as number of credits the client had in Credit Bureau before the application date.

* bureau_balance.csv

  * Monthly balances of previous credits in Credit Bureau.
  * This table has one row for each month of history of every previous credit reported to Credit Bureau – i.e the table has (#loans in sample * # of relative previous credits * # of months where we have some history observable for the previous credits) rows.

* POS_CASH_balance.csv

  * Monthly balance snapshots of previous POS (point of sales) and cash loans that the applicant had with Home Credit.
  * This table has one row for each month of history of every previous credit in Home Credit (consumer credit and cash loans) related to loans in our sample – i.e. the table has (#loans in sample * # of relative previous credits * # of months in which we have some history observable for the previous credits) rows.

* credit_card_balance.csv

  * Monthly balance snapshots of previous credit cards that the applicant has with Home Credit.
  * This table has one row for each month of history of every previous credit in Home Credit (consumer credit and cash loans) related to loans in our sample – i.e. the table has (#loans in sample * # of relative previous credit cards * # of months where we have some history observable for the previous credit card) rows.

* previous_application.csv

  * All previous applications for Home Credit loans of clients who have loans in our sample.
  * There is one row for each previous application related to loans in our data sample.

* installments_payments.csv

  * Repayment history for the previously disbursed credits in Home Credit related to the loans in our sample.
  * There is a) one row for every payment that was made plus b) one row each for missed payment.
  * One row is equivalent to one payment of one installment OR one installment corresponding to one payment of one previous Home Credit credit related to loans in our sample.

* HomeCredit_columns_description.csv

  * This file contains descriptions for the columns in the various data files.
  
  ![Figure 1](/images/home_credit.png)
  
# Data Preprocessing

I spent almost a week to just go through all the provided data. Besides main train and test data set, other information is distributed in 6 other data sets with 2 being the summary and 4 others monthly records complementary to 2 summary data sets. I mainly tried to understand the meaning of each column and the correlations between different columns. It definitely pays off even though I spent a lot of time on it.

I found in days related features 365243 is very common and it means the days doesn't exist, in another word, np.nan values. So I replaced 365243 with np.nan. Besides this, I also found train and test data set distribution are quite different from various perspectives. I tried changing weights of different samples or downsampling the train data. But none of these methods works and they make results worse. Probably oversampling with SMOTE can work, however, I can't figure out how to define the distance between different samples so I didn't try this method. Another thing worth mentioning in this competition is to deal with Nan values. I found leave np.nan values as they are work the best for me.

# Feature Engineering

Based on my own experience and top solutions in the competition, feature engineering (FE) is the key to success in this competition. To perform good FE, I think two things are the most important: 1. Domain knowledge 2. Patience and attention to details. For domain knowledge, I searched online and learned that the high ratio between credit card balance the balance limit suggests a potential risk even though this person may not have bad payment records. This inspires me to create a lot of ratio features between different balances. For patience and details, people may expect to find the fancy and magic feature to completely solve the problem and land a gold medal immediately. However, in this competition, I found the point is not to leave out any details or easy feautres. These features include ratio and difference features between raw features, or FE based on the most recent data instead of all the data provided in each data set. My FE applies to all data sets. It includes:

* Ratio and difference features between different days (DAYS_BIRTH, DAYS_EMPLOYED) and different money amounts (AMT_APPLICATION, AMT_INCOME, AMT_ANNUITY)
* Aggregated statistical features (max, min, mean, sum) from all, most recent (2 years, 1 year, 6 months, 3 months) and different types (approved, closed, active) of the data
* Aggregated statistical features with money amount threshould over first or last a few installments
* Label encoding categorical features. This doesn't have any effects on the result. But since it does not make the result worse, I prefer it to one-hot encoding because label encoding also helps to save the space
* Target encoding with categorical features such as education level or occupation

# Modelling

Final model is heavily based on lightgbm models. I trained 10 first level lightgbm models with 5-fold validation scheme with different random seeds. I also trained 1 xgboost and 1 catboost models with 5-fold validation scheme. In the second level model I combined out of folds results from 12 models as features with earlier features and used lightgbm to fit. It improves both the local CV and LB results. The final result is 0.79718 ranked 214/7190.

# Summary

* What works
  * Ratio and difference features and aggregated statistical features based on ratio and difference features 
  * Aggregated statistical features based on subsets of different data sets (data from last 2 year, 1 year, 3 month. First or last 20 installments records)
  * Hyperparameter tunning
  * Model stacking
  * Feature selection based on shap value or feature importance change after shuffling of targets 
 
* What doesn't work
  * Sample downsampling
  * Different sample weights
  * Outlier removing based on target and oof results or some feature values
  * Try to figure out application from same people
 
# Things to improve

* The way to master feature engineering goes on and on and on
* A very interesting idea I came across in the top solutions is to model the interest rate and use the prediction as a feature. This reminds me to be more sensitive and flexible about modelling instead of only focusing on the main task.
* Similar to the modelling of the interest rate, top solutions in the competition also suggest the modelling of EXT_1 and use the prediction as a feature or to fill nan values.
