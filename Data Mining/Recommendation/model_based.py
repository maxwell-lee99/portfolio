from pyspark import SparkContext
import os
import sys
import itertools
from collections import defaultdict, Counter
import math
from datetime import datetime
import random
from operator import itemgetter
from statistics import mean
import json
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
from matplotlib import pyplot


def json_load(row):

    row=json.loads(row)
    try:
        price_range=float(row['attributes']['RestaurantsPriceRange2'])
    except:
        price_range=np.nan

    try:
        is_open=float(row['is_open'])
    except:
        is_open=np.nan

    return (row['business_id'],(row['stars'],row['review_count'],price_range,is_open))

def clean_rows(row):

    if val_check:
        if row[1][1]:
            '''
            business_id, user_id, business_stars, business_reviews, price_range, is_open
            '''
            
            return (row[1][0][0],(row[0],row[1][1][0],row[1][1][1],row[1][1][2],row[1][1][3]))
        else:
            return (row[1][0][0],(row[0],np.nan, np.nan, np.nan))

    else:

        if row[1][1]:
            '''
            business_id, user_id, stars *target*, business_stars, business_reviews, price_range, is_open
            '''
            
            return (row[1][0][0],(row[0],row[1][0][1],row[1][1][0],row[1][1][1],row[1][1][2],row[1][1][3]))
        else:
            return (row[1][0][0],(row[0],row[1][0][1],np.nan, np.nan, np.nan))
    
def user_clean_rows(row):

    if val_check:
        if row[1][1]:
            return (row[0],[row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][1][0]])
        else:
            return (row[0],[row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],np.nan])

    else:

        '''user_id, business_id, stars, business_stars, business_reviews, price_range, average_stars'''

    
    
        if row[1][1]:
            return (row[0],[row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],row[1][1][0]])
        else:
            return (row[0],[row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],np.nan])


def yelp_in(row):
    
    return (row.split(',')[1],(row.split(',')[0],float(row.split(',')[2])))

def map_user(row):

    row=json.loads(row)
    return(row['user_id'],(row['average_stars'],))

def ratio(row):
    counts={1:0,2:0,3:0,4:0,5:0}
    
    for score in row[1]:
        counts[score] +=1
    result=[]

    for key,value in counts.items():
        result.append(value/len(row[1]))

    result.append(max(counts,key=counts.get))
    
    return(row[0],result)


def average_clean(row):

    if val_check:
        
        try:
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],row[1][1][0],row[1][1][1],row[1][1][2],row[1][1][3],row[1][1][4],row[1][1][5])
        except:
            print(row)
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan)
    else:
        
        if row[1][1]:
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],row[1][0][5],row[1][1][0],row[1][1][1],row[1][1][2],row[1][1][3],row[1][1][4],row[1][1][5])
        else:
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],row[1][0][5],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan)
    

if __name__ == "__main__":
    
    init_time=datetime.now()

    sc=SparkContext('local[*]','testReview')
    sc.setLogLevel("WARN")

    folder=sys.argv[1]
    train=os.path.join(folder,'yelp_train.csv')
    business_file=os.path.join(folder,'business.json')
    user=os.path.join(folder,'user.json')
    
    # train='hw3/yelp_val.csv'
    val=sys.argv[2]

    
    val_check=False

    rdd = sc.textFile(train)
    
    header=rdd.first()
    no_header=rdd.filter(lambda row: row!=header)

    train_rdd = no_header.map(yelp_in)

    business_rdd=sc.textFile(business_file)\
        .map(json_load)

    joined=train_rdd.leftOuterJoin(business_rdd)\
        .map(clean_rows)
    
    user_rdd=sc.textFile(user).map(map_user)

    user_joined=joined.leftOuterJoin(user_rdd)\
        .map(user_clean_rows)

    

    user_averages = no_header.map(lambda row: (row.split(',')[0],[float(row.split(',')[2])]))\
    .reduceByKey(lambda a,b: a+b)\
    .map(ratio)

    average_joined=user_joined.leftOuterJoin(user_averages).map(average_clean)

    

    
    

    

    features=['user_average','business_stars','business_reviews','price_range','1_ratio','2_ratio','3_ratio','4_ratio','5_ratio','mode']
    all_columns=['user_id','business_id','stars','business_stars','business_reviews','price_range','user_average','1_ratio','2_ratio','3_ratio','4_ratio','5_ratio','mode']

    train_df=pd.DataFrame(average_joined.collect(),columns=all_columns)

    train_df.to_csv('train_df.csv',index=False)
    
    train_x=train_df[features]

    train_y=train_df['stars']

    model=XGBRegressor(n_estimators=400,max_depth=4)
    model.fit(train_x,train_y)
    
    val_check=True
    val_rdd=sc.textFile(val)
    val_header=val_rdd.first()
    val_no_header=val_rdd.filter(lambda row: row!=val_header)

    val_rdd = val_no_header.map(lambda row: (row.split(',')[1],(row.split(',')[0],)))
       
    val_joined=val_rdd.leftOuterJoin(business_rdd)\
        .map(clean_rows)

    val_user_joined=val_joined.leftOuterJoin(user_rdd)\
        .map(user_clean_rows)

    
    val_average_joined=val_user_joined.leftOuterJoin(user_averages).map(average_clean)

    

    

    all_columns=['user_id','business_id','business_stars','business_reviews','price_range','user_average','1_ratio','2_ratio','3_ratio','4_ratio','5_ratio','mode']

    val_df=pd.DataFrame(val_average_joined.collect(),columns=all_columns)

    val_df.to_csv('val_df.csv',index=False)

    test_x=val_df[features]

    pred_y=model.predict(test_x)

    for i in range(len(pred_y)):
        if pred_y[i]>5:
            pred_y[i]=5
        elif pred_y[i]<1:
            pred_y[i]=1

    final_df=val_df[['user_id','business_id']]
    final_df['predictions']=pred_y

    outfile=sys.argv[3]

    final_df=final_df.sort_values(by=['user_id','business_id'])

    final_df.to_csv(outfile,index=False)

    

    print('Runtime:',(datetime.now()-init_time).total_seconds())

    
    

    