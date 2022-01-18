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
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][1][0])
        else:
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],np.nan)

    else:

        '''user_id, business_id, stars, business_stars, business_reviews, price_range, average_stars'''

    
    
        if row[1][1]:
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],row[1][1][0])
        else:
            return (row[0],row[1][0][0],row[1][0][1],row[1][0][2],row[1][0][3],row[1][0][4],np.nan)


def yelp_in(row):
    
    return (row.split(',')[1],(row.split(',')[0],float(row.split(',')[2])))

def map_user(row):

    row=json.loads(row)
    return(row['user_id'],(row['average_stars'],))



def get_weights(iterator):
    weight_dictionary={}
    x=[item for item in iterator]
    for row in x:
        inter_dict={}
        for item in x:
            if row[0] != item[0]:
                intersection=row[1].keys()&item[1].keys()
                if len(intersection)>50:
                    row_reviews=[row[1].get(x) for x in intersection]
                    item_reviews=[item[1].get(x) for x in intersection]
                    row_avg=mean(row_reviews)
                    item_avg=mean(item_reviews)
                    numerator=0
                    left_denominator=0
                    right_denominator=0
                    for i in range(len(row_reviews)):
                        numerator+=(row_reviews[i]-row_avg)*(item_reviews[i]-item_avg)
                        left_denominator+=(row_reviews[i]-row_avg)**2
                        right_denominator+=(item_reviews[i]-item_avg)**2
                    if left_denominator != 0 and right_denominator != 0:
                        corr=numerator/(math.sqrt(left_denominator)*math.sqrt(right_denominator))
                        inter_dict[item[0]]=round(corr,2)
        if len(inter_dict)>0:
            weight_dictionary[row[0]]=inter_dict                
    
    return (1,weight_dictionary)

def convert_index(items):
    index=0
    result={}
    for item in items:
        result[item]=index
        index+=1
    return result

def get_biz_average(id):
    return biz_averages[id]

def get_predictions(row):
    result=3000
    summed_weights=0
    
    try:
        user_id=int(converted_users[row.split(',')[0]])
        
    except:
        user_id=False
    try:
        biz_id=int(converted_biz[row.split(',')[1]])
    except:
        biz_id=False
    
    
    if type(user_id) is type(int()) and type(biz_id) is type(int()):
        try:
            biz_weights=weights[biz_id]
        except:
            biz_weights={}
        
        user_review=user_reviews[user_id]
        
        intersection=biz_weights.keys()&user_review.keys()
        if len(intersection)>2:
            numerator=0
            denomin=0
            
            intersect_weights=[(i,biz_weights[i],abs(biz_weights[i])) for i in intersection]
            topN=sorted(intersect_weights, key= lambda row: row[2])[:N]
            
            summed_weights=sum([x[2] for x in intersect_weights])
            

            
            for i in topN:
                numerator+=i[1]*user_review[i[0]]
                denomin+=i[2]
            try:
                result=numerator/denomin
            except:
                result=get_biz_average(biz_id)
        else:
            result=get_biz_average(biz_id)
            
    elif type(user_id) is not type(int()) and type(biz_id) is type(int()):
        result=get_biz_average(biz_id)
    else:
        result=global_average
    if result<1:
        result=1
    if result>5:
        result=5
    return (row.split(',')[0],row.split(',')[1],result,summed_weights)

if __name__ == "__main__":
    
    init_time=datetime.now()

    sc=SparkContext('local[*]','testReview')
    sc.setLogLevel("WARN")

    #2_1

    N=3
    
    folder=sys.argv[1]
    train=os.path.join(folder,'yelp_train.csv')
    business_file=os.path.join(folder,'business.json')
    user=os.path.join(folder,'user.json')

    rdd = sc.textFile(train,4)
    
    header=rdd.first()
    no_header=rdd.filter(lambda row: row!=header)

    all_biz=no_header.map(lambda row: row.split(',')[1]).distinct().collect()
    converted_biz=convert_index(all_biz)

    all_users=no_header.map(lambda row: row.split(',')[0]).distinct().collect()
    converted_users=convert_index(all_users)

    biz_averages = no_header.map(lambda row: (converted_biz[row.split(',')[1]],float(row.split(',')[2])))\
    .mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda v: round(v[0]/v[1],2)) \
    .collectAsMap()
        
    global_average=mean(biz_averages.values())
    

    weight=no_header.map(lambda row: (converted_biz[row.split(',')[1]],frozenset([(converted_users[row.split(',')[0]],float(row.split(',')[2]))])))\
        .reduceByKey(lambda a,b: a.union(b))\
        .map(lambda row: (row[0], dict(row[1])))\
        .mapPartitions(get_weights)\
        .filter(lambda row: row != 1)\
        .collect()
    
    weights={}
    for dic in weight:
        weights.update(dic)
    
    
    inter_user_reviews=no_header.map(lambda row: (converted_users[row.split(',')[0]],frozenset([(converted_biz[row.split(',')[1]],float(row.split(',')[2]))])))\
        .reduceByKey(lambda a,b: a.union(b))\
        .map(lambda row: {row[0]: dict(row[1])})\
        .collect()


    user_reviews={}
    for dic in inter_user_reviews:
        user_reviews.update(dic)
    
    
    val=sys.argv[2]
    val_rdd=sc.textFile(val)
    val_header=val_rdd.first()
    train_predictions=rdd.filter(lambda row: row!=header).map(get_predictions).collect()
    val_predictions=val_rdd.filter(lambda row: row!=val_header).map(get_predictions).collect()

    
    
    #2_3

    val_check=False

    rdd = sc.textFile(train,4)
    
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

    features=['user_average','business_stars','business_reviews','price_range']
    all_columns=['user_id','business_id','stars','business_stars','business_reviews','price_range','user_average']

    train_df=pd.DataFrame(user_joined.collect(),columns=all_columns)

    train_df=train_df.sort_values(by=['user_id','business_id'])

    train_df=train_df.reset_index(drop=True)
    
    train_x=train_df[features]

    train_y=train_df['stars']

    model=XGBRegressor(n_estimators=200,max_depth=4)
    model.fit(train_x,train_y)

    train_predicted=model.predict(train_x)
    
    val_check=True
    val_rdd=sc.textFile(val)
    val_header=val_rdd.first()
    val_no_header=val_rdd.filter(lambda row: row!=val_header)

    val_rdd = val_no_header.map(lambda row: (row.split(',')[1],(row.split(',')[0],)))
       
    val_joined=val_rdd.leftOuterJoin(business_rdd)\
        .map(clean_rows)

    val_user_joined=val_joined.leftOuterJoin(user_rdd)\
        .map(user_clean_rows)

    all_columns=['user_id','business_id','business_stars','business_reviews','price_range','user_average']

    val_df=pd.DataFrame(val_user_joined.collect(),columns=all_columns)

    test_x=val_df[features]

    pred_y=model.predict(test_x)

    for i in range(len(pred_y)):
        if pred_y[i]>5:
            pred_y[i]=5
        elif pred_y[i]<1:
            pred_y[i]=1

    final_df=val_df[['user_id','business_id']]
    final_df['predictions']=pred_y

    final_df=final_df.sort_values(by=['user_id','business_id'])

    final_df=final_df.reset_index(drop=True)


    val_combined_df=pd.DataFrame(val_predictions,columns=['user_id','business_id','task1_prediction','task1_total_weight'])

    val_combined_df=val_combined_df.sort_values(by=['user_id','business_id'])

    

    val_combined_df.reset_index(inplace=True,drop=True)

    val_combined_df['task2_prediction']=final_df['predictions']

    #val_combined_df['diff']=val_combined_df['task2_prediction']-val_combined_df['task1_prediction']

    val_new_train_x=val_combined_df[['task1_prediction','task1_total_weight','task2_prediction']]

    val_new_train_x['task2_total_weight']=[1 for i in range(len(val_new_train_x))]-val_combined_df['task1_total_weight']

    final_pred=val_new_train_x['task1_prediction']*val_new_train_x['task1_total_weight']+val_new_train_x['task2_prediction']*val_new_train_x['task2_total_weight']

    for i in range(len(final_pred)):
        if final_pred[i]>5:
            final_pred[i]=5
        elif final_pred[i]<1:
            final_pred[i]=1

    final_pred_df=pd.DataFrame()

    final_pred_df[['user_id','business_id']]=val_combined_df[['user_id','business_id']]
    final_pred_df['predictions']=final_pred

    outfile=sys.argv[3]

    final_pred_df.to_csv(outfile,index=False)
    





    print('Runtime:',(datetime.now()-init_time).total_seconds())
    
