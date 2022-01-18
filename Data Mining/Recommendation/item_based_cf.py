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



def get_weights(iterator):
    weight_dictionary={}
    x=[item for item in iterator]
    for row in x:
        inter_dict={}
        for item in x:
            if row[0] != item[0]:
                intersection=row[1].keys()&item[1].keys()
                if len(intersection)>5:
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
    return (row.split(',')[0],row.split(',')[1],result)

if __name__ == "__main__":
    
    init_time=datetime.now()

    sc=SparkContext('local[*]','testReview')
    sc.setLogLevel("WARN")

    N=3
    
    train='hw3/yelp_train.csv'

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
    
    
    val='hw3/yelp_val_in.csv'
    val_rdd=sc.textFile(val)
    header=val_rdd.first()
    predictions=val_rdd.filter(lambda row: row!=header).map(get_predictions).collect()
    
    
    outfile='yelp_val_predictions.csv'
    
    textFile=open(outfile,'w')
    textFile.write('user_id,business_id,prediction')
    for pair in predictions:
        textFile.write('\n{},{},{}'.format(pair[0],pair[1],pair[2]))
    







    





    print((datetime.now()-init_time).total_seconds())
    
