from pyspark import SparkContext
import os
import sys
import itertools
from collections import defaultdict
import math
from datetime import datetime
import random

def convert_index(items):
    index=0
    result={}
    for item in items:
        result[item]=index
        index+=1
    return result

def hashes(iterator):
    for row in iterator:
        for biz in row[1]:
            for i in range(num_hashes):
                result=(row[0]*ax[i]+b[i])%num_all_users
                if result<sig_matrix[i][biz]:
                    sig_matrix[i][biz]=result
    
    return [sig_matrix]
    
def combiner(a,b):
    for i in range(len(a)):
        for x in range(len(a[i])):
            if a[i][x]>b[i][x]:
                a[i][x]=b[i][x]
    return a
    
def sim(iterator):
    
    d=defaultdict(tuple)
    for item in iterator:
        for i in range(len(item)):
            d[i]+=(item[i],)
    new_d=defaultdict(list)
    for key, value in d.items():
        new_d[hash(value)].append(key)
    pairs=()
    for key, value in new_d.items():
        if len(value)>1:
            for pair in itertools.combinations(value,2):
                pairs+=(pair,)
    
    return pairs


if __name__ == '__main__':
    
    init_time=datetime.now()

    sc=SparkContext('local[*]','testReview')
    sc.setLogLevel("WARN")
    num_hashes=150
    bands=50
    rows=3
    s=.5
    infile=sys.argv[1]
    outfile=sys.argv[2]

    rdd = sc.textFile(infile)
    header=rdd.first()
    no_header=rdd.filter(lambda row: row!=header)
    
    all_biz=no_header.map(lambda row: row.split(',')[1]).distinct().collect()
    converted_biz=convert_index(all_biz)

    all_users=no_header.map(lambda row: row.split(',')[0]).distinct().collect()
    converted_users=convert_index(all_users)

    
    sig_matrix=[[math.inf for biz in all_biz] for i in range(num_hashes)]
    
    num_all_users=len(all_users)
    ax=[(random.randint(1,num_all_users)**2)*2-1 for i in range(num_hashes)]
    b=[random.randint(1,num_all_users) for i in range(num_hashes)]

    final_sig_matrix=no_header.map(lambda row: (converted_users[row.split(',')[0]],frozenset((converted_biz[row.split(',')[1]],))))\
        .reduceByKey(lambda a,b: a.union(b))\
        .mapPartitions(hashes)\
        .reduce(combiner)
    candidate_pairs=()
    

    for i in range(bands):
        if i==bands-1:
            candidate_pairs+=(sim(final_sig_matrix[(i*rows):]))
        else:
            candidate_pairs+=(sim(final_sig_matrix[(i*rows):((i+1)*rows)]))

    candidate_pairs=frozenset(candidate_pairs)
    
    
    print(len(candidate_pairs))
    
    transposed=list(map(list, zip(*final_sig_matrix)))
    
    checked_candidate_pairs=[]
    for pair in candidate_pairs:
        list1=transposed[pair[0]]
        list2=transposed[pair[1]]
        count=0
        for i in range(num_hashes):
            if list1[i]==list2[i]:
                count+=1
        if count/num_hashes>=s*.8:
            checked_candidate_pairs.append((pair[0],pair[1]))
    print(checked_candidate_pairs)
    in_pairs=()
    for pair in checked_candidate_pairs:
        in_pairs+=pair
    
    in_pairs=frozenset(in_pairs)

    
    rdd_in_pairs=no_header.filter(lambda row: converted_biz[row.split(',')[1]] in in_pairs)\
        .map(lambda row: (converted_biz[row.split(',')[1]],frozenset((converted_users[row.split(',')[0]],))))\
        .reduceByKey(lambda a,b: a.union(b))\
        
    final_pairs=[]
    biz_and_users=dict(rdd_in_pairs.collect())
    print(biz_and_users)
    for pair in checked_candidate_pairs:
        print()
        set1=biz_and_users[pair[0]]
        set2=biz_and_users[pair[1]]
        similarity=len(set1.intersection(set2))/len(set1.union(set2))
        if similarity>=s:
            if all_biz[pair[0]]<all_biz[pair[1]]:
                final_pairs.append((all_biz[pair[0]],all_biz[pair[1]],similarity))
            else:
                final_pairs.append((all_biz[pair[1]],all_biz[pair[0]],similarity))
     
    
    sorted_final_pairs=sorted(final_pairs, key=lambda x: (x[0],x[1]))
    print(len(final_pairs))
    
    textFile=open(outfile,'w')
    textFile.write('business_id_1,business_id_2,similarity')
    for pair in sorted_final_pairs:
        textFile.write('\n{},{},{}'.format(pair[0],pair[1],pair[2]))


    



    




    print((datetime.now()-init_time).total_seconds())
        
    
    

