from pyspark import SparkContext
import sys
import itertools
from collections import defaultdict
import csv
from datetime import datetime

def apriori_get_frequents(buckets):
    support=s/num_partitions
    counts=defaultdict(int)
    frequents=set()
    for bucket in buckets:
        for item in bucket:
            counts[item]+=1
    for item, count in counts.items():
        if count>=support:
            frequents.add(item)
    return frozenset(frequents)

def apriori_get_candidate_counts(current_candidate_sets, k):
    counts=defaultdict(int)
    if k>1:
        current_candidate_items=set()
        for item in current_candidate_sets:
            for inner_item in item:
                counts[inner_item]+=1
        for item, count in counts.items():
            if count>=(k-1):
                current_candidate_items.add(item)
        return current_candidate_items
    else:
        return current_candidate_sets
    


def apriori_get_buckets(buckets, current_candidate_items,k):
    new_buckets=[]
    for bucket in buckets:
        new_bucket=set()
        for item in bucket:
            if k==2:
                if item in current_candidate_items:
                    new_bucket.add(item)
            else:
                for inner_item in item:
                    if inner_item in current_candidate_items:
                        new_bucket.add(inner_item)
        new_bucket=sorted(new_bucket)
        if len(new_bucket)>=k:
            new_bucket=frozenset(combo for combo in itertools.combinations(new_bucket,k))
            new_buckets.append(new_bucket)
    
    
    return new_buckets

def apriori(iterator):
    '''
    iterator: the entire amount of information passed from the mapPartition, passed as an iterable
    buckets: list that holds each of the buckets in the partition
    bucket: frozenset given as the second argument in the iterator
    k: length of current set
    get_frequents(): function that takes in the buckets, returns a set of of sets, called current_candidate_sets
    current_candidate_sets: set of sets of items that most recently passed the condition of having support of k-length sets
    get_candidate_counts(): function that takes in current_candidate_sets and k, counts how many sets each singleton appears in and returns
    a set of singletons that pass the condition of appearing in k! candidate sets
    current_candidate_items: set of items that pass the condition of being in k! k candidate sets, this will be used to filter
    the next read of buckets
    x: boolean indicating whether the most recent current_candidate_set has length>0
    get_buckets(): function that takes in the previous buckets and the current_candidate_items, returns the buckets 
    filled with k-combos which are made after buckets are filtered to only contain the elements in the current_candidate_items
    all_candidates: set of all candidate sets in the form (k,v) where k is the length, v is the set of candidates,
    this is what will be returned
    '''
    buckets=[]
    for bucket in iterator:
        buckets.append(bucket[1])
    
    k=1
    all_candidates=set()
    
    while len(buckets) > 0:
        current_candidate_sets=apriori_get_frequents(buckets)
        all_candidates.add((k,current_candidate_sets))
        current_candidate_items=apriori_get_candidate_counts(current_candidate_sets,k)
        k+=1
        buckets=apriori_get_buckets(buckets, current_candidate_items,k)
        
    return all_candidates

def pass2_get_counts(buckets):
    counts=defaultdict(int)
    for bucket in buckets:
        for item in bucket:
            counts[item]+=1
    return counts

def pass2_get_buckets(buckets,k):
    try:
        new_buckets=[]
        for bucket in buckets:
            new_bucket=set()
            for item in bucket:
                if k==1:
                    if item in k_level_candidates[k]:
                        new_bucket.add(item)
                else:
                    for inner_item in item:
                        if inner_item in k_level_candidates[k]:
                            new_bucket.add(inner_item)
            new_bucket=sorted(new_bucket)
            if len(new_bucket)>=k+1:
                new_bucket=frozenset(combo for combo in itertools.combinations(new_bucket,k+1) if combo in candidates[k][1])
                new_buckets.append(new_bucket)
            
    except:
        new_buckets=[]
        
                
    return new_buckets

def pass2(iterator):
    results=[]
    buckets=[]
    for bucket in iterator:
        new_bucket=set()
        for item in bucket[1]:
            if item in candidates[0][1]:
                new_bucket.add(item)
        if len(new_bucket)>0:
            buckets.append(new_bucket)
    k=1
    
    while len(buckets)>0:
        
        item_counts=pass2_get_counts(buckets)
        for item, count in item_counts.items():
            if k==1:
                results.append(((item,),count))

            else:
                results.append((item,count))
        buckets=pass2_get_buckets(buckets,k)
       
        k+=1
    return results

def clean_lines(line):
    try:
        date=line.split(',')[0].strip('"')[:len(line.split(',')[0].strip('"'))-4]+line.split(',')[0].strip('"')[len(line.split(',')[0].strip('"'))-2:]
        profile_id=line.split(',')[1].strip('"')
        date_profile_id='{}-{}'.format(date,profile_id)
        product_id=int(line.split(',')[5].strip('"').lstrip('0'))
    except:
        date_profile_id="DATE-CUSTOMER_ID"
        product_id="PRODUCT_ID"

    return (date_profile_id,product_id)

def get_k_level_candidates(candidates):
    k_level_candidates=[]
    for candidate_set in candidates:
        if candidate_set[0]==1:
            k_level_candidates.append(candidate_set[1])
        else:
            k_level_candidate=set()
            for item in candidate_set[1]:
                for inner_item in item:
                    k_level_candidate.add(inner_item)
            k_level_candidates.append(k_level_candidate)
    return k_level_candidates

def get_sorted_lists(true_frequents,is_candidate):
    sorted_lists=[]
    if is_candidate:
        for ls in true_frequents:
            if ls[0]==1:
                sorted_list=sorted(ls[1])
                sorted_lists.append(sorted_list)
            else:   
                sorted_list=sorted(ls[1], key= lambda element: element[0:])
                sorted_lists.append(sorted_list)
    else:
        for ls in true_frequents:
            sorted_list=sorted(ls[1], key= lambda element: element[0:])
            sorted_lists.append(sorted_list)

    return sorted_lists

def write_file(sorted_candidates,sorted_frequents):
    
    textFile=open(output,'w')
    candidates_line=True
    for sorted_set in [sorted_candidates,sorted_frequents]:
        #Write the header
        if candidates_line==True:
            textFile.write('Candidates:\n')
        else:
            textFile.write('Frequent Itemsets:\n')
        
        first_line=True
        for line in sorted_set:
            first_element=True
            for element in line:
                if first_line==True and candidates_line==False:
                    for inner_element in element:
                            if first_element==True: 
                                textFile.write("('{}')".format(inner_element))
                                first_element=False
                            else:
                                textFile.write(",('{}')".format(inner_element))
                elif first_line==True and candidates_line==True:
                    if first_element==True:
                        textFile.write("('{}')'".format(element))
                        first_element=False
                    else:
                        textFile.write(",('{}')".format(element))
                        
                else:
                    if first_element==True:
                        textFile.write('{}'.format(element))
                        first_element=False
                    else:
                        textFile.write(',{}'.format(element))
            first_line=False
            textFile.write('\n')
        candidates_line=False

if __name__ == '__main__':

    init_time=datetime.now()
    qualified=int(sys.argv[1])
    s=int(sys.argv[2])
    input_file=sys.argv[3]
    output=sys.argv[4]

    sc=SparkContext('local[*]','testReview')
    sc.setLogLevel("WARN")
    rdd = sc.textFile(input_file)
    header=rdd.first()
    lines=rdd.filter(lambda lines: lines!=header).map(clean_lines)
    
    #Write out to CSV
    with open('intermediate.csv','w') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(("DATE-CUSTOMER_ID","PRODUCT_ID"))
        for row in lines.collect():
            csv_out.writerow(row)

    
    rdd=lines.map(lambda lines: (lines[0],[str(lines[1])]))\
        .reduceByKey(lambda a,b: a+b)\
        .filter(lambda lines: len(lines[1])>qualified)\
        .map(lambda lines: (lines[0],frozenset(lines[1])))
    
    num_partitions=rdd.getNumPartitions()

    uncollected_candidates=rdd.mapPartitions(apriori)\
        .reduceByKey(lambda a,b: a.union(b))\
        .sortByKey()\
    
    candidates=uncollected_candidates.collect()

    formatted_candidates=uncollected_candidates.map(lambda lines: (lines[0],list(lines[1]))).collect()
    sorted_candidates=get_sorted_lists(formatted_candidates,True)
    k_level_candidates=get_k_level_candidates(candidates)
    
    

    true_frequents=rdd.mapPartitions(pass2)\
    .reduceByKey(lambda a, b: a+b)\
    .filter(lambda lines: lines[1]>=s)\
    .map(lambda lines: (len(lines[0]),list((lines[0],))))\
    .reduceByKey(lambda a,b: a+b)\
    .sortByKey()\
    .collect()
    sorted_frequents=get_sorted_lists(true_frequents,False)
    
    write_file(sorted_candidates,sorted_frequents)
    print((datetime.now()-init_time).total_seconds())
    
